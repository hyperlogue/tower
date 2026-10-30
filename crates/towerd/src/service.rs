use crate::{Error, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tower_pal::{Paths, Peer};
use tower_protocol::*;
use tower_store::{Store, StoredAgent};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn random(n: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0; n];
    getrandom::fill(&mut bytes)
        .map_err(|_| Error::Other("system random source unavailable".into()))?;
    Ok(bytes)
}
fn base32(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut bits = 0u32;
    let mut count = 0;
    let mut out = String::new();
    for b in bytes {
        bits = (bits << 8) | u32::from(*b);
        count += 8;
        while count >= 5 {
            count -= 5;
            out.push(ALPHABET[((bits >> count) & 31) as usize] as char);
        }
    }
    if count > 0 {
        out.push(ALPHABET[((bits << (5 - count)) & 31) as usize] as char)
    }
    out
}
fn secret() -> Result<Secret> {
    Ok(Secret(base32(&random(32)?)))
}
fn hash(token: &Secret) -> Vec<u8> {
    Sha256::digest(token.0.as_bytes()).to_vec()
}
fn authentic(token: &Secret, expected: &[u8]) -> bool {
    hash(token).as_slice().ct_eq(expected).into()
}
fn app(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}
fn invalid(message: &str) -> RpcError {
    RpcError::protocol(-32602, "invalid_params", message)
}
fn storage(error: tower_store::Error) -> RpcError {
    match error {
        tower_store::Error::NotFound => app("not_found", "Record not found"),
        tower_store::Error::Conflict(message) => app("conflict", message),
        _ => RpcError::protocol(-32603, "internal_error", "Storage operation failed"),
    }
}
fn value<T: Serialize>(v: T) -> std::result::Result<Value, RpcError> {
    serde_json::to_value(v)
        .and_then(serde_json::from_value)
        .map_err(|_| RpcError::protocol(-32603, "internal_error", "Response encoding failed"))
}
fn params<T: DeserializeOwned>(v: &Value) -> std::result::Result<T, RpcError> {
    let json = serde_json::to_value(v).map_err(|_| invalid("Invalid parameters"))?;
    serde_json::from_value(if json.is_null() {
        serde_json::json!({})
    } else {
        json
    })
    .map_err(|_| invalid("Parameters do not match the method schema"))
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct Config {
    history: History,
    http: Http,
}
#[derive(Deserialize)]
#[serde(default)]
struct History {
    owned_retention_days: u64,
}
impl Default for History {
    fn default() -> Self {
        Self {
            owned_retention_days: 30,
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct Http {
    enabled: bool,
}

enum Actor {
    Human,
    Agent {
        record: Box<AgentRecord>,
        strong: bool,
    },
    Token(TokenInfo),
    Unregistered,
}
impl Actor {
    fn id(&self) -> String {
        match self {
            Self::Human => "human".into(),
            Self::Agent { record, .. } => record.id.clone(),
            Self::Token(info) => format!("token:{}", info.name),
            Self::Unregistered => "unregistered".into(),
        }
    }
    fn authorized(&self, scope: &str) -> bool {
        match self {
            Self::Human => true,
            Self::Token(t) => t.scope.iter().any(|s| s == scope || s == "admin"),
            _ => scope == "read",
        }
    }
}
pub(super) struct Service {
    paths: Paths,
    store: Arc<dyn Store>,
    startup: String,
    retention_ms: u64,
    /// Serialize identity resolution and writes, including filesystem credential publication.
    mutation: Mutex<()>,
}
impl Service {
    pub fn new(paths: Paths, store: Arc<dyn Store>, startup: &str) -> Result<Self> {
        let config: Config =
            match tower_pal::read_config(&paths.config_home.join("tower/config.toml"))? {
                Some(text) => toml::from_str(&text)
                    .map_err(|_| Error::Other("invalid tower configuration".into()))?,
                None => Config::default(),
            };
        if config.http.enabled {
            return Err(Error::Other(
                "this build implements UDS only; HTTP is not available yet".into(),
            ));
        }
        let retention_ms = config
            .history
            .owned_retention_days
            .checked_mul(86_400_000)
            .ok_or_else(|| Error::Other("owned retention duration is too large".into()))?;
        let host_path = paths.state.join("host_id");
        let host_id = match tower_pal::read_private(&host_path) {
            Ok(bytes) => {
                String::from_utf8(bytes).map_err(|_| Error::Other("invalid host_id file".into()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let id = base32(&random(16)?);
                tower_pal::write_private(&host_path, id.as_bytes())?;
                id
            }
            Err(e) => return Err(e.into()),
        };
        if host_id.len() != 26
            || !host_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        {
            return Err(Error::Other("invalid host_id file".into()));
        }
        let node = format!(
            "h{}-{}",
            &host_id[..10],
            base32(&random(3)?)[..4].to_owned()
        );
        store.initialize(&host_id, &node)?;
        Ok(Self {
            paths,
            store,
            startup: startup.into(),
            retention_ms,
            mutation: Mutex::new(()),
        })
    }
    pub fn recover_registry(&self) -> Result<()> {
        self.store.mark_unconfirmed(now_ms())?;
        Ok(())
    }
    pub fn maintain(&self) -> Result<()> {
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| Error::Other("mutation lock poisoned".into()))?;
        let now = now_ms();
        for r in self.store.agents(&AgentFilter::default())? {
            if r.ownership != "registered" {
                continue;
            }
            let dead = match (r.pid, r.pid_start_time) {
                (Some(pid), Some(start)) => match tower_pal::process_identity(pid) {
                    Ok(identity) => identity.start_time != start,
                    Err(e) => matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                    ),
                },
                _ => now.saturating_sub(r.last_seen) >= 180_000,
            };
            if dead {
                self.store.mark_lost(&r.id, now)?;
            }
        }
        for id in self.store.expire(now, self.retention_ms)? {
            tower_pal::remove_private(&self.paths.token_path(&id)?)?;
        }
        Ok(())
    }
    fn all(&self) -> std::result::Result<Vec<AgentRecord>, RpcError> {
        self.store
            .agents(&AgentFilter {
                include_exited: true,
                ..Default::default()
            })
            .map_err(storage)
    }
    fn resolve(&self, id: &str) -> std::result::Result<StoredAgent, RpcError> {
        if id.is_empty() {
            return Err(invalid("Session ID cannot be empty"));
        }
        match self.store.agent(id) {
            Ok(v) => return Ok(v),
            Err(tower_store::Error::NotFound) => {}
            Err(e) => return Err(storage(e)),
        }
        let candidates: Vec<_> = self
            .all()?
            .into_iter()
            .filter(|r| r.id.starts_with(id))
            .collect();
        match candidates.as_slice() {
            [r] => self.store.agent(&r.id).map_err(storage),
            [] => Err(app("not_found", "Session not found")),
            _ => {
                let mut e = app("ambiguous_id", "Session prefix matches multiple records");
                e.data.details.insert(
                    "candidates".into(),
                    Value::Array(
                        candidates
                            .into_iter()
                            .map(|r| Value::String(r.id))
                            .collect(),
                    ),
                );
                Err(e)
            }
        }
    }
    fn actor(&self, context: &CallerContext, peer: Peer) -> std::result::Result<Actor, RpcError> {
        if peer.uid != tower_pal::uid() {
            return Err(app("unauthorized", "Wrong system user"));
        }
        if let Some(token) = &context.agent_token {
            if context.bearer_token.is_some() {
                return Err(app("unauthorized", "Conflicting credentials"));
            }
            let digest = hash(token);
            let stored = match &context.agent_id {
                Some(id) => self.store.agent(id),
                None => self.store.agent_by_token_hash(&digest),
            }
            .map_err(|e| match e {
                tower_store::Error::NotFound => app("unauthorized", "Invalid agent credential"),
                other => storage(other),
            })?;
            if !bool::from(digest.as_slice().ct_eq(&stored.token_hash)) {
                return Err(app("unauthorized", "Invalid agent credential"));
            }
            return Ok(Actor::Agent {
                record: Box::new(stored.record),
                strong: true,
            });
        }
        if let Some(token) = &context.bearer_token {
            let stored = self
                .store
                .token_by_hash(&hash(token))
                .map_err(|e| match e {
                    tower_store::Error::NotFound => {
                        app("unauthorized", "Invalid, revoked, or expired token")
                    }
                    other => storage(other),
                })?;
            if stored.info.expires_at.is_some_and(|end| end <= now_ms()) {
                return Err(app("unauthorized", "Invalid, revoked, or expired token"));
            }
            return Ok(Actor::Token(stored.info));
        }
        let weak = |result: tower_store::Result<StoredAgent>| match result {
            Ok(stored) => Ok(Actor::Agent {
                record: Box::new(stored.record),
                strong: false,
            }),
            Err(tower_store::Error::NotFound) => Ok(Actor::Unregistered),
            Err(error) => Err(storage(error)),
        };
        if let Some(id) = &context.agent_id {
            return weak(self.store.agent(id));
        }
        if context.native_session_id.is_some() || context.harness.is_some() {
            return match (&context.harness, &context.native_session_id) {
                (Some(h), Some(n)) => weak(self.store.agent_by_native_id(h, n)),
                _ => Ok(Actor::Unregistered),
            };
        }
        let all = self.all()?;
        let mut pid = peer.pid;
        let mut seen = HashSet::new();
        for _ in 0..64 {
            let Some(current) = pid else { break };
            if !seen.insert(current) {
                break;
            }
            let Ok(p) = tower_pal::process_identity(current) else {
                break;
            };
            if let Some(record) = all
                .iter()
                .find(|r| r.pid == Some(p.pid) && r.pid_start_time == Some(p.start_time))
            {
                return Ok(Actor::Agent {
                    record: Box::new(record.clone()),
                    strong: false,
                });
            }
            if p.parent <= 1 {
                break;
            }
            pid = Some(p.parent);
        }
        Ok(Actor::Human)
    }
    fn credential(&self, p: &AgentCredential) -> std::result::Result<StoredAgent, RpcError> {
        let agent = self.resolve(&p.id)?;
        if !authentic(&p.token, &agent.token_hash) {
            return Err(app("unauthorized", "Invalid registration token"));
        }
        Ok(agent)
    }
    fn register_agent(
        &self,
        request: &Request,
        actor: &Actor,
        now: u64,
    ) -> std::result::Result<Value, RpcError> {
        let p: RegisterParams = params(&request.params)?;
        if matches!(&actor, Actor::Token(_)) {
            require(actor, "admin")?;
        }
        identifier(&p.harness, 64)?;
        if p.native_session_id
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.len() > 512 || v.contains('\0'))
        {
            return Err(invalid("Invalid native session ID"));
        }
        if p.display_name
            .as_ref()
            .is_some_and(|v| v.chars().count() > 200)
        {
            return Err(invalid("Display name is too long"));
        }
        if matches!(&actor, Actor::Unregistered)
            && (request
                .context
                .harness
                .as_ref()
                .is_some_and(|h| h != &p.harness)
                || request
                    .context
                    .native_session_id
                    .as_ref()
                    .is_some_and(|n| Some(n) != p.native_session_id.as_ref()))
        {
            return Err(app(
                "unauthorized",
                "Agent registration must match its caller context",
            ));
        }
        if let Actor::Agent { record, .. } = &actor
            && (p.harness != record.harness || p.native_session_id != record.native_session_id)
        {
            return Err(app("denied", "An agent can only register its own session"));
        }
        let cwd = self
            .paths
            .display_cwd(&p.cwd)
            .map_err(|_| invalid("cwd must be an absolute path or start with ~/"))?;
        let identity = p
            .pid
            .map(|pid| {
                tower_pal::process_identity(pid)
                    .map_err(|_| invalid("PID is not a live accessible process"))
            })
            .transpose()?;
        let host = self.store.host().map_err(storage)?;
        let id = format!(
            "{}-{}",
            host.node_name,
            base32(&random(10).map_err(|_| app("internal_error", "Random source unavailable"))?)
        );
        let token = secret().map_err(|_| app("internal_error", "Random source unavailable"))?;
        let mut candidate = AgentRecord {
            id: id.clone(),
            display_name: p.display_name,
            node: host.node_name,
            harness: p.harness,
            mode: None,
            ownership: "registered".into(),
            trust: "weak".into(),
            native_session_id: p.native_session_id,
            pid: identity.map(|p| p.pid),
            pid_start_time: identity.map(|p| p.start_time),
            cwd,
            status: "unknown".into(),
            status_detail: None,
            caps: Capabilities::unavailable("not_implemented"),
            pool_session: None,
            attach_count: 0,
            sources: vec!["cli".into()],
            registered_at: now,
            updated_at: now,
            last_seen: now,
            revision: 0,
            labels: BTreeMap::new(),
            task_refs: Vec::new(),
            sync_state: if host.mode == "allgather" {
                "pending"
            } else {
                "synced"
            }
            .into(),
        };
        // Resolve all identity keys and authenticate the resolved target before
        // mutating metadata or publishing a credential. The Store rechecks its ID.
        let target = self
            .store
            .registration_target(&candidate)
            .map_err(storage)?;
        if let Actor::Agent { record, .. } = &actor
            && target.as_ref().is_none_or(|a| a.record.id != record.id)
        {
            return Err(app("denied", "An agent can only register its own session"));
        }
        let (token, expected_id, created_path) = if let Some(existing) = target {
            if existing.record.ownership == "owned"
                && !matches!(&actor, Actor::Agent {record,strong:true} if record.id==existing.record.id)
            {
                return Err(app(
                    "denied",
                    "Owned registration requires its existing credential",
                ));
            }
            let path = self
                .paths
                .token_path(&existing.record.id)
                .map_err(|_| app("internal_error", "Token path failure"))?;
            let bytes = tower_pal::read_private(&path).map_err(|_| {
                app(
                    "internal_error",
                    "Existing registration credential is unavailable",
                )
            })?;
            let token = Secret(
                String::from_utf8(bytes)
                    .map_err(|_| app("internal_error", "Invalid registration credential file"))?,
            );
            if !authentic(&token, &existing.token_hash) {
                return Err(app(
                    "internal_error",
                    "Credential file and registry disagree",
                ));
            }
            // A report without a PID may retain a binding only if it is still live.
            if candidate.pid.is_none()
                && let (Some(pid), Some(start)) =
                    (existing.record.pid, existing.record.pid_start_time)
                && tower_pal::process_identity(pid).is_ok_and(|p| p.start_time == start)
            {
                candidate.pid = Some(pid);
                candidate.pid_start_time = Some(start);
            }
            (token, existing.record.id, None)
        } else {
            let path = self
                .paths
                .token_path(&id)
                .map_err(|_| app("internal_error", "Token path failure"))?;
            tower_pal::write_private(&path, token.0.as_bytes())
                .map_err(|_| app("internal_error", "Cannot write registration credential"))?;
            (token, id, Some(path))
        };
        let registered = match self.store.register(candidate, &hash(&token), &expected_id) {
            Ok(v) => v,
            Err(e) => {
                if let Some(path) = created_path {
                    let _ = tower_pal::remove_private(&path);
                }
                return Err(storage(e));
            }
        };
        value(RegisterResult {
            id: registered.agent.record.id,
            token,
            trust: registered.agent.record.trust,
        })
    }
    pub fn dispatch(&self, request: &Request, peer: Peer) -> std::result::Result<Value, RpcError> {
        // Authentication and authorization are re-evaluated on every request, not cached per socket.
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        let actor = self.actor(&request.context, peer)?;
        let now = now_ms();
        match request.method.as_str() {
            "hello" => {
                let p: HelloParams = params(&request.params)?;
                if p.protocol_version.major != PROTOCOL_VERSION.major {
                    return Err(app(
                        "protocol_incompatible",
                        "Protocol major versions differ",
                    ));
                }
                value(HelloResult {
                    protocol_version: PROTOCOL_VERSION,
                    capabilities: vec![
                        "registry.v1".into(),
                        "rules.local.v1".into(),
                        "tokens.v1".into(),
                        "daemon.control.v1".into(),
                    ],
                })
            }
            "daemon.info" => {
                require(&actor, "read")?;
                let host = self.store.host().map_err(storage)?;
                value(DaemonInfo {
                    version: env!("CARGO_PKG_VERSION").into(),
                    protocol_version: PROTOCOL_VERSION,
                    pid: tower_pal::pid(),
                    host_id: host.host_id,
                    node_name: host.node_name,
                    mode: host.mode,
                    pool: PoolInfo {
                        running: false,
                        proto_version: None,
                    },
                    allgather: AllgatherInfo {
                        connected: false,
                        policy_revision: host.policy_revision,
                    },
                    startup: self.startup.clone(),
                })
            }
            "daemon.shutdown" => {
                require(&actor, "admin")?;
                value(serde_json::json!({"stopping":true}))
            }
            "identity.whoami" => {
                require(&actor, "read")?;
                let actor_id = actor.id();
                match actor {
                    Actor::Human => value(serde_json::json!({"id":actor_id,"trust":"local"})),
                    Actor::Agent { record, strong } => value(
                        serde_json::json!({"id":actor_id,"harness":record.harness,"ownership":record.ownership,"trust":if strong {"strong"} else {"weak"}}),
                    ),
                    Actor::Token(info) => {
                        value(serde_json::json!({"id":actor_id,"scope":info.scope}))
                    }
                    Actor::Unregistered => {
                        value(serde_json::json!({"state":"unregistered","trust":"weak"}))
                    }
                }
            }
            "agent.list" => {
                require(&actor, "read")?;
                let mut p: ListParams = params(&request.params)?;
                let prefix = p
                    .filter
                    .cwd_prefix
                    .take()
                    .map(|prefix| {
                        self.paths
                            .absolute_cwd(&prefix)
                            .map_err(|_| invalid("Invalid cwd prefix"))
                    })
                    .transpose()?;
                let mut records = self.store.agents(&p.filter).map_err(storage)?;
                if let Some(prefix) = prefix {
                    records.retain(|record| {
                        self.paths
                            .absolute_cwd(&record.cwd)
                            .is_ok_and(|cwd| cwd.starts_with(&prefix))
                    });
                }
                value(records)
            }
            "agent.get" => {
                require(&actor, "read")?;
                let p: IdParams = params(&request.params)?;
                value(self.resolve(&p.id)?.record)
            }
            "agent.register" => self.register_agent(request, &actor, now),
            "agent.update" => {
                let mut p: UpdateParams = params(&request.params)?;
                let agent = self.credential(&AgentCredential {
                    id: p.id.clone(),
                    token: p.token.clone(),
                })?;
                if agent.record.ownership == "owned" {
                    return Err(app(
                        "denied",
                        "Owned lifecycle updates require the launcher source",
                    ));
                }
                if p.status.as_ref().is_some_and(|s| {
                    !matches!(
                        s.as_str(),
                        "starting"
                            | "idle"
                            | "working"
                            | "waiting_input"
                            | "waiting_approval"
                            | "unknown"
                            | "exited"
                    )
                }) {
                    return Err(invalid("Unsupported status update"));
                }
                if matches!(&p.display_name,Patch::Set(v) if v.chars().count()>200) {
                    return Err(invalid("Display name is too long"));
                }
                if let Patch::Set(Value::Object(detail)) = &mut p.detail {
                    // This foundation accepts bounded operational metadata, never prompts/transcripts.
                    detail.retain(|key, _| {
                        matches!(key.as_str(), "last_tool" | "model" | "context_tokens")
                    });
                    for (key, v) in detail.iter() {
                        if key == "context_tokens" {
                            if !matches!(
                                v,
                                Value::Number(Number::U64(_)) | Value::Number(Number::I64(0..))
                            ) {
                                return Err(invalid(
                                    "context_tokens must be a non-negative integer",
                                ));
                            }
                        } else if !matches!(v,Value::String(s) if s.len()<=128 && s.bytes().all(|b|b.is_ascii_alphanumeric()||b"_./:- ".contains(&b)))
                        {
                            return Err(invalid("Invalid operational detail"));
                        }
                    }
                }
                p.id = agent.record.id;
                value(self.store.update(&p, now).map_err(storage)?)
            }
            "agent.heartbeat" | "agent.unregister" => {
                let p: AgentCredential = params(&request.params)?;
                let agent = self.credential(&p)?;
                if agent.record.ownership == "owned" {
                    return Err(app(
                        "denied",
                        "Owned lifecycle requires the launcher source",
                    ));
                }
                if request.method == "agent.heartbeat" {
                    self.store
                        .heartbeat(&agent.record.id, now)
                        .map_err(storage)?;
                } else {
                    self.store
                        .unregister(&agent.record.id, now)
                        .map_err(storage)?;
                }
                value(serde_json::json!({"ok":true}))
            }
            "rules.list" => {
                require(&actor, "read")?;
                value(self.store.rules().map_err(storage)?)
            }
            "rules.put" => {
                require(&actor, "admin")?;
                let mut p: RulePutParams = params(&request.params)?;
                if p.rule.id.is_empty() {
                    p.rule.id = format!(
                        "rule_{}",
                        base32(
                            &random(10)
                                .map_err(|_| app("internal_error", "Random source unavailable"))?
                        )
                    );
                }
                validate_rule(&p.rule)?;
                p.rule.created_by = actor.id();
                p.rule.created_at = now;
                self.store.put_rule(p.rule.clone()).map_err(storage)?;
                value(p.rule)
            }
            "rules.remove" => {
                require(&actor, "admin")?;
                let p: IdParams = params(&request.params)?;
                self.store.remove_rule(&p.id).map_err(storage)?;
                value(serde_json::json!({"ok":true}))
            }
            "rules.replace" => {
                require(&actor, "admin")?;
                let mut p: RulesReplaceParams = params(&request.params)?;
                if p.rules.len() > 1024 {
                    return Err(invalid("Too many rules"));
                }
                let mut ids = HashSet::new();
                for rule in &mut p.rules {
                    validate_rule(rule)?;
                    if !ids.insert(rule.id.clone()) {
                        return Err(invalid("Duplicate rule ID"));
                    }
                    rule.created_by = actor.id();
                    rule.created_at = now;
                }
                self.store.replace_rules(&p.rules).map_err(storage)?;
                value(serde_json::json!({"ok":true}))
            }
            "token.issue" => {
                require(&actor, "admin")?;
                let mut p: TokenIssueParams = params(&request.params)?;
                identifier(&p.name, 64)?;
                if let Some(duration) = &p.expires {
                    if p.expires_at.is_some() {
                        return Err(invalid("Choose expires or expires_at, not both"));
                    }
                    p.expires_at = Some(
                        now.checked_add(parse_duration(duration)?)
                            .ok_or_else(|| invalid("Expiry overflow"))?,
                    );
                }
                if p.scope.is_empty()
                    || p.scope.iter().any(|s| {
                        !matches!(
                            s.as_str(),
                            "read" | "send" | "subscribe" | "approve" | "admin"
                        )
                    })
                {
                    return Err(invalid("Unknown or empty token scope"));
                }
                if p.expires_at.is_some_and(|end| end <= now) {
                    return Err(invalid("Token expiry must be in the future"));
                }
                let token =
                    secret().map_err(|_| app("internal_error", "Random source unavailable"))?;
                self.store
                    .issue_token(
                        TokenInfo {
                            name: p.name,
                            scope: p.scope,
                            created_at: now,
                            expires_at: p.expires_at,
                        },
                        &hash(&token),
                    )
                    .map_err(storage)?;
                value(TokenResult { token })
            }
            "token.list" => {
                require(&actor, "admin")?;
                value(
                    self.store
                        .tokens()
                        .map_err(storage)?
                        .into_iter()
                        .map(|v| v.info)
                        .collect::<Vec<_>>(),
                )
            }
            "token.revoke" => {
                require(&actor, "admin")?;
                let p: NameParams = params(&request.params)?;
                self.store.revoke_token(&p.name).map_err(storage)?;
                value(serde_json::json!({"ok":true}))
            }
            _ => Err(RpcError::protocol(
                -32601,
                "method_not_found",
                "Method is not implemented",
            )),
        }
    }
}
fn require(actor: &Actor, scope: &str) -> std::result::Result<(), RpcError> {
    if actor.authorized(scope) {
        Ok(())
    } else {
        Err(app("denied", "Caller lacks the required scope"))
    }
}
fn parse_duration(input: &str) -> std::result::Result<u64, RpcError> {
    if input.len() < 2 || !input.is_ascii() {
        return Err(invalid("Use an expiry such as 30d or 12h"));
    }
    let (number, unit) = input.split_at(input.len() - 1);
    let n: u64 = number
        .parse()
        .map_err(|_| invalid("Invalid expiry duration"))?;
    let scale = match unit {
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(invalid("Invalid expiry unit")),
    };
    n.checked_mul(scale)
        .filter(|v| *v > 0)
        .ok_or_else(|| invalid("Invalid expiry duration"))
}
fn identifier(s: &str, max: usize) -> std::result::Result<(), RpcError> {
    if s.is_empty()
        || s.len() > max
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
    {
        Err(invalid("Invalid identifier"))
    } else {
        Ok(())
    }
}
fn validate_rule(rule: &Rule) -> std::result::Result<(), RpcError> {
    identifier(&rule.id, 128)?;
    if !matches!(rule.action.as_str(), "allow" | "ask" | "deny") {
        return Err(invalid("Unknown rule action"));
    }
    for selector in [&rule.from, &rule.to] {
        if selector.is_empty()
            || selector.len() > 4096
            || selector.contains('\0')
            || selector.chars().any(char::is_control)
        {
            return Err(invalid("Invalid rule selector"));
        }
        if selector != "*" && (selector.contains('*') || selector.contains('?')) {
            return Err(invalid("Use whole-selector * or harness/cwd selectors"));
        }
        if selector == "harness:" || selector == "cwd:" || selector == "token:" {
            return Err(invalid("Empty rule selector"));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
