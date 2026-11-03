mod clipboard;
mod enrollment;
mod events;
mod execution;
mod managed;
mod messaging;
mod scan;
mod sync;
mod terminal;
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
    messaging: messaging::Limits,
    scan: scan::Config,
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

enum Actor {
    Human,
    Allgather {
        scope: Vec<String>,
    },
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
            Self::Allgather { .. } => "allgather".into(),
            Self::Agent { record, .. } => record.id.clone(),
            Self::Token(info) => format!("token:{}", info.name),
            Self::Unregistered => "unregistered".into(),
        }
    }
    fn authorized(&self, scope: &str) -> bool {
        match self {
            Self::Human => true,
            Self::Allgather { scope: scopes } => scopes
                .iter()
                .any(|s| s == scope || (scope != "acp:control" && s == "admin")),
            Self::Token(t) => t.scope.iter().any(|s| s == scope || s == "admin"),
            _ => matches!(scope, "read" | "subscribe"),
        }
    }
}
pub(super) struct Service {
    paths: Paths,
    pub(crate) clipboard: Arc<tower_clipboard::Broker>,
    pub(crate) metrics: crate::metrics::Metrics,
    store: Arc<Store>,
    backends: tower_sessions::Backends,
    startup: String,
    retention_ms: u64,
    allgather_config: crate::allgather::Config,
    pending_enrollments: Mutex<BTreeMap<String, crate::allgather::enrollment::Pending>>,
    connected: std::sync::atomic::AtomicBool,
    remote_epoch: std::sync::atomic::AtomicU64,
    policy_epoch: std::sync::atomic::AtomicU64,
    message_limits: messaging::Limits,
    traffic: Mutex<messaging::Traffic>,
    scan: scan::Config,
    last_scan: Mutex<std::time::Instant>,
    /// Serialize identity resolution and writes, including filesystem credential publication.
    mutation: Mutex<()>,
    events: tokio::sync::broadcast::Sender<Arc<EventNotification>>,
    published: Mutex<events::Published>,
}
impl Service {
    pub fn new(
        paths: Paths,
        store: Arc<Store>,
        startup: &str,
        backends: tower_sessions::Backends,
    ) -> Result<Self> {
        let config: Config =
            match tower_pal::read_config(&paths.config_home.join("tower/config.toml"))? {
                Some(text) => toml::from_str(&text)
                    .map_err(|_| Error::Other("invalid tower configuration".into()))?,
                None => Config::default(),
            };
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
            paths: paths.clone(),
            clipboard: Arc::new(tower_clipboard::Broker::new(paths.state.join("clipboard"))),
            metrics: crate::metrics::Metrics::new(paths.home.clone()),
            store,
            backends,
            startup: startup.into(),
            retention_ms,
            allgather_config: crate::allgather::Config::load(&paths)?,
            pending_enrollments: Mutex::new(BTreeMap::new()),
            connected: std::sync::atomic::AtomicBool::new(false),
            remote_epoch: std::sync::atomic::AtomicU64::new(0),
            policy_epoch: std::sync::atomic::AtomicU64::new(0),
            message_limits: config.messaging,
            traffic: Mutex::new(messaging::Traffic::default()),
            scan: config.scan,
            last_scan: Mutex::new(std::time::Instant::now()),
            mutation: Mutex::new(()),
            events: tokio::sync::broadcast::channel(256).0,
            published: Mutex::new(events::Published::default()),
        })
    }
    pub(crate) fn authorize_host_metrics(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<(), RpcError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Empty {}
        let _: Empty = params(&request.params)?;
        require(&self.actor(&request.context, peer)?, "read")
    }
    pub fn recover_registry(&self) -> Result<()> {
        self.store.mark_unconfirmed(now_ms())?;
        self.sync_managed()?;
        Ok(())
    }
    pub fn maintain(&self) -> Result<()> {
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| Error::Other("mutation lock poisoned".into()))?;
        self.sync_managed()?;
        self.scan_processes()?;
        self.maintain_clipboard().map_err(Error::Rpc)?;
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
        for session in self.store.expiring_managed(now, self.retention_ms)? {
            if let Some(backend) = self.backends.get(&session.protocol) {
                backend.cleanup(&session).map_err(Error::Rpc)?;
            }
        }
        for id in self.store.expire(now, self.retention_ms)? {
            tower_pal::remove_private(&self.paths.token_path(&id)?)?;
        }
        for a in self.store.expire_approvals(now)? {
            self.emit("approval.resolved",&a.id,serde_json::json!({"approval_id":a.id,"state":a.state,"reason":a.reason,"from":a.from,"to":a.to})).map_err(Error::Rpc)?;
        }
        self.publish().map_err(Error::Rpc)?;
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
            let matches = all
                .iter()
                .filter(|r| r.pid == Some(p.pid) && r.pid_start_time == Some(p.start_time))
                .collect::<Vec<_>>();
            if matches.len() > 1 || matches.iter().any(|r| r.shared_process) {
                return Ok(Actor::Unregistered);
            }
            if let Some(record) = matches.first() {
                return Ok(Actor::Agent {
                    record: Box::new((*record).clone()),
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
        self.register_source(request, actor, now, "cli")
    }
    fn register_source(
        &self,
        request: &Request,
        actor: &Actor,
        now: u64,
        source: &str,
    ) -> std::result::Result<Value, RpcError> {
        if matches!(actor, Actor::Token(_)) {
            require(actor, "admin")?;
        }
        let mut p: RegisterParams = params(&request.params)?;
        let supplied_delivery = p.delivery.is_some();
        let mut endpoint = p.delivery.take();
        if p.harness == "codex" && source != "scan" {
            if let Some(e) = &endpoint {
                if p.native_session_id.is_none() {
                    p.native_session_id = e.thread_id.clone();
                }
                if e.thread_id.is_some() && e.thread_id != p.native_session_id {
                    return Err(invalid(
                        "Delivery thread must match the native session identity",
                    ));
                }
            }
            if let Some(id) = &p.native_session_id {
                let e = endpoint.get_or_insert_with(DeliveryEndpoint::default);
                e.thread_id = Some(id.clone());
            }
        }
        let message_reason = if source == "scan" {
            "scan_only"
        } else {
            match p.harness.as_str() {
                "claude-code" => "no_messaging_token",
                "codex" => "no_thread_id",
                _ => "harness_unsupported",
            }
        };
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
        if let (Some(adapter), Some(endpoint)) = (self.backends.delivery(), &endpoint) {
            adapter.validate(endpoint)?;
        }
        if p.protocol
            .as_deref()
            .is_some_and(|p| !tower_sessions::valid_protocol(p))
        {
            return Err(invalid("Invalid session protocol"));
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
        let shared_codex = p.harness == "codex"
            && (endpoint.as_ref().is_some_and(|e| e.app_server.is_some())
                || (p.native_session_id.is_some()
                    && identity.is_some_and(|i| {
                        tower_pal::inspect_process(i.pid)
                            .is_ok_and(|p| p.argv.iter().take(3).any(|s| s == "app-server"))
                    })));
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
            mode: shared_codex.then(|| "app-server".into()),
            ownership: "registered".into(),
            protocol: p.protocol,
            trust: "weak".into(),
            native_session_id: p.native_session_id,
            pid: identity.map(|p| p.pid),
            pid_start_time: identity.map(|p| p.start_time),
            shared_process: p.shared_process || shared_codex,
            cwd,
            status: "unknown".into(),
            status_detail: None,
            caps: Capabilities {
                kill: identity.is_some() && !shared_codex && !p.shared_process,
                ..Capabilities::unavailable(message_reason)
            },
            pool_session: None,
            attach_count: 0,
            sources: vec![source.into()],
            registered_at: now,
            updated_at: now,
            last_seen: now,
            revision: 0,
            labels: BTreeMap::new(),
            tags: Default::default(),
            needs_attention: false,
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
            if existing.record.ownership == "registered" && !supplied_delivery && source != "scan" {
                let mut previous = self.store.delivery(&existing.record.id).map_err(storage)?;
                if existing.record.harness == "codex" && previous.thread_id.is_none() {
                    previous.thread_id = candidate
                        .native_session_id
                        .clone()
                        .or_else(|| existing.record.native_session_id.clone());
                }
                endpoint = Some(previous);
            }
            candidate.shared_process |= existing.record.shared_process;
            candidate.caps.kill &= !candidate.shared_process;
            // A report without a PID may retain a binding only if it is still live.
            if candidate.pid.is_none()
                && let (Some(pid), Some(start)) =
                    (existing.record.pid, existing.record.pid_start_time)
                && tower_pal::process_identity(pid).is_ok_and(|p| p.start_time == start)
            {
                candidate.pid = Some(pid);
                candidate.pid_start_time = Some(start);
                candidate.shared_process = existing.record.shared_process;
                candidate.caps.kill = existing.record.caps.kill;
                candidate.mode = existing.record.mode.clone().or(candidate.mode);
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
        if registered.agent.record.ownership == "registered"
            && let Some(endpoint) = endpoint
            && self.backends.delivery().is_some()
        {
            self.set_delivery(DeliveryParams {
                id: registered.agent.record.id.clone(),
                token: token.clone(),
                delivery: endpoint,
            })?;
        }
        value(RegisterResult {
            id: registered.agent.record.id,
            token,
            trust: registered.agent.record.trust,
        })
    }
    pub(crate) fn conversations(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<Value, RpcError> {
        require(&self.actor(&request.context, peer)?, "read")?;
        let mut p: ConversationListParams = params(&request.params)?;
        if p.limit.is_some_and(|n| n == 0 || n > 1000) {
            return Err(invalid("limit must be between 1 and 1000"));
        }
        if let Some(prefix) = &p.cwd_prefix {
            p.cwd_prefix = Some(
                self.paths
                    .absolute_cwd(prefix)
                    .map_err(|_| invalid("Invalid directory prefix"))?
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        if let Some(protocol) = &p.protocol {
            self.backends.select(Some(protocol))?;
        }
        let known = self.all()?;
        let mut result = ConversationList::default();
        for backend in self
            .backends
            .iter()
            .filter(|b| p.protocol.as_deref().is_none_or(|s| s == b.protocol()))
        {
            let mut part = backend.conversations(&p)?;
            for c in &mut part.conversations {
                c.tower_id = known
                    .iter()
                    .find(|r| {
                        r.harness == c.harness
                            && r.native_session_id.as_ref() == Some(&c.native_session_id)
                    })
                    .map(|r| r.id.clone());
            }
            result.conversations.extend(part.conversations);
            result.errors.extend(part.errors);
            result.truncated |= part.truncated;
        }
        result.conversations.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.harness.cmp(&b.harness))
                .then_with(|| a.native_session_id.cmp(&b.native_session_id))
        });
        result.truncated |= result.conversations.len() > p.limit.unwrap_or(100);
        result.conversations.truncate(p.limit.unwrap_or(100));
        require(&self.actor(&request.context, peer)?, "read")?;
        value(result)
    }
    pub fn dispatch(&self, request: &Request, peer: Peer) -> std::result::Result<Value, RpcError> {
        let result = self.dispatch_inner(request, peer);
        if result.is_ok() {
            self.publish()?;
        }
        result
    }
    fn dispatch_inner(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<Value, RpcError> {
        // Authentication and authorization are re-evaluated on every request, not cached per socket.
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        if request.method == "runtime.authorize" {
            return self.authorize_runtime(request, peer);
        }
        let actor = self.actor(&request.context, peer)?;
        self.dispatch_actor(request, actor, peer)
    }
    fn dispatch_actor(
        &self,
        request: &Request,
        actor: Actor,
        peer: Peer,
    ) -> std::result::Result<Value, RpcError> {
        let now = now_ms();
        if matches!(
            request.method.as_str(),
            "agent.update" | "agent.delivery" | "agent.heartbeat" | "agent.unregister"
        ) {
            if matches!(actor, Actor::Token(_)) {
                require(&actor, "admin")?;
            }
            if let Actor::Agent { record, .. } = &actor {
                let p: IdParams = params(&request.params)?;
                if self.resolve(&p.id)?.record.id != record.id {
                    return Err(app(
                        "denied",
                        "Agent credentials cannot update another session",
                    ));
                }
            }
        }
        match request.method.as_str() {
            "allgather.login" | "allgather.logout" | "allgather.status" => {
                self.enrollment(request, &actor)
            }
            "agent.hook" => self.hook_report(request, &actor),
            "agent.delivery" => self.set_delivery(params(&request.params)?),
            "approval.list" | "approval.status" | "approval.resolve" => {
                self.approval_dispatch(request, &actor)
            }
            "daemon.service_access" => {
                require(&actor, "admin")?;
                value(serde_json::json!({"authorized":true}))
            }
            "subscribe" => {
                require(&actor, "subscribe")?;
                let p: SubscribeParams = params(&request.params)?;
                if p.topics.is_empty()
                    || p.topics.iter().any(|t| {
                        !matches!(
                            t.as_str(),
                            "agents" | "sessions" | "messages" | "approvals" | "sync"
                        )
                    })
                {
                    return Err(app(
                        "unsupported",
                        "Supported subscription topics: agents, sessions, messages, approvals, sync",
                    ));
                }
                if let Some(id) = &p.approval_id {
                    if p.topics != ["approvals"] {
                        return Err(invalid(
                            "An approval filter requires only the approvals topic",
                        ));
                    }
                    let mut check = request.clone();
                    check.method = "approval.status".into();
                    check.params = value(IdParams { id: id.clone() })?;
                    self.approval_dispatch(&check, &actor)?;
                }
                self.sync_managed().map_err(storage)?;
                self.publish()?;
                let state = self
                    .published
                    .lock()
                    .map_err(|_| app("internal_error", "Event state unavailable"))?;
                value(SubscribeResult {
                    subscription_id: format!(
                        "sub_{}",
                        base32(
                            &random(10)
                                .map_err(|_| app("internal_error", "Random source unavailable"))?
                        )
                    ),
                    cursor: state.seq,
                    agents: state.records.values().cloned().collect(),
                })
            }
            "unsubscribe" => {
                require(&actor, "subscribe")?;
                value(serde_json::json!({"unsubscribed":true}))
            }
            "harness.list" => {
                require(&actor, "read")?;
                value(
                    self.backends
                        .iter()
                        .flat_map(|b| b.harnesses())
                        .collect::<Vec<_>>(),
                )
            }
            "conversation.list" => self.conversations(request, peer),
            "session.list_resumable" => {
                require(&actor, "read")?;
                let filter: AgentFilter = params(&request.params)?;
                let records = self
                    .store
                    .agents(&AgentFilter {
                        include_exited: true,
                        ..filter
                    })
                    .map_err(storage)?;
                value(
                    records
                        .into_iter()
                        .filter(|r| {
                            r.ownership == "owned"
                                && r.caps.resume
                                && matches!(r.status.as_str(), "exited" | "lost")
                        })
                        .collect::<Vec<_>>(),
                )
            }
            "session.open" => {
                require(&actor, "admin")?;
                let p: SessionOpenParams = params(&request.params)?;
                let cwd = self
                    .paths
                    .display_cwd(&p.cwd)
                    .map_err(|_| invalid("Invalid working directory"))?;
                self.session_policy(
                    &actor,
                    crate::policy::Subject {
                        id: "*",
                        harness: Some(&p.harness),
                        cwd: Some(&cwd),
                    },
                )?;
                self.managed_open(p)
            }
            "session.restart" | "session.resume" => {
                require(&actor, "admin")?;
                let p: SessionRestartParams = params(&request.params)?;
                let record = self.resolve(&p.id)?.record;
                self.session_policy(
                    &actor,
                    crate::policy::Subject {
                        id: &record.id,
                        harness: Some(&record.harness),
                        cwd: Some(&record.cwd),
                    },
                )?;
                if self.store.host().map_err(storage)?.mode == "allgather"
                    && !self.policy_ready()
                    && (p.args.is_some()
                        || p.mode
                            .as_ref()
                            .is_some_and(|m| Some(m) != record.mode.as_ref()))
                {
                    return Err(app(
                        "denied",
                        "Reconnect to Allgather before changing launch settings",
                    ));
                }
                self.managed_restart(p, request.method == "session.restart")
            }
            "session.tags.patch" => {
                require(&actor, "admin")?;
                let p: SessionTagsPatch = params(&request.params)?;
                let id = self.resolve(&p.id)?.record.id;
                value(
                    self.store
                        .edit_session(&id, &p.add, &p.remove, false, now)
                        .map_err(storage)?,
                )
            }
            "session.attention.clear" => {
                require(&actor, "admin")?;
                let p: IdParams = params(&request.params)?;
                let id = self.resolve(&p.id)?.record.id;
                value(
                    self.store
                        .edit_session(&id, &[], &[], true, now)
                        .map_err(storage)?,
                )
            }
            "session.attach_info" => {
                require(&actor, "admin")?;
                self.clipboard_attach_info(&request.params)
            }
            "session.kill" => {
                require(&actor, "admin")?;
                #[derive(Deserialize)]
                struct Kill {
                    id: String,
                    #[serde(default)]
                    confirm: bool,
                }
                let p: Kill = params(&request.params)?;
                let record = self.resolve(&p.id)?.record;
                if record.ownership == "registered" {
                    if !record.caps.kill {
                        return Err(app(
                            "not_owned",
                            "External session has no managed stop capability",
                        ));
                    }
                    if !p.confirm {
                        return Err(app(
                            "confirmation_required",
                            "Use --yes to stop an externally managed process",
                        ));
                    }
                    let (Some(pid), Some(start)) = (record.pid, record.pid_start_time) else {
                        return Err(app(
                            "unsupported",
                            "Target has no verified process identity",
                        ));
                    };
                    tower_pal::terminate_process(pid, start).map_err(|_| {
                        app("stop_unconfirmed", "Cannot signal this process incarnation")
                    })?;
                    self.store.expect_stop(&record.id).map_err(storage)?;
                    value(serde_json::json!({"id":record.id,"state":"signal_sent"}))
                } else {
                    self.managed_kill(&p.id)
                }
            }
            "session.snapshot" => {
                require(&actor, "read")?;
                let p: IdParams = params(&request.params)?;
                self.terminal_snapshot(&p.id)
            }
            "hello" => {
                let p: HelloParams = params(&request.params)?;
                if p.protocol_version.major != PROTOCOL_VERSION.major {
                    return Err(app(
                        "protocol_incompatible",
                        "Protocol major versions differ",
                    ));
                }
                let mut capabilities = vec![
                    "registry.v1".into(),
                    "messages.v1".into(),
                    "approvals.v1".into(),
                    "rules.local.v1".into(),
                    "tokens.v1".into(),
                    "daemon.control.v1".into(),
                    "events.registry.v1".into(),
                    "sessions.managed.v1".into(),
                    "clipboard.images.v1".into(),
                    "sessions.metadata.v1".into(),
                    "host.metrics.v1".into(),
                ];
                capabilities.extend(
                    self.backends
                        .iter()
                        .flat_map(|b| b.features())
                        .map(|f| (*f).into()),
                );
                value(HelloResult {
                    protocol_version: PROTOCOL_VERSION,
                    capabilities,
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
                    pool: self.terminal_pool_info(),
                    allgather: AllgatherInfo {
                        connected: self.connected.load(std::sync::atomic::Ordering::Relaxed),
                        policy_current: self.policy_ready(),
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
                        serde_json::json!({"id":actor_id,"display_name":record.display_name,"harness":record.harness,"ownership":record.ownership,"trust":if strong {"strong"} else {"weak"}}),
                    ),
                    Actor::Allgather { scope } => {
                        value(serde_json::json!({"id":"allgather","scope":scope}))
                    }
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
                self.sync_managed().map_err(storage)?;
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
                self.sync_managed().map_err(storage)?;
                let p: IdParams = params(&request.params)?;
                value(self.resolve(&p.id)?.record)
            }
            "agent.register" => self.register_agent(request, &actor, now),
            "agent.wrap" => {
                require(&actor, "admin")?;
                self.register_source(request, &actor, now, "wrapper")
            }
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
                if matches!(&p.detail,Patch::Set(v) if !matches!(v,Value::Object(_))) {
                    return Err(invalid("detail must be an object"));
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
                let delivery = self.prepare_delivery_update(&agent, &p)?;
                p.id = agent.record.id;
                value(
                    self.store
                        .update_with_delivery(&p, now, delivery)
                        .map_err(storage)?,
                )
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
                self.emit(
                    "rules.changed",
                    &p.rule.id,
                    serde_json::json!({"operation":"put"}),
                )?;
                self.refresh_approvals()?;
                value(p.rule)
            }
            "rules.remove" => {
                require(&actor, "admin")?;
                let p: IdParams = params(&request.params)?;
                self.store.remove_rule(&p.id).map_err(storage)?;
                self.refresh_approvals()?;
                self.emit(
                    "rules.changed",
                    &p.id,
                    serde_json::json!({"operation":"remove"}),
                )?;
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
                self.refresh_approvals()?;
                self.emit(
                    "rules.changed",
                    "local",
                    serde_json::json!({"operation":"replace"}),
                )?;
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
        if selector == "harness:" || selector == "cwd:" || selector == "token:" {
            return Err(invalid("Empty rule selector"));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
