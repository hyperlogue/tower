mod agent;
mod app_server;
mod client;
mod injection;
pub mod launcher;
pub mod local;
mod pool;
mod session;
pub use client::{attach, attach_with_terminal};
use serde::{Serialize, de::DeserializeOwned};
use tower_pal::Paths;
use tower_pool_proto::{Launch, Reply, Request, Session};
use tower_protocol::{
    Capabilities, PoolInfo, ProtocolVersion, RpcError, Secret, SessionOpenParams, Value,
    terminal::{AttachInfo, SnapshotResult},
};
use tower_sessions::{
    ManagedBackend, ManagedSession, Observation, PreparedSession, Result,
    terminal::{TerminalAccess, TerminalState},
};

pub struct TerminalBackend {
    paths: Paths,
    inventory: std::sync::Mutex<tower_agents::inventory::Inventory>,
    codex_inventory: std::sync::Mutex<
        Option<(
            std::path::PathBuf,
            std::time::Instant,
            tower_agents::inventory::InventoryResult,
        )>,
    >,
    changes: std::sync::OnceLock<tokio::sync::watch::Sender<u64>>,
}
impl TerminalBackend {
    pub fn new(paths: Paths) -> Self {
        Self {
            paths,
            inventory: Default::default(),
            codex_inventory: Default::default(),
            changes: Default::default(),
        }
    }
    /// One pool subscription and state watch shared by every `watch` caller.
    fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes
            .get_or_init(|| {
                let (changed, _) = tokio::sync::watch::channel(0u64);
                let bump = {
                    let changed = changed.clone();
                    std::sync::Arc::new(move || changed.send_modify(|n| *n = n.wrapping_add(1)))
                };
                let sessions = self.paths.state.join("sessions");
                match tower_pal::private_dir(&sessions)
                    .and_then(|_| tower_pal::TreeWatch::new(&sessions, "state.json"))
                {
                    Ok(mut states) => {
                        let bump = bump.clone();
                        tokio::spawn(async move {
                            loop {
                                states.changed().await;
                                bump();
                            }
                        });
                    }
                    Err(e) => eprintln!("tower: managed session state is unobservable: {e}"),
                }
                tokio::spawn(pool::watch(self.paths.clone(), bump));
                changed
            })
            .subscribe()
    }
    fn codex_history(&self, refresh: bool) -> Result<tower_agents::inventory::InventoryResult> {
        let config = tower_agents::codex_service::Config::load(&self.paths)
            .map_err(|_| error("invalid_config", "Invalid host Codex configuration"))?;
        let mut cache = self
            .codex_inventory
            .lock()
            .map_err(|_| error("internal_error", "Codex inventory unavailable"))?;
        if let Some((endpoint, at, found)) = &*cache
            && !refresh
            && endpoint == &config.endpoint
            && at.elapsed() < std::time::Duration::from_secs(2)
        {
            return Ok(found.clone());
        }
        let found = codex_block(async {
            let host = tower_agents::codex_host::ensure(&self.paths).await?;
            let mut client =
                tower_agents::codex_service::Client::connect(&host.config.endpoint).await?;
            let mut found = tower_agents::inventory::InventoryResult::default();
            let mut cursor = serde_json::Value::Null;
            let mut cursors = std::collections::BTreeSet::new();
            let mut ids = std::collections::BTreeSet::new();
            for _ in 0..50 {
                let page=client.request("thread/list",serde_json::json!({"cursor":cursor,"limit":100,"modelProviders":[],"sortKey":"updated_at","sortDirection":"desc","archived":false})).await?;
                let rows = page["data"]
                    .as_array()
                    .ok_or(tower_agents::codex_service::Error::Protocol)?;
                if rows.len() > 100 {
                    return Err(tower_agents::codex_service::Error::Limit);
                }
                for row in rows {
                    if let Some(candidate) = codex_candidate(row, &host.config.endpoint) {
                        if !ids.insert(candidate.native.id.clone()) {
                            return Err(tower_agents::codex_service::Error::Protocol);
                        }
                        found.candidates.push(candidate);
                        if found.candidates.len() >= 4096 {
                            found.truncated = true;
                            return Ok(found);
                        }
                    }
                }
                match page.get("nextCursor") {
                    None | Some(serde_json::Value::Null) => return Ok(found),
                    Some(serde_json::Value::String(next))
                        if !next.is_empty()
                            && next.len() <= 4096
                            && cursors.insert(next.clone()) =>
                    {
                        cursor = serde_json::Value::String(next.clone())
                    }
                    _ => return Err(tower_agents::codex_service::Error::Protocol),
                }
            }
            found.truncated = true;
            Ok(found)
        })?;
        *cache = Some((config.endpoint, std::time::Instant::now(), found.clone()));
        Ok(found)
    }
    fn codex_conversation(&self, id: &str) -> Result<tower_agents::inventory::Candidate> {
        if !tower_agents::events::native_id(id) {
            return Err(error("invalid_params", "Invalid native session identity"));
        }
        codex_block(async {
            let host = tower_agents::codex_host::ensure(&self.paths).await?;
            let mut client =
                tower_agents::codex_service::Client::connect(&host.config.endpoint).await?;
            let response = client
                .request(
                    "thread/read",
                    serde_json::json!({"threadId":id,"includeTurns":false}),
                )
                .await?;
            codex_candidate(&response["thread"], &host.config.endpoint)
                .filter(|candidate| candidate.native.id == id)
                .ok_or(tower_agents::codex_service::Error::Protocol)
        })
    }
    fn codex_mode(&self, request: &SessionOpenParams) -> Result<String> {
        if request
            .mode
            .as_deref()
            .is_some_and(|mode| !matches!(mode, "app-server" | "native"))
        {
            return Err(error(
                "invalid_params",
                "Codex mode must be app-server or native",
            ));
        }
        if request.mode.as_deref() == Some("native") {
            return Ok("native".into());
        }
        tower_agents::codex_service::Config::load(&self.paths).map_err(|_| {
            error(
                "invalid_config",
                "Invalid host Codex endpoint configuration",
            )
        })?;
        Ok("app-server".into())
    }
}
fn codex_block<T>(
    operation: impl std::future::Future<Output = tower_agents::codex_service::Result<T>>,
) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| error("backend_unavailable", "Cannot start Codex inventory reader"))?
        .block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(20), operation)
                .await
                .map_err(|_| error("conversation_unavailable", "Host Codex inventory timed out"))?
                .map_err(|_| {
                    error(
                        "conversation_unavailable",
                        "Host Codex inventory is unavailable",
                    )
                })
        })
}
fn codex_candidate(
    row: &serde_json::Value,
    endpoint: &std::path::Path,
) -> Option<tower_agents::inventory::Candidate> {
    if row["ephemeral"] == true
        || !row["parentThreadId"].is_null()
        || row["threadSource"]
            .as_str()
            .is_some_and(|source| source != "user")
    {
        return None;
    }
    let id = row["id"]
        .as_str()
        .filter(|id| tower_agents::events::native_id(id))?;
    let cwd = row["cwd"]
        .as_str()
        .filter(|cwd| cwd.len() <= 4096 && std::path::Path::new(cwd).is_absolute())?;
    let updated_at = row["updatedAt"].as_u64()?.saturating_mul(1000);
    Some(tower_agents::inventory::Candidate {
        harness: tower_agents::Harness::Codex,
        native: tower_agents::runtime::NativeSession {
            id: id.into(),
            resume_ref: None,
        },
        cwd: cwd.into(),
        title: row["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .map(|name| name.chars().take(200).collect()),
        updated_at,
        namespace: Some(format!("codex-endpoint:{}", endpoint.display())),
    })
}

fn error(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}
fn encode(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(|_| error("internal_error", "Cannot encode terminal session data"))
}
fn decode<T: DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(|_| error("invalid_params", "Invalid terminal session data"))
}
fn name(session: &ManagedSession) -> Result<String> {
    let value: serde_json::Value = decode(&session.binding)?;
    value
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| error("invalid_params", "Terminal binding has no session name"))
}
fn observation(id: &str, name: &str, session: Option<&Session>) -> Observation {
    let state = session.map_or("lost", |s| s.state.as_str());
    let active = matches!(state, "reserved" | "running");
    Observation {
        id: id.into(),
        binding: None,
        status: match state {
            "reserved" => "starting",
            "running" => "unknown",
            "exited" => "exited",
            _ => "lost",
        }
        .into(),
        pid: session.and_then(|s| s.pid),
        pid_start_time: session.and_then(|s| s.pid_start_time),
        caps: Capabilities {
            attach: active,
            kill: active,
            snapshot: session.is_some(),
            ..Capabilities::unavailable("not_implemented")
        },
        terminal: Some(TerminalState {
            pool_session: name.into(),
            attach_count: session.map_or(0, |s| s.attach_count),
            running: false,
            terminated: false,
        }),
        recovery_pending: state == "reserved",
        metadata: Default::default(),
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Binding {
    name: String,
    plan: tower_agents::runtime::LauncherPlan,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Plan {
    spec: tower_agents::runtime::AgentSpec,
    #[serde(default)]
    terminal: tower_protocol::TerminalOptions,
}
fn binding(session: &ManagedSession) -> Result<Binding> {
    decode(&session.binding)
}
fn codex_cleanup_confirmed(plan: &tower_agents::runtime::LauncherPlan) -> bool {
    launcher::read_state(&std::path::Path::new(&plan.state_dir).join("state.json")).is_ok_and(
        |state| {
            state.id == plan.id
                && state.generation == plan.generation
                && state.private.get("codex_cleanup_confirmed")
                    == Some(&serde_json::Value::Bool(true))
        },
    )
}

fn launcher_alive(state: &tower_agents::runtime::RuntimeState) -> Result<bool> {
    if state.boot_id
        != tower_pal::boot_id()
            .map_err(|_| error("backend_unavailable", "Cannot verify boot identity"))?
    {
        return Ok(false);
    }
    for (pid, start) in [
        (Some(state.launcher_pid), Some(state.launcher_start_time)),
        (state.agent_pid, state.agent_start_time),
    ] {
        if let (Some(pid), Some(start)) = (pid, start) {
            match tower_pal::process_identity(pid) {
                Ok(p) if p.start_time == start => return Ok(true),
                Err(e)
                    if !matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                    ) =>
                {
                    return Err(error(
                        "backend_unavailable",
                        "Cannot verify previous process exit",
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(false)
}
impl TerminalBackend {
    fn state(&self, session: &ManagedSession, record: Option<&Session>) -> Result<Observation> {
        let name = name(session)?;
        let mut result = observation(&session.id, &name, record);
        if let Ok(binding) = binding(session) {
            result.caps.resume = binding.plan.spec.resume.is_some() && !binding.plan.spec.fork;
        }

        if let Ok(binding) = binding(session)
            && let Ok(mut state) = launcher::read_state(
                &std::path::Path::new(&binding.plan.state_dir).join("state.json"),
            )
            && state.id == session.id
            && state.generation == binding.plan.generation
        {
            let live = record.is_some_and(|r| {
                r.state == "running"
                    && r.pid == Some(state.launcher_pid)
                    && r.pid_start_time == Some(state.launcher_start_time)
            }) && state.boot_id == tower_pal::boot_id().unwrap_or_default()
                && tower_pal::process_identity(state.launcher_pid)
                    .is_ok_and(|p| p.start_time == state.launcher_start_time);
            if let Some(terminal) = &mut result.terminal {
                terminal.running = live && state.exit_code.is_none();
                let gone = record.is_none()
                    && state.exit_code.is_some()
                    && !launcher_alive(&state)?
                    && tower_pal::protocol_group_terminated(tower_pal::ProcessIdentity {
                        pid: state.launcher_pid,
                        start_time: state.launcher_start_time,
                        parent: 0,
                    })
                    .unwrap_or(false);
                terminal.terminated = (record.is_some_and(|r| r.state == "exited") || gone)
                    && state.exit_code.is_some()
                    && (state.harness != tower_agents::Harness::Codex
                        || !state.private.contains_key("server_pid")
                        || codex_cleanup_confirmed(&binding.plan));
            }
            if live || record.is_some_and(|r| r.state == "exited") || record.is_none() {
                if live {
                    result.status = state.status.clone();
                }
                if record.is_none() {
                    if launcher_alive(&state)? {
                        result.status = "unknown".into();
                        result.recovery_pending = true;
                    } else if tower_agents::config::resume_options(
                        binding.plan.spec.harness,
                        &binding.plan.spec.args,
                    )
                    .is_err()
                    {
                        result.status = "exited".into();
                        state.detail.error = Some("session_resume_failed".into());
                    } else if state.exit_code.is_some()
                        || state.stop_requested
                        || !session.auto_resume
                        || state.native.is_none()
                        || state.private.get("native_resumable")
                            == Some(&serde_json::Value::Bool(false))
                    {
                        result.status = "exited".into();
                        if state.exit_code.is_none()
                            && !state.stop_requested
                            && session.auto_resume
                            && (state.native.is_none()
                                || state.private.get("native_resumable")
                                    == Some(&serde_json::Value::Bool(false)))
                        {
                            state.detail.error = Some("session_resume_failed".into());
                        }
                    }
                }
                result.pid = state.agent_pid;
                result.pid_start_time = state.agent_start_time;
                result.caps.resume = state.native.is_some()
                    && state.private.get("native_resumable")
                        != Some(&serde_json::Value::Bool(false));
                if live
                    && !state.stop_requested
                    && !matches!(state.status.as_str(), "exited" | "lost")
                {
                    let mut endpoint: tower_protocol::DeliveryEndpoint = state
                        .private
                        .get("delivery")
                        .cloned()
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default();
                    endpoint.thread_id = state.native.as_ref().map(|n| n.id.clone());
                    let mut caps = tower_agents::delivery::capabilities(
                        state.harness.id(),
                        &endpoint,
                        state.private.get("queue_supported")
                            == Some(&serde_json::Value::Bool(true)),
                    );
                    if binding.plan.spec.mode.as_deref() == Some("app-server")
                        && state.native.is_some()
                    {
                        caps.message = "native".into();
                        caps.message_reason = None;
                        caps.wake_when_idle = true;
                        caps.reply_path = true;
                    }
                    if caps.message == "none"
                        && state.private.get("pty_supported")
                            == Some(&serde_json::Value::Bool(true))
                        && injection::allowed(&self.paths, state.harness)
                    {
                        caps.message = "pty".into();
                        caps.message_reason = None;
                        caps.wake_when_idle = true;
                        caps.reply_path = true;
                    }
                    result.caps.message = caps.message;
                    result.caps.message_reason = caps.message_reason;
                    result.caps.wake_when_idle = caps.wake_when_idle;
                    result.caps.reply_path = caps.reply_path;
                }
                if binding.plan.spec.uses_app_server() {
                    result.pid = state
                        .private
                        .get("server_pid")
                        .and_then(|v| v.as_u64())
                        .and_then(|v| u32::try_from(v).ok());
                    result.pid_start_time = state
                        .private
                        .get("server_start_time")
                        .and_then(|v| v.as_u64());
                }
                result.metadata = tower_sessions::SessionMetadata {
                    shared_process: Some(binding.plan.spec.uses_app_server()),
                    native_session_id: state.native.map(|n| n.id),
                    display_name: state
                        .display_name
                        .map_or(tower_protocol::Patch::Clear, tower_protocol::Patch::Set),
                    cwd: self.paths.display_cwd(&state.cwd).ok(),
                    detail: Some(encode(&state.detail)?),
                };
            }
        }
        Ok(result)
    }
}
fn clipboard_mode(harness: tower_agents::Harness) -> Option<tower_pool_proto::ClipboardMode> {
    match harness {
        tower_agents::Harness::Codex => Some(tower_pool_proto::ClipboardMode::FileUrl),
        tower_agents::Harness::ClaudeCode if tower_pal::operating_system() == "linux" => {
            Some(tower_pool_proto::ClipboardMode::NativeKey)
        }
        _ => None,
    }
}
impl ManagedBackend for TerminalBackend {
    fn clipboard(
        &self,
        session: &ManagedSession,
    ) -> Result<Option<tower_sessions::ClipboardTarget>> {
        let binding = binding(session)?;
        Ok(clipboard_mode(binding.plan.spec.harness)
            .is_some()
            .then_some(tower_sessions::ClipboardTarget {
                generation: binding.plan.generation,
            }))
    }

    fn send_message(
        &self,
        session: &ManagedSession,
        text: &str,
    ) -> Result<tower_protocol::DeliveryReceipt> {
        let binding = binding(session)?;
        let state =
            launcher::read_state(&std::path::Path::new(&binding.plan.state_dir).join("state.json"))
                .map_err(|_| error("delivery_failed", "Session state is unavailable"))?;
        if state.generation != binding.plan.generation
            || state.id != session.id
            || state.stop_requested
            || matches!(state.status.as_str(), "exited" | "lost")
        {
            return Err(error(
                "delivery_failed",
                "Target session is no longer active",
            ));
        }
        let token = tower_pal::read_private(
            &self
                .paths
                .token_path(&session.id)
                .map_err(|_| error("internal_error", "Invalid session identity"))?,
        )
        .map_err(|_| error("internal_error", "Session credential is unavailable"))?;
        let token = String::from_utf8(token)
            .map_err(|_| error("internal_error", "Invalid session credential"))?;
        match launcher::send_message(&binding.plan, &token, text, false) {
            Err(e)
                if matches!(e.data.code.as_str(), "channel_unavailable" | "unsupported")
                    && injection::allowed(&self.paths, binding.plan.spec.harness)
                    && state.private.get("pty_supported")
                        == Some(&serde_json::Value::Bool(true)) =>
            {
                launcher::send_message(&binding.plan, &token, text, true)
            }
            Err(mut e) if e.data.code == "channel_unavailable" => {
                e.data.code = "delivery_failed".into();
                Err(e)
            }
            result => result,
        }
    }

    fn conversations(
        &self,
        request: &tower_protocol::ConversationListParams,
    ) -> Result<tower_protocol::ConversationList> {
        let harness = request
            .harness
            .as_deref()
            .map(|h| {
                tower_agents::Harness::parse(h)
                    .ok_or_else(|| error("unsupported", "Unknown harness"))
            })
            .transpose()?;
        let mut found = tower_agents::inventory::InventoryResult::default();
        if harness.is_none_or(|h| h == tower_agents::Harness::Codex) {
            match self.codex_history(request.refresh) {
                Ok(codex) => found = codex,
                Err(_) => found
                    .errors
                    .push("codex: host_inventory_unavailable".into()),
            }
        }
        {
            let mut inventory = self
                .inventory
                .lock()
                .map_err(|_| error("internal_error", "Inventory unavailable"))?;
            let selected: Vec<_> = tower_agents::Harness::ALL
                .into_iter()
                .filter(|h| harness.is_none_or(|selected| selected == *h))
                .collect();
            let entries =
                inventory.list_selected(&tower_pal::environment(), &selected, request.refresh);
            let mut seen: std::collections::HashSet<_> = found
                .candidates
                .iter()
                .map(|c| (c.harness.id(), c.native.id.clone()))
                .collect();
            found.candidates.extend(
                entries
                    .candidates
                    .into_iter()
                    .filter(|c| seen.insert((c.harness.id(), c.native.id.clone()))),
            );
            found.errors.extend(entries.errors);
            found.truncated |= entries.truncated;
        }
        found.candidates.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.native.id.cmp(&b.native.id))
        });
        let mut conversations = found
            .candidates
            .into_iter()
            .filter(|c| {
                request
                    .cwd_prefix
                    .as_ref()
                    .is_none_or(|prefix| tower_pal::path_prefix(&c.cwd, prefix))
            })
            .map(|c| tower_protocol::Conversation {
                protocol: self.protocol().into(),
                harness: c.harness.id().into(),
                native_session_id: c.native.id,
                cwd: self.paths.display_cwd(&c.cwd).unwrap_or(c.cwd),
                display_name: c.title,
                updated_at: c.updated_at,
                fork: c.harness.can_fork(),
                tower_id: None,
            })
            .collect::<Vec<_>>();
        let limit = request.limit.unwrap_or(100).min(1000);
        let truncated = found.truncated || conversations.len() > limit;
        conversations.truncate(limit);
        Ok(tower_protocol::ConversationList {
            conversations,
            errors: found.errors,
            truncated,
        })
    }
    fn watch(&self, wake: std::sync::Arc<dyn Fn() + Send + Sync>) {
        let mut changes = self.changes();
        tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                wake();
            }
        });
    }
    fn cleanup(&self, session: &ManagedSession) -> Result<()> {
        if let Ok(binding) = binding(session)
            && let Ok(state) = launcher::read_state(
                &std::path::Path::new(&binding.plan.state_dir).join("state.json"),
            )
        {
            if launcher_alive(&state)? {
                return Err(error(
                    "session_running",
                    "Cannot remove a live session's resources",
                ));
            }
            if state.harness == tower_agents::Harness::Codex
                && state.private.contains_key("server_pid")
                && state.private.get("codex_cleanup_confirmed")
                    != Some(&serde_json::Value::Bool(true))
            {
                return Err(error(
                    "stop_unconfirmed",
                    "Codex cleanup is unconfirmed; private environment files must be retained",
                ));
            }
        }
        self.paths
            .token_path(&session.id)
            .map_err(|_| error("invalid_params", "Invalid session ID"))?;
        tower_pal::remove_private_tree(&self.paths.state.join("sessions").join(&session.id))
            .map_err(|_| error("cleanup_failed", "Cannot remove private session resources"))
    }
    fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        let env = tower_pal::environment();
        tower_agents::Harness::ALL
            .iter()
            .map(|h| tower_protocol::HarnessInfo {
                harness: h.id().into(),
                label: h.label().into(),
                protocol: "terminal".into(),
                executable: h.program().into(),
                available: tower_pal::find_program(h.program(), &env).is_some(),
                modes: if *h == tower_agents::Harness::Codex {
                    vec!["app-server".into(), "native".into()]
                } else {
                    vec![]
                },
                resume: true,
            })
            .collect()
    }
    fn protocol(&self) -> &str {
        "terminal"
    }
    fn features(&self) -> &'static [&'static str] {
        &[
            "sessions.codex-app-server.v1",
            "sessions.agents.v1",
            "sessions.resume.v1",
            "sessions.conversations.v1",
            "sessions.restart-options.v1",
        ]
    }
    fn prepare(&self, id: &str, request: &SessionOpenParams) -> Result<PreparedSession> {
        let harness = tower_agents::Harness::parse(&request.harness)
            .ok_or_else(|| error("unsupported", "Unknown agent harness"))?;
        let selected = request
            .resume
            .as_ref()
            .map(|seed| {
                if harness == tower_agents::Harness::Codex
                    && request.mode.as_deref() != Some("native")
                {
                    return self.codex_conversation(&seed.native_session_id);
                }
                self.inventory
                    .lock()
                    .map_err(|_| error("internal_error", "Inventory unavailable"))?
                    .resolve(&tower_pal::environment(), harness, &seed.native_session_id)
                    .map_err(|e| error("conversation_unavailable", &e.to_string()))
            })
            .transpose()?;
        let mut effective = request.clone();
        if let Some(c) = &selected {
            effective.cwd = c.cwd.clone();
        }
        let mode = if harness == tower_agents::Harness::Codex {
            Some(self.codex_mode(&effective)?)
        } else {
            request.mode.clone()
        };
        let spec = tower_agents::runtime::AgentSpec {
            native_home: if harness == tower_agents::Harness::Codex
                && mode.as_deref() == Some("app-server")
            {
                Some(
                    tower_agents::codex_host::namespace(&self.paths).map_err(|_| {
                        error(
                            "invalid_config",
                            "Invalid host Codex endpoint configuration",
                        )
                    })?,
                )
            } else {
                selected.as_ref().and_then(|c| c.namespace.clone())
            },
            harness,
            cwd: selected
                .as_ref()
                .map(|c| c.cwd.clone())
                .unwrap_or_else(|| request.cwd.clone()),
            args: request.args.clone(),
            mode: mode.clone(),
            resume: selected.map(|c| c.native),
            fork: request.resume.as_ref().is_some_and(|s| s.fork),
        };
        if let Some(options) = &request.terminal {
            if options
                .term
                .as_deref()
                .is_some_and(|term| !tower_pool_proto::valid_term(term))
            {
                return Err(error("invalid_params", "Invalid terminal type"));
            }
            if options
                .size
                .is_some_and(|s| s.rows == 0 || s.cols == 0 || s.rows > 256 || s.cols > 512)
            {
                return Err(error(
                    "invalid_params",
                    "Terminal size must be 1–256 rows and 1–512 columns",
                ));
            }
        }
        tower_agents::config::validate(&spec)
            .map_err(|e| error("invalid_params", &e.to_string()))?;
        pool::ensure(&self.paths)
            .map_err(|_| error("pool_unavailable", "Cannot connect to terminal pool"))?;
        let mut bytes = [0; 8];
        getrandom::fill(&mut bytes)
            .map_err(|_| error("internal_error", "Random source unavailable"))?;
        let generation: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let root = self.paths.state.join("sessions").join(id).join(&generation);
        let run = self.paths.run.join("agents");
        tower_pal::private_dir(&root)
            .and_then(|_| tower_pal::private_dir(&run))
            .map_err(|_| error("internal_error", "Cannot create session directory"))?;
        let plan = tower_agents::runtime::LauncherPlan {
            id: id.into(),
            generation: generation.clone(),
            state_dir: root.to_string_lossy().into_owned(),
            tower_socket: self.paths.socket.to_string_lossy().into_owned(),
            control_socket: run
                .join(format!("{generation}.sock"))
                .to_string_lossy()
                .into_owned(),
            spec: spec.clone(),
            continue_turn: false,
            embedding: None,
        };
        let binding = Binding {
            name: id.into(),
            plan,
        };
        Ok(PreparedSession {
            harness: harness.id().into(),
            cwd: spec.cwd.clone(),
            native_session_id: (!spec.fork)
                .then(|| spec.resume.as_ref().map(|n| n.id.clone()))
                .flatten(),
            binding: encode(&binding)?,
            launch: encode(Plan {
                spec,
                terminal: request.terminal.clone().unwrap_or_default(),
            })?,
            mode,
            caps: Capabilities {
                attach: true,
                kill: true,
                snapshot: true,
                ..Capabilities::unavailable("not_implemented")
            },
            terminal: Some(TerminalState {
                pool_session: id.into(),
                attach_count: 0,
                running: false,
                terminated: false,
            }),
        })
    }
    fn start(&self, session: &ManagedSession, token: &Secret) -> Result<Observation> {
        let binding = binding(session)?;
        let plan: Plan = decode(
            session
                .launch
                .as_ref()
                .ok_or_else(|| error("invalid_params", "No launch plan"))?,
        )?;
        let path = std::path::Path::new(&binding.plan.state_dir).join("plan.json");
        tower_pal::write_private(
            &path,
            &serde_json::to_vec(&binding.plan)
                .map_err(|_| error("internal_error", "Cannot serialize launch plan"))?,
        )
        .map_err(|_| error("internal_error", "Cannot save launch plan"))?;
        let launch = Launch {
            name: binding.name.clone(),
            argv: vec![
                tower_pal::Program::Cli
                    .path()
                    .map_err(|_| error("internal_error", "Cannot find Tower executable"))?
                    .to_string_lossy()
                    .into_owned(),
                "__agent".into(),
                "run".into(),
                path.to_string_lossy().into_owned(),
            ],
            clipboard: clipboard_mode(plan.spec.harness).map(|mode| {
                tower_pool_proto::ClipboardConfig {
                    socket: self.paths.socket.to_string_lossy().into_owned(),
                    generation: binding.plan.generation.clone(),
                    mode,
                }
            }),
            cwd: plan.spec.cwd,
            env: [
                ("TOWER_AGENT_ID".into(), session.id.clone()),
                ("TOWER_AGENT_TOKEN".into(), token.0.clone()),
                (
                    "TOWER_SOCKET".into(),
                    self.paths.socket.to_string_lossy().into_owned(),
                ),
            ]
            .into(),
            term: plan.terminal.term,
            defer: plan.terminal.defer,
            size: plan.terminal.size.map(|s| tower_pool_proto::Size {
                rows: s.rows,
                cols: s.cols,
            }),
        };
        pool::ensure(&self.paths)
            .map_err(|_| error("pool_unavailable", "Cannot start terminal pool"))?;
        if pool::list(&self.paths).is_ok_and(|records| {
            records
                .iter()
                .any(|r| r.name == binding.name && r.state == "exited")
        }) {
            match pool::request(
                &self.paths,
                &Request::Remove {
                    name: binding.name.clone(),
                },
            ) {
                Ok(Reply::Ok) => {}
                _ => {
                    return Err(error(
                        "backend_unavailable",
                        "Cannot remove the stopped terminal session",
                    ));
                }
            }
        }
        match pool::request(&self.paths, &Request::Create { launch }) {
            Ok(Reply::Created { session: created }) if created.name == binding.name => {
                self.state(session, Some(&created))
            }
            _ => Err(error(
                "launch_failed",
                "Launch could not be confirmed; inspect the session before retrying",
            )),
        }
    }
    fn observe(&self, sessions: &[ManagedSession]) -> Result<Vec<Observation>> {
        if sessions.is_empty() {
            return Ok(vec![]);
        }
        pool::ensure(&self.paths)
            .map_err(|_| error("backend_unavailable", "Cannot start terminal pool"))?;
        let records = pool::list(&self.paths)
            .map_err(|_| error("backend_unavailable", "Terminal pool unavailable"))?;
        sessions
            .iter()
            .map(|s| {
                let n = name(s)?;
                let record = records.iter().find(|r| r.name == n);
                self.state(s, record)
            })
            .collect()
    }
    fn stop(&self, session: &ManagedSession) -> Result<Observation> {
        let name = name(session)?;
        let binding = binding(session)?;
        let codex = binding.plan.spec.uses_app_server();
        let reserved = if codex {
            pool::list(&self.paths)
                .map_err(|_| error("stop_unconfirmed", "Cannot read terminal session state"))?
                .iter()
                .any(|entry| entry.name == name && entry.state == "reserved")
        } else {
            false
        };
        // Holding the launcher lock makes reservation cancellation atomic with startup.
        let _reservation_guard = if reserved {
            Some(
                tower_pal::SingletonLock::acquire(
                    &std::path::Path::new(&binding.plan.state_dir).join("launcher.lock"),
                )
                .map_err(|_| {
                    error(
                        "stop_unconfirmed",
                        "The Codex launcher is starting; retry the stop",
                    )
                })?,
            )
        } else {
            None
        };
        let cleaned = codex && codex_cleanup_confirmed(&binding.plan);
        if !reserved && !cleaned {
            let stopped = self
                .paths
                .token_path(&session.id)
                .ok()
                .and_then(|path| tower_pal::read_private(&path).ok())
                .and_then(|token| String::from_utf8(token).ok())
                .is_some_and(|token| launcher::stop(&binding.plan, &token).is_ok());
            if codex && !stopped && !codex_cleanup_confirmed(&binding.plan) {
                return Err(error(
                    "stop_unconfirmed",
                    "Codex thread cleanup is unconfirmed; the shared server remains running",
                ));
            }
        }
        if !matches!(
            pool::request(&self.paths, &Request::Kill { name: name.clone() }),
            Ok(Reply::Ok)
        ) {
            let observation = self.state(session, None)?;
            if observation.terminal.as_ref().is_some_and(|t| t.terminated) {
                return Ok(observation);
            }
            return Err(error(
                "stop_unconfirmed",
                "Terminal pool could not confirm termination",
            ));
        }
        let records = pool::list(&self.paths)
            .map_err(|_| error("stop_unconfirmed", "Cannot verify termination"))?;
        self.state(session, records.iter().find(|r| r.name == name))
    }
    fn prepare_resume(
        &self,
        session: &ManagedSession,
        mode: Option<&str>,
        args: Option<&[String]>,
    ) -> Result<PreparedSession> {
        let mut terminal = session
            .launch
            .as_ref()
            .map(decode::<Plan>)
            .transpose()?
            .map(|p| p.terminal)
            .unwrap_or_default();
        terminal.defer = false;
        let old = binding(session)?;
        let mut spec = old.plan.spec;
        match launcher::read_state(&std::path::Path::new(&old.plan.state_dir).join("state.json")) {
            Ok(state) => {
                if state.generation != old.plan.generation || state.id != session.id {
                    return Err(error(
                        "resume_unavailable",
                        "Session state belongs to another incarnation",
                    ));
                }
                if state.private.get("native_resumable") == Some(&serde_json::Value::Bool(false)) {
                    return Err(error(
                        "resume_unavailable",
                        "This native conversation is ephemeral",
                    ));
                }
                spec.resume = Some(state.native.ok_or_else(|| {
                    error(
                        "resume_unavailable",
                        "The agent has not reported a native session identity",
                    )
                })?);
                spec.native_home = state
                    .private
                    .get("native_home")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .or(spec.native_home);
                spec.cwd = state.cwd;
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && spec.resume.is_some()
                    && !spec.fork => {}
            Err(_) => {
                return Err(error(
                    "resume_unavailable",
                    "No native session state is available",
                ));
            }
        }
        spec.args = match args {
            Some(args) => args.to_vec(),
            None => tower_agents::config::resume_options(spec.harness, &spec.args)
                .map_err(|e| error("resume_options_required", &e.to_string()))?,
        };
        spec.fork = false;
        let old_mode = spec.mode.clone();
        spec.mode = mode.map(str::to_owned).or(spec.mode);
        tower_agents::config::validate(&spec)
            .map_err(|e| error("invalid_params", &e.to_string()))?;
        if spec.harness == tower_agents::Harness::Codex && old_mode != spec.mode {
            let native = &spec.resume.as_ref().expect("resume checked").id;
            let target = if spec.uses_app_server() {
                self.codex_conversation(native)?
            } else {
                self.inventory.lock().map_err(|_| error("internal_error", "Inventory unavailable"))?
                    .resolve(&tower_pal::environment(), spec.harness, native)
                    .map_err(|_| error("conversation_unavailable", "The target Codex mode cannot find this conversation in its native store"))?
            };
            spec.native_home = target.namespace;
        }
        let request = SessionOpenParams {
            checkout_id: None,
            resume: None,
            protocol: Some("terminal".into()),
            harness: spec.harness.id().into(),
            cwd: spec.cwd.clone(),
            mode: spec.mode.clone(),
            args: spec.args.clone(),
            headless: true,
            terminal: Some(terminal.clone()),
        };
        let mut prepared = self.prepare(&session.id, &request)?;
        let mut binding: Binding = decode(&prepared.binding)?;
        binding.plan.spec = spec.clone();
        prepared.binding = encode(binding)?;
        prepared.launch = encode(Plan { spec, terminal })?;
        Ok(prepared)
    }
    fn recover(&self, session: &ManagedSession) -> Result<Option<PreparedSession>> {
        if !session.auto_resume || session.launch.is_none() {
            return Ok(None);
        }
        pool::ensure(&self.paths)
            .map_err(|_| error("backend_unavailable", "Cannot start terminal pool"))?;
        let old = binding(session)?;
        if pool::list(&self.paths)
            .map_err(|_| error("backend_unavailable", "Cannot inspect pool"))?
            .iter()
            .any(|r| r.name == old.name)
        {
            return Ok(None);
        }
        match launcher::read_state(&std::path::Path::new(&old.plan.state_dir).join("state.json")) {
            Ok(state) => {
                if state.id != session.id || state.generation != old.plan.generation {
                    return Err(error("backend_error", "Session state identity mismatch"));
                }
                if launcher_alive(&state)?
                    || state.exit_code.is_some()
                    || state.stop_requested
                    || state.native.is_none()
                    || state.private.get("native_resumable")
                        == Some(&serde_json::Value::Bool(false))
                {
                    return Ok(None);
                }
                let mut prepared = self.prepare_resume(session, None, None)?;
                if state.status == "working" {
                    let mut binding: Binding = decode(&prepared.binding)?;
                    binding.plan.continue_turn = true;
                    prepared.binding = encode(binding)?;
                }
                Ok(Some(prepared))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let plan: Plan = decode(session.launch.as_ref().expect("checked above"))?;
                self.prepare(
                    &session.id,
                    &SessionOpenParams {
                        checkout_id: None,
                        resume: None,
                        protocol: Some("terminal".into()),
                        harness: plan.spec.harness.id().into(),
                        cwd: plan.spec.cwd,
                        mode: plan.spec.mode,
                        args: plan.spec.args,
                        headless: false,
                        terminal: Some(plan.terminal),
                    },
                )
                .map(Some)
            }
            Err(_) => Err(error("backend_error", "Cannot read recovery state")),
        }
    }
    fn terminal(&self) -> Option<&dyn TerminalAccess> {
        Some(self)
    }
}
impl TerminalAccess for TerminalBackend {
    fn attach_info(&self, session: &ManagedSession) -> Result<AttachInfo> {
        Ok(AttachInfo {
            pool_socket: self.paths.pool_socket().to_string_lossy().into_owned(),
            pool_session: name(session)?,
            clipboard: None,
        })
    }
    fn snapshot(&self, session: &ManagedSession) -> Result<SnapshotResult> {
        let mut snapshot = pool::snapshot(&self.paths, &name(session)?)
            .map_err(|_| error("snapshot_unavailable", "Terminal snapshot is unavailable"))?;
        snapshot.id = session.id.clone();
        Ok(snapshot)
    }
    fn pool_info(&self) -> PoolInfo {
        let version = pool::version(&self.paths).ok().flatten();
        PoolInfo {
            running: version.is_some(),
            proto_version: version.map(|major| ProtocolVersion { major, minor: 0 }),
        }
    }
    fn restart_pool(&self) -> Result<()> {
        pool::restart(&self.paths)
            .map_err(|_| error("pool_unavailable", "Cannot restart terminal pool"))
    }
}
