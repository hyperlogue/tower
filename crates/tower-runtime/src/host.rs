use crate::{Binding, Journal, Limits, Result, engine, error, internal, random_id, wire};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tower_protocol::{ExecutionCommand, ExecutionStatus, RuntimeCursor};
use tower_sessions::execution::RuntimeUpdates;

#[derive(Serialize, Deserialize)]
pub(crate) struct Call {
    pub credential: String,
    pub method: String,
    pub params: Value,
}
struct Host {
    paths: tower_pal::Paths,
    journal: Arc<Journal>,
    owners: Mutex<BTreeMap<String, Arc<engine::Handle>>>,
    shutdown: tokio::sync::Notify,
    terminal: Option<Arc<dyn tower_sessions::ManagedBackend>>,
}
fn key(b: &Binding) -> String {
    serde_json::to_string(&(b.namespace(), &b.runtime_id, &b.generation)).expect("string tuple")
}
fn parse<T: serde::de::DeserializeOwned>(v: Value) -> Result<T> {
    serde_json::from_value(v).map_err(|_| error("invalid_params", "Invalid runtime request"))
}
fn value(v: impl Serialize) -> Result<Value> {
    serde_json::to_value(v).map_err(internal)
}
impl Host {
    fn request_shutdown(&self) -> Result<()> {
        let owners = self.owners.lock().map_err(internal)?;
        for h in owners.values() {
            let _ = h
                .control
                .try_send(engine::Control::Stop("runtime_shutdown".into()));
        }
        Ok(())
    }
    fn execute(&self, b: Binding, c: ExecutionCommand) -> Result<Value> {
        let terminal = b.protocol == "terminal";
        if (!terminal
            && (b.protocol
                != if b.harness == "codex" {
                    "codex-app-server"
                } else {
                    "acp"
                }
                || !matches!(b.harness.as_str(), "codex" | "claude-code" | "antigravity")))
            || !std::path::Path::new(&b.cwd).is_absolute()
        {
            return Err(error("invalid_params", "Unsupported runtime binding"));
        }
        let prior = self.journal.status(
            &b.namespace(),
            &ExecutionStatus {
                command_id: c.command_id.clone(),
                runtime_id: c.runtime_id.clone(),
                generation: c.generation.clone(),
            },
        )?;
        if prior.state != "missing" {
            return value(self.journal.admit(&b, &c)?);
        }
        if terminal {
            crate::terminal::validate(&b, &c)?;
            let backend = self
                .terminal
                .as_ref()
                .ok_or_else(|| error("unsupported_control", "Terminal execution is unavailable"))?;
            if c.kind == "launch"
                && !backend
                    .harnesses()
                    .iter()
                    .any(|h| h.harness == b.harness && h.available)
            {
                return Err(error(
                    "unsupported_control",
                    "Terminal harness is unavailable",
                ));
            }
        }
        let mut owners = self.owners.lock().map_err(internal)?;
        let handle = if let Some(h) = owners.get(&key(&b)) {
            h.clone()
        } else {
            if !matches!(c.kind.as_str(), "launch" | "resume") {
                return Err(error("runtime_lost", "Runtime owner is unavailable"));
            }
            if self
                .journal
                .binding(&b.namespace(), &b.runtime_id, &b.generation)
                .is_ok()
            {
                return Err(error(
                    "runtime_fenced",
                    "An old generation cannot be restarted",
                ));
            }
            let (handle, normal, control) = engine::channels();
            let receipt = self.journal.admit(&b, &c)?;
            handle
                .normal
                .try_send(c)
                .map_err(|_| error("runtime_capacity", "Runtime queue is full"))?;
            owners.insert(key(&b), handle.clone());
            let failed_binding = b.clone();
            let failed_journal = self.journal.clone();
            let task = if terminal {
                tokio::spawn(crate::terminal::run(
                    self.paths.clone(),
                    self.journal.clone(),
                    b,
                    self.terminal
                        .as_ref()
                        .expect("checked terminal backend")
                        .clone(),
                    normal,
                    control,
                    false,
                ))
            } else {
                tokio::spawn(engine::run(
                    self.paths.clone(),
                    self.journal.clone(),
                    b,
                    handle,
                    normal,
                    control,
                ))
            };
            tokio::spawn(async move {
                if task.await.is_err() {
                    let _ = failed_journal.fence(&failed_binding, "owner_task_failed", "unknown");
                }
            });
            return value(receipt);
        };
        if terminal && c.kind != "cancel" {
            return Err(error(
                "runtime_conflict",
                "A terminal runtime already has a launch",
            ));
        }
        if c.kind == "reply" {
            let p = serde_json::to_value(&c.payload).map_err(internal)?;
            handle.requests.validate_reply(&p)?;
        }
        let result = if matches!(c.kind.as_str(), "cancel" | "reply") {
            let permit = handle
                .control
                .clone()
                .try_reserve_owned()
                .map_err(|_| error("control_capacity", "Runtime control queue is full"))?;
            let receipt = self.journal.admit(&b, &c)?;
            permit.send(engine::Control::Command(c));
            receipt
        } else {
            let permit = handle
                .normal
                .clone()
                .try_reserve_owned()
                .map_err(|_| error("runtime_capacity", "Runtime command queue is full"))?;
            let receipt = self.journal.admit(&b, &c)?;
            permit.send(c);
            receipt
        };
        value(result)
    }
    fn dispatch(&self, method: &str, p: Value) -> Result<Value> {
        match method {
            "hello" => Ok(json!({"version":1})),
            "shutdown" => Ok(Value::Null),
            "execute" => self.execute(parse(p["binding"].clone())?, parse(p["command"].clone())?),
            "terminal.session" => {
                let binding: Binding = parse(p["binding"].clone())?;
                if p["live"] == true && self.journal.state(&binding)?.1 {
                    return Err(error(
                        "runtime_fenced",
                        "Terminal generation is no longer live",
                    ));
                }
                value(crate::terminal::session(&self.journal, &binding)?)
            }
            "status" => value(
                self.journal.status(
                    p["namespace"]
                        .as_str()
                        .ok_or_else(|| error("invalid_params", "Namespace required"))?,
                    &parse(p["command"].clone())?,
                )?,
            ),
            "list" => {
                self.owners
                    .lock()
                    .map_err(internal)?
                    .retain(|_, h| !h.normal.is_closed());
                let namespace = p["namespace"].as_str();
                let after = p["after"].as_u64().unwrap_or(0);
                let mut views = Vec::new();
                let mut cursor = None;
                let mut bytes = 0usize;
                let entries = self.journal.views_page(namespace, after)?;
                let more = entries.len() == 64;
                for (id, view) in entries {
                    let size = serde_json::to_vec(&view).map_err(internal)?.len();
                    if bytes + size > tower_protocol::MAX_FRAME_BYTES - 4096 {
                        if views.is_empty() {
                            return Err(error(
                                "too_large",
                                "Runtime record exceeds its frame budget",
                            ));
                        }
                        return Ok(json!({"records":views,"cursor":cursor}));
                    }
                    cursor = Some(id);
                    bytes += size;
                    views.push(view);
                }
                Ok(json!({"records":views,"cursor":if more{cursor}else{None}}))
            }
            "updates" => {
                let namespace = p["namespace"]
                    .as_str()
                    .ok_or_else(|| error("invalid_params", "Namespace required"))?;
                let connection = p["connection"]
                    .as_str()
                    .filter(|v| crate::identifier(v))
                    .ok_or_else(|| error("invalid_params", "Connection identity required"))?;
                let receipts = self.journal.receipt_updates(namespace)?;
                let losses = self.journal.losses(namespace)?;
                let mut requests = Vec::new();
                let mut budget = tower_protocol::MAX_FRAME_BYTES.saturating_sub(
                    serde_json::to_vec(&receipts).map_err(internal)?.len()
                        + serde_json::to_vec(&losses).map_err(internal)?.len()
                        + 4096,
                );
                let owners = self.owners.lock().map_err(internal)?;
                for binding in self.journal.bindings(Some(namespace))? {
                    if let Some(h) = owners.get(&key(&binding)) {
                        let committed = self.journal.cursor(&binding)?.cursor;
                        requests.extend(h.requests.updates(connection, committed, &mut budget)?);
                    }
                }
                value(RuntimeUpdates {
                    receipts,
                    losses,
                    requests,
                })
            }
            "cursor" => value(self.journal.cursor(&parse(p)?)?),
            "reannounce" => {
                let b: Binding = parse(p)?;
                let (state, fenced, _) = self.journal.state(&b)?;
                if !fenced
                    && self
                        .owners
                        .lock()
                        .map_err(internal)?
                        .get(&key(&b))
                        .is_some_and(|h| !h.normal.is_closed())
                {
                    let m = self.journal.metadata(&b)?;
                    if b.protocol == "terminal" {
                        if m.pointer("/observation/terminal/running") == Some(&Value::Bool(true)) {
                            self.journal.append(
                                &b,
                                "runtime_state",
                                parse(crate::terminal::state_payload(&b, &m, "running", false))?,
                                None,
                                true,
                            )?;
                        }
                        return Ok(Value::Null);
                    }
                    if m[if b.protocol == "acp" {
                        "acp_session_id"
                    } else {
                        "native_session_id"
                    }]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
                    {
                        let mut payload = json!({"state":match state.as_str(){"working"=>"running","waiting_approval"|"waiting_input"=>"waiting",s=>s},"owner_id":b.owner_id,"tower_session_id":b.tower_session_id});
                        if m["native_session_id"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty())
                        {
                            payload["native_session_id"] = m["native_session_id"].clone();
                        }
                        if b.protocol == "acp" {
                            payload["acp_session_id"] = m["acp_session_id"].clone();
                        }
                        self.journal
                            .append(&b, "runtime_state", parse(payload)?, None, true)?;
                    }
                }
                Ok(Value::Null)
            }
            "batch" => value(self.journal.batch(&parse(p)?)?),
            "ack" => {
                let ns = p["namespace"]
                    .as_str()
                    .ok_or_else(|| error("invalid_params", "Namespace required"))?;
                let v = &p["value"];
                match p["kind"].as_str() {
                    Some("request") => {
                        let id = v["source_request_id"].as_str().unwrap_or("");
                        let rev = v["request_revision"].as_u64().unwrap_or(0);
                        let owners = self.owners.lock().map_err(internal)?;
                        for binding in self.journal.bindings(Some(ns))? {
                            if let Some(h) = owners.get(&key(&binding)) {
                                h.requests.acknowledge(
                                    id,
                                    rev,
                                    v["connection"].as_str().unwrap_or(""),
                                    v["peer_state"].as_str().unwrap_or(""),
                                )?;
                            }
                        }
                    }
                    Some("receipt") => {
                        let sent = v["receipt_revision"].as_u64().unwrap_or(0);
                        if let Some(reported) = v["reported_revision"].as_u64()
                            && reported > sent
                        {
                            let command: ExecutionStatus = parse(v.clone())?;
                            let retained = self.journal.status(ns, &command)?;
                            if reported > retained.receipt_revision {
                                let b = self.journal.binding(
                                    ns,
                                    &command.runtime_id,
                                    &command.generation,
                                )?;
                                self.dispatch(
                                    "fence",
                                    json!({"binding":b,"reason":"receipt_history_lost"}),
                                )?;
                                return Err(error(
                                    "receipt_history_lost",
                                    "Receiver receipt exceeds durable evidence",
                                ));
                            }
                        }
                        self.journal.acknowledge_receipt(
                            ns,
                            v["command_id"].as_str().unwrap_or(""),
                            sent,
                        )?;
                    }
                    Some("events") => {
                        let cursor: RuntimeCursor = parse(v.clone())?;
                        let b = self
                            .journal
                            .binding(ns, &cursor.runtime_id, &cursor.generation)?;
                        self.journal.acknowledge_events(&b, &cursor)?;
                    }
                    Some("loss") => {
                        let b = self.journal.binding(
                            ns,
                            v["runtime_id"].as_str().unwrap_or(""),
                            v["generation"].as_str().unwrap_or(""),
                        )?;
                        self.journal.acknowledge_loss(
                            &b,
                            v["loss_id"].as_str().unwrap_or(""),
                            v["report_revision"].as_u64().unwrap_or(0),
                        )?;
                    }
                    _ => return Err(error("invalid_params", "Unknown runtime acknowledgement")),
                }
                Ok(Value::Null)
            }
            "stop" | "fence" => {
                let b: Binding = parse(p["binding"].clone())?;
                let reason = if method == "stop" {
                    "local_stop"
                } else {
                    p["reason"]
                        .as_str()
                        .filter(|v| crate::identifier(v))
                        .unwrap_or("history_conflict")
                };
                if method == "fence" {
                    self.journal.fence(&b, reason, "unknown")?;
                }
                let owners = self.owners.lock().map_err(internal)?;
                if let Some(h) = owners.get(&key(&b)) {
                    h.control
                        .try_send(engine::Control::Stop(reason.into()))
                        .map_err(|_| error("control_capacity", "Cannot enqueue runtime stop"))?;
                } else if method == "stop" {
                    self.journal.fence(&b, reason, "unknown")?;
                }
                Ok(Value::Null)
            }
            _ => Err(error("unsupported", "Unknown runtime operation")),
        }
    }
}
pub(crate) fn credential(paths: &tower_pal::Paths) -> Result<String> {
    String::from_utf8(
        tower_pal::read_private(&paths.state.join("runtime/owner.key")).map_err(internal)?,
    )
    .map_err(internal)
}
pub(crate) fn prepare(paths: &tower_pal::Paths) -> Result<()> {
    tower_pal::private_dir(&paths.state.join("runtime")).map_err(internal)?;
    let path = paths.state.join("runtime/owner.key");
    match tower_pal::read_private(&path) {
        Ok(v) if v.len() == 64 => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => tower_pal::write_private(
            &path,
            format!("{}{}", random_id("")?, random_id("")?).as_bytes(),
        )
        .map_err(internal),
        _ => Err(error("runtime_storage", "Invalid runtime owner credential")),
    }
}
async fn recover_shared_codex(paths: &tower_pal::Paths, metadata: &Value) -> bool {
    let (Some(pid), Some(start)) = (
        metadata["pid"].as_u64().and_then(|v| u32::try_from(v).ok()),
        metadata["pid_start_time"].as_u64(),
    ) else {
        return false;
    };
    let Some(thread) = metadata["native_session_id"].as_str() else {
        return false;
    };
    let Ok(namespace) = tower_agents::codex_host::namespace(paths) else {
        return false;
    };
    if metadata["native_home"] != namespace {
        return false;
    }
    let cleanup = async {
        let config = tower_agents::codex_service::Config::load(paths)?;
        let mut client = tower_agents::codex_service::Client::connect(&config.endpoint).await?;
        if !client
            .process()
            .is_some_and(|current| current.pid == pid && current.start_time == start)
        {
            return Err(tower_agents::codex_service::Error::Unconfirmed);
        }
        client.stop_thread(thread, None).await?;
        client.wait_unloaded(thread, Duration::from_secs(5)).await
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(25), cleanup).await,
        Ok(Ok(()))
    )
}
pub async fn run(
    paths: tower_pal::Paths,
    terminal: Option<Arc<dyn tower_sessions::ManagedBackend>>,
) -> Result<()> {
    paths.prepare().map_err(internal)?;
    let _lock =
        tower_pal::SingletonLock::acquire(&paths.run.join("runtime.lock")).map_err(internal)?;
    prepare(&paths)?;
    let credential = credential(&paths)?;
    #[derive(Default, Deserialize)]
    #[serde(default)]
    struct Configuration {
        runtime: Limits,
    }
    let configuration: Configuration =
        tower_pal::read_config(&paths.config_home.join("tower/config.toml"))
            .map_err(internal)?
            .map(|text| toml::from_str(&text))
            .transpose()
            .map_err(|_| error("invalid_config", "Invalid runtime limits"))?
            .unwrap_or_default();
    let limits = configuration.runtime;
    if limits.runtime_spool_bytes == 0
        || limits.host_spool_bytes < limits.runtime_spool_bytes
        || limits.control_reserve_bytes < 65536
        || limits.receipt_count == 0
        || limits.receipt_retention_days == 0
    {
        return Err(error(
            "invalid_config",
            "Runtime limits must reserve bulk and control capacity",
        ));
    }
    let journal = Arc::new(Journal::open(
        &paths.state.join("runtime/runtime.db"),
        limits,
    )?);
    let boot = tower_pal::boot_id().map_err(internal)?;
    let mut recovery = Vec::new();
    let mut terminal_recovery = Vec::new();
    let mut shared_recovery = tokio::task::JoinSet::new();
    for binding in journal.bindings(None)? {
        let (_, fenced, termination) = journal.state(&binding)?;
        if fenced && termination == "confirmed" {
            engine::cleanup_environment(&paths, &binding);
            continue;
        }
        let m = journal.metadata(&binding)?;
        if binding.protocol == "terminal" {
            if terminal.is_some() && !m["boot_id"].as_str().is_some_and(|old| old != boot) {
                terminal_recovery.push(binding);
            } else {
                journal.fence(
                    &binding,
                    "terminal_owner_lost",
                    if m["boot_id"].as_str().is_some_and(|old| old != boot) {
                        "confirmed"
                    } else {
                        "unknown"
                    },
                )?;
            }
            continue;
        }
        if m["boot_id"].as_str().is_some_and(|old| old != boot) {
            journal.fence(&binding, "owner_restarted", "confirmed")?;
            engine::cleanup_environment(&paths, &binding);
            continue;
        }
        if binding.protocol == "codex-app-server" || m["shared_process"] == true {
            journal.fence(&binding, "owner_restarted", "unknown")?;
            if binding.protocol == "codex-app-server" {
                if shared_recovery.len() >= 4
                    && let Some(Ok((previous, confirmed))) = shared_recovery.join_next().await
                    && confirmed
                {
                    journal.fence(&previous, "owner_restarted", "confirmed")?;
                    engine::cleanup_environment(&paths, &previous);
                }
                let paths = paths.clone();
                shared_recovery.spawn(async move {
                    let confirmed = recover_shared_codex(&paths, &m).await;
                    (binding, confirmed)
                });
            }
            continue;
        }
        if let (Some(pid), Some(start)) = (
            m["pid"].as_u64().and_then(|v| u32::try_from(v).ok()),
            m["pid_start_time"].as_u64(),
        ) {
            let identity = tower_pal::ProcessIdentity {
                pid,
                parent: 0,
                start_time: start,
            };
            let _ = tower_pal::kill_protocol_group(identity);
            recovery.push((binding, identity));
        } else {
            journal.fence(&binding, "owner_restarted", "unknown")?;
        }
    }
    for _ in 0..40 {
        if recovery
            .iter()
            .all(|(_, i)| tower_pal::protocol_group_terminated(*i).unwrap_or(false))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for (binding, identity) in recovery {
        journal.fence(
            &binding,
            "owner_restarted",
            if tower_pal::protocol_group_terminated(identity).unwrap_or(false) {
                "confirmed"
            } else {
                "unknown"
            },
        )?;
    }
    while let Some(result) = shared_recovery.join_next().await {
        if let Ok((binding, true)) = result {
            journal.fence(&binding, "owner_restarted", "confirmed")?;
            engine::cleanup_environment(&paths, &binding);
        }
    }
    journal.compact()?;
    let listener = tower_pal::IpcListener::bind(&paths.run.join("runtime.sock"))
        .await
        .map_err(internal)?;
    let host = Arc::new(Host {
        paths,
        journal,
        owners: Mutex::new(BTreeMap::new()),
        shutdown: tokio::sync::Notify::new(),
        terminal,
    });
    for binding in terminal_recovery {
        let (handle, normal, control) = engine::channels();
        host.owners
            .lock()
            .map_err(internal)?
            .insert(key(&binding), handle);
        tokio::spawn(crate::terminal::run(
            host.paths.clone(),
            host.journal.clone(),
            binding,
            host.terminal
                .as_ref()
                .expect("recovery backend checked")
                .clone(),
            normal,
            control,
            true,
        ));
    }
    let gate = Arc::new(tokio::sync::Semaphore::new(32));
    let mut maintenance = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _=async {tokio::select!{_=tower_pal::shutdown_signal()=>{},_=host.shutdown.notified()=>{}}}=>{host.request_shutdown()?;for _ in 0..100{if host.owners.lock().map_err(internal)?.values().all(|h|h.normal.is_closed()){break;}tokio::time::sleep(Duration::from_millis(50)).await;}return Ok(());}
            _=maintenance.tick()=>{
                host.journal.compact()?;
            }
            accepted=listener.accept()=>{
                let (mut socket, peer) = accepted.map_err(internal)?;
                if peer.uid != tower_pal::uid() {
                    continue;
                }
                let Ok(permit) = gate.clone().try_acquire_owned() else {
                    continue;
                };
                let host = host.clone();
                let credential = credential.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = tokio::time::timeout(Duration::from_secs(10), async {
                        let call: Call = wire::read(&mut socket).await?;
                        let result = if bool::from(call.credential.as_bytes().ct_eq(credential.as_bytes())) {
                            host.dispatch(&call.method, call.params)
                        } else {
                            Err(error("unauthorized", "Invalid runtime owner credential"))
                        };
                        wire::write(&mut socket, &result).await?;
                        if call.method == "shutdown" && result.is_ok() {
                            host.shutdown.notify_one();
                        }
                        Ok::<(), tower_protocol::RpcError>(())
                    })
                    .await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_ack_keeps_unsent_evidence_and_fences_unrecoverable_history() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        let host = Host {
            paths: tower_pal::Paths {
                state: path.into(),
                run: path.join("run"),
                socket: path.join("towerd.sock"),
                database: path.join("tower.db"),
                tokens: path.join("tokens"),
                logs: path.join("logs"),
                config_home: path.join("config"),
                home: path.into(),
            },
            journal: Arc::new(Journal::open(&path.join("runtime.db"), Limits::default()).unwrap()),
            owners: Mutex::new(BTreeMap::new()),
            shutdown: tokio::sync::Notify::new(),
            terminal: None,
        };
        let b = Binding {
            origin: "https://allgather.example".into(),
            machine_id: "machine".into(),
            session_id: "session".into(),
            runtime_id: "runtime".into(),
            generation: "generation".into(),
            tower_session_id: "tower-session".into(),
            owner_id: "owner".into(),
            protocol: "codex-app-server".into(),
            harness: "codex".into(),
            cwd: "/work/project".into(),
            resume_session_id: None,
        };
        let c = ExecutionCommand {
            command_id: "command".into(),
            session_id: b.session_id.clone(),
            runtime_id: b.runtime_id.clone(),
            generation: b.generation.clone(),
            kind: "launch".into(),
            payload: parse(json!({})).unwrap(),
            expires_at: "2099-01-01T00:00:00Z".into(),
        };
        let first = host.journal.admit(&b, &c).unwrap();
        host.journal.begin_dispatch(&b, &c.command_id).unwrap();
        let latest = host
            .journal
            .finish_dispatch(&b, &c.command_id, true, None, None)
            .unwrap();
        let ack = |sent, reported| {
            json!({
                "namespace":b.namespace(), "kind":"receipt", "value":{
                    "command_id":c.command_id, "runtime_id":b.runtime_id, "generation":b.generation,
                    "receipt_revision":sent, "reported_revision":reported
                }
            })
        };
        host.dispatch("ack", ack(first.receipt_revision, latest.receipt_revision))
            .unwrap();
        let updates = host.journal.receipt_updates(&b.namespace()).unwrap();
        assert_eq!(updates, vec![latest.clone()]);
        host.dispatch("ack", ack(latest.receipt_revision, latest.receipt_revision))
            .unwrap();
        assert!(
            host.journal
                .receipt_updates(&b.namespace())
                .unwrap()
                .is_empty()
        );
        let error = host
            .dispatch(
                "ack",
                ack(latest.receipt_revision, latest.receipt_revision + 1),
            )
            .unwrap_err();
        assert_eq!(error.data.code, "receipt_history_lost");
        assert!(host.journal.state(&b).unwrap().1);
        assert_eq!(
            host.journal.losses(&b.namespace()).unwrap()[0].reason,
            "receipt_history_lost"
        );
    }
}
