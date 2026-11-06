use crate::{Binding, Journal, Limits, Result, engine, error, internal};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};
use tower_protocol::{
    ExecutionCommand, ExecutionReceipt, ExecutionStatus, RuntimeCursor, RuntimeEvents,
};
use tower_sessions::{
    ManagedBackend, ManagedSession,
    execution::{
        Authorization, Authorize, ExecutionHost, HarnessControls, RuntimeUpdates, RuntimeView,
    },
};

/// The server-supplied policy check, bound once at startup.
#[derive(Clone, Default)]
pub(crate) struct Authority(Arc<OnceLock<Authorize>>);
impl Authority {
    pub(crate) async fn check(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        // Continued runtimes start before the server binds its policy.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let authorize = loop {
            if let Some(authorize) = self.0.get() {
                break authorize;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(error(
                    "authority_unavailable",
                    "Tower authorization is not ready",
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        authorize(Authorization {
            binding: b.clone(),
            command_id: c.command_id.clone(),
            kind: c.kind.clone(),
            cleanup: engine::cleanup_command(c),
            target: None,
        })
        .await
    }
    pub(crate) async fn check_target(
        &self,
        b: &Binding,
        c: &ExecutionCommand,
        target: &Binding,
    ) -> Result<()> {
        self.check(b, c).await?;
        let authorize = self
            .0
            .get()
            .ok_or_else(|| error("authority_unavailable", "Tower authorization is not ready"))?;
        authorize(Authorization {
            binding: b.clone(),
            command_id: c.command_id.clone(),
            kind: c.kind.clone(),
            cleanup: false,
            target: Some(target.clone()),
        })
        .await
    }
}

/// Owns managed runtimes inside the server: their durable journal, the
/// engine task of each live generation, and recovery after a restart.
pub struct Runtime {
    paths: tower_pal::Paths,
    journal: Arc<Journal>,
    owners: Mutex<BTreeMap<String, Arc<engine::Handle>>>,
    terminal: Arc<dyn ManagedBackend>,
    authority: Authority,
    tasks: tokio::runtime::Handle,
}
fn key(b: &Binding) -> String {
    serde_json::to_string(&(b.namespace(), &b.runtime_id, &b.generation)).expect("string tuple")
}
impl Runtime {
    /// Opens the journal and settles every generation the previous server
    /// left behind. Call it while holding the server's singleton lock.
    pub async fn open(
        paths: tower_pal::Paths,
        terminal: Arc<dyn ManagedBackend>,
    ) -> Result<Arc<Self>> {
        #[derive(Default, serde::Deserialize)]
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
        let live = recover(&paths, &journal).await?;
        journal.compact()?;
        let runtime = Arc::new(Self {
            paths,
            journal,
            owners: Mutex::new(BTreeMap::new()),
            terminal,
            authority: Authority::default(),
            tasks: tokio::runtime::Handle::current(),
        });
        for binding in live {
            let (handle, normal, control) = engine::channels();
            runtime.owners()?.insert(key(&binding), handle.clone());
            if let Some(command) = runtime.journal.force_command(&binding)? {
                handle
                    .normal
                    .try_send(command.clone())
                    .map_err(|_| error("runtime_capacity", "Recovery queue is full"))?;
                runtime.spawn_force(binding, command, handle, normal, control, true)?;
            } else {
                runtime.spawn(binding, handle, normal, control, engine::Start::Continue);
            }
        }
        let weak = Arc::downgrade(&runtime);
        runtime.tasks.spawn(async move {
            let mut maintenance = tokio::time::interval(Duration::from_secs(60));
            maintenance.tick().await;
            loop {
                maintenance.tick().await;
                let Some(runtime) = weak.upgrade() else { break };
                if runtime.journal.compact().is_err() {
                    eprintln!("tower: runtime journal compaction failed");
                }
            }
        });
        Ok(runtime)
    }
    pub(crate) fn context(&self) -> engine::Context {
        engine::Context {
            paths: self.paths.clone(),
            journal: self.journal.clone(),
            authority: self.authority.clone(),
        }
    }
    pub(crate) fn paths(&self) -> &tower_pal::Paths {
        &self.paths
    }
    /// Owners whose task has ended are dropped, so callers see the runtime
    /// as unowned instead of as a full queue.
    fn owners(&self) -> Result<MutexGuard<'_, BTreeMap<String, Arc<engine::Handle>>>> {
        let mut owners = self.owners.lock().map_err(internal)?;
        owners.retain(|_, h| !h.normal.is_closed());
        Ok(owners)
    }
    fn owner(&self, b: &Binding) -> Result<Option<Arc<engine::Handle>>> {
        Ok(self.owners()?.get(&key(b)).cloned())
    }
    pub(crate) fn terminal_session(&self, b: &Binding, live: bool) -> Result<ManagedSession> {
        if live && self.journal.state(b)?.1 {
            return Err(error(
                "runtime_fenced",
                "Terminal generation is no longer live",
            ));
        }
        crate::terminal::session(&self.journal, b)
    }
    fn spawn_owner(&self, b: Binding, c: ExecutionCommand) -> Result<ExecutionReceipt> {
        let mut owners = self.owners()?;
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
            .try_send(c.clone())
            .map_err(|_| error("runtime_capacity", "Runtime queue is full"))?;
        owners.insert(key(&b), handle.clone());
        drop(owners);
        if tower_sessions::execution::force_resume(&c)?.is_some() {
            if let Err(error) =
                self.spawn_force(b.clone(), c.clone(), handle, normal, control, false)
            {
                self.journal.force_failed(&b, &c, &error.data.code)?;
                return self.journal.status(
                    &b.namespace(),
                    &ExecutionStatus {
                        command_id: c.command_id,
                        runtime_id: b.runtime_id.clone(),
                        generation: b.generation.clone(),
                    },
                );
            }
        } else {
            self.spawn(b, handle, normal, control, engine::Start::Launch);
        }
        Ok(receipt)
    }
    fn spawn_force(
        &self,
        b: Binding,
        c: ExecutionCommand,
        handle: Arc<engine::Handle>,
        normal: tokio::sync::mpsc::Receiver<ExecutionCommand>,
        control: tokio::sync::mpsc::Receiver<engine::Control>,
        recover: bool,
    ) -> Result<()> {
        let targets = self
            .journal
            .force_targets(&b, &c.command_id)?
            .into_iter()
            .map(|target| Ok((target.clone(), self.owner(&target)?)))
            .collect::<Result<Vec<_>>>()?;
        for (_, owner) in &targets {
            if let Some(owner) = owner {
                owner.requests.retire_all()?;
            }
        }
        let failed = (b.clone(), c.clone(), self.journal.clone());
        let task = self.tasks.spawn(
            crate::force::Work {
                ctx: self.context(),
                binding: b,
                command: c,
                targets,
            }
            .run(handle, normal, control, recover),
        );
        self.tasks.spawn(async move {
            if task.await.is_err() {
                let _ = failed
                    .2
                    .force_failed(&failed.0, &failed.1, "cleanup_unconfirmed");
            }
        });
        Ok(())
    }
    fn spawn(
        &self,
        b: Binding,
        handle: Arc<engine::Handle>,
        normal: tokio::sync::mpsc::Receiver<ExecutionCommand>,
        control: tokio::sync::mpsc::Receiver<engine::Control>,
        start: engine::Start,
    ) {
        let failed = (b.clone(), self.journal.clone());
        let task = if b.protocol == "terminal" {
            self.tasks.spawn(crate::terminal::run(
                self.context(),
                b,
                self.terminal.clone(),
                normal,
                control,
                start == engine::Start::Continue,
            ))
        } else {
            self.tasks.spawn(engine::run(
                self.context(),
                b,
                handle,
                normal,
                control,
                start,
            ))
        };
        self.tasks.spawn(async move {
            if task.await.is_err() {
                let (binding, journal) = failed;
                let _ = journal.fence(&binding, "owner_task_failed", "unknown");
            }
        });
    }
}
impl ExecutionHost for Runtime {
    fn bind(&self, authorize: Authorize) {
        let _ = self.authority.0.set(authorize);
    }
    /// Suspends structured sessions so that the next server continues them.
    /// Terminal sessions live in the pool and need nothing.
    fn shutdown(&self) -> tower_sessions::execution::Future<()> {
        let owners: Vec<_> = (|| -> Result<Vec<_>> {
            let owners = self.owners()?;
            Ok(self
                .journal
                .bindings(None)?
                .iter()
                .filter(|b| b.protocol != "terminal")
                .filter_map(|b| owners.get(&key(b)).cloned())
                .collect())
        })()
        .unwrap_or_default();
        Box::pin(async move {
            for h in &owners {
                let _ = h.control.try_send(engine::Control::Suspend);
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            while owners.iter().any(|h| !h.normal.is_closed())
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }
    fn validate_new(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        self.journal.validate_new(b, c)
    }
    fn controls(&self) -> BTreeMap<String, HarnessControls> {
        let env = tower_pal::environment();
        let mut out = BTreeMap::new();
        for (harness, bin, protocol, permissions, configure, cancel) in [
            ("codex", "codex", "codex-app-server", true, true, "turn"),
            ("claude-code", "claude", "acp", true, false, "turn"),
            ("antigravity", "agy", "acp", false, false, "runtime"),
        ] {
            if tower_pal::find_program(bin, &env).is_some() {
                let mut capabilities = vec!["prompt".into(), "resume".into()];
                if permissions {
                    capabilities.push("permission_reply".into());
                    capabilities.push("elicitation_reply".into());
                }
                if configure {
                    capabilities.push("configure".into());
                }
                out.insert(
                    harness.into(),
                    HarnessControls {
                        protocol: protocol.into(),
                        capabilities,
                        cancel_mode: Some(cancel.into()),
                    },
                );
            }
        }
        out
    }
    fn execute(&self, b: &Binding, c: &ExecutionCommand) -> Result<ExecutionReceipt> {
        tower_sessions::execution::force_resume(c)?;
        let terminal = b.protocol == "terminal";
        let structured = b.protocol
            == if b.harness == "codex" {
                "codex-app-server"
            } else {
                "acp"
            }
            && matches!(b.harness.as_str(), "codex" | "claude-code" | "antigravity");
        if !(terminal || structured) || !std::path::Path::new(&b.cwd).is_absolute() {
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
            return self.journal.admit(b, c);
        }
        if terminal {
            crate::terminal::validate(b, c)?;
            if c.kind == "launch"
                && !self
                    .terminal
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
        let Some(handle) = self.owner(b)? else {
            if !matches!(c.kind.as_str(), "launch" | "resume") {
                return Err(error("runtime_lost", "Runtime owner is unavailable"));
            }
            return self.spawn_owner(b.clone(), c.clone());
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
        if matches!(c.kind.as_str(), "cancel" | "reply") {
            let permit = handle
                .control
                .clone()
                .try_reserve_owned()
                .map_err(|_| error("control_capacity", "Runtime control queue is full"))?;
            let receipt = self.journal.admit(b, c)?;
            permit.send(engine::Control::Command(c.clone()));
            Ok(receipt)
        } else {
            let permit = handle
                .normal
                .clone()
                .try_reserve_owned()
                .map_err(|_| error("runtime_capacity", "Runtime command queue is full"))?;
            let receipt = self.journal.admit(b, c)?;
            permit.send(c.clone());
            Ok(receipt)
        }
    }
    fn status(&self, ns: &str, c: &ExecutionStatus) -> Result<ExecutionReceipt> {
        self.journal.status(ns, c)
    }
    fn list(&self, namespace: Option<&str>) -> Result<Vec<RuntimeView>> {
        self.journal.views(namespace)
    }
    fn updates(&self, namespace: &str, connection: &str) -> Result<RuntimeUpdates> {
        if !crate::identifier(connection) {
            return Err(error("invalid_params", "Connection identity required"));
        }
        let receipts = self.journal.receipt_updates(namespace)?;
        let losses = self.journal.losses(namespace)?;
        let mut budget = tower_protocol::MAX_FRAME_BYTES.saturating_sub(
            serde_json::to_vec(&receipts).map_err(internal)?.len()
                + serde_json::to_vec(&losses).map_err(internal)?.len()
                + 4096,
        );
        let mut requests = Vec::new();
        let owners = self.owners()?;
        for binding in self.journal.bindings(Some(namespace))? {
            if let Some(h) = owners.get(&key(&binding)) {
                let committed = self.journal.cursor(&binding)?.cursor;
                requests.extend(h.requests.updates(connection, committed, &mut budget)?);
            }
        }
        Ok(RuntimeUpdates {
            receipts,
            losses,
            requests,
        })
    }
    fn batch(&self, b: &Binding) -> Result<Option<RuntimeEvents>> {
        self.journal.batch(b)
    }
    fn cursor(&self, b: &Binding) -> Result<RuntimeCursor> {
        self.journal.cursor(b)
    }
    fn reannounce(&self, b: &Binding) -> Result<()> {
        let (state, fenced, _) = self.journal.state(b)?;
        if fenced || self.owner(b)?.is_none() {
            return Ok(());
        }
        let m = self.journal.metadata(b)?;
        let payload = if b.protocol == "terminal" {
            if m.pointer("/observation/terminal/running") != Some(&Value::Bool(true)) {
                return Ok(());
            }
            crate::terminal::state_payload(b, &m, "running", false)
        } else {
            let id = if b.protocol == "acp" {
                "acp_session_id"
            } else {
                "native_session_id"
            };
            if !m[id].as_str().is_some_and(|s| !s.is_empty()) {
                return Ok(());
            }
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
            payload
        };
        self.journal.append(
            b,
            "runtime_state",
            serde_json::from_value(payload).map_err(internal)?,
            None,
            true,
        )?;
        Ok(())
    }
    fn acknowledge(&self, ns: &str, kind: &str, v: tower_protocol::Value) -> Result<()> {
        fn parse<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
            serde_json::from_value(v.clone())
                .map_err(|_| error("invalid_params", "Invalid runtime acknowledgement"))
        }
        let v = serde_json::to_value(v).map_err(internal)?;
        match kind {
            "request" => {
                let owners = self.owners()?;
                for binding in self.journal.bindings(Some(ns))? {
                    if let Some(h) = owners.get(&key(&binding)) {
                        h.requests.acknowledge(
                            v["source_request_id"].as_str().unwrap_or(""),
                            v["request_revision"].as_u64().unwrap_or(0),
                            v["connection"].as_str().unwrap_or(""),
                            v["peer_state"].as_str().unwrap_or(""),
                        )?;
                    }
                }
            }
            "receipt" => {
                let sent = v["receipt_revision"].as_u64().unwrap_or(0);
                if let Some(reported) = v["reported_revision"].as_u64()
                    && reported > sent
                {
                    let command: ExecutionStatus = parse(&v)?;
                    let retained = self.journal.status(ns, &command)?;
                    if reported > retained.receipt_revision {
                        let b =
                            self.journal
                                .binding(ns, &command.runtime_id, &command.generation)?;
                        self.fence(&b, "receipt_history_lost")?;
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
            "events" => {
                let cursor: RuntimeCursor = parse(&v)?;
                let b = self
                    .journal
                    .binding(ns, &cursor.runtime_id, &cursor.generation)?;
                self.journal.acknowledge_events(&b, &cursor)?;
            }
            "loss" => {
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
        Ok(())
    }
    fn fence(&self, b: &Binding, reason: &str) -> Result<()> {
        let reason = if crate::identifier(reason) {
            reason
        } else {
            "history_conflict"
        };
        self.journal.fence(b, reason, "unknown")?;
        if let Some(h) = self.owner(b)? {
            h.control
                .try_send(engine::Control::Stop(reason.into()))
                .map_err(|_| error("control_capacity", "Cannot enqueue runtime stop"))?;
        }
        Ok(())
    }
    fn stop(&self, b: &Binding) -> Result<()> {
        match self.owner(b)? {
            Some(h) => h
                .control
                .try_send(engine::Control::Stop("local_stop".into()))
                .map_err(|_| error("control_capacity", "Cannot enqueue runtime stop"))?,
            None => {
                self.journal.fence(b, "local_stop", "unknown")?;
            }
        }
        for _ in 0..400 {
            let (_, fenced, termination) = self.journal.state(b)?;
            if fenced && termination == "confirmed" {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(error(
            "stop_unconfirmed",
            "Runtime termination was not confirmed",
        ))
    }
}

/// Settles generations left by a previous server and returns the live ones
/// to continue: terminal generations whose pool sessions may still run, and
/// structured generations whose native session can be reopened.
async fn recover(paths: &tower_pal::Paths, journal: &Journal) -> Result<Vec<Binding>> {
    let boot = tower_pal::boot_id().map_err(internal)?;
    let mut live = Vec::new();
    let mut processes = Vec::new();
    for binding in journal.bindings(None)? {
        let (_, fenced, termination) = journal.state(&binding)?;
        if fenced {
            if termination == "confirmed" {
                engine::cleanup_environment(paths, &binding);
            }
            continue;
        }
        if journal.force_command(&binding)?.is_some() {
            live.push(binding);
            continue;
        }
        let m = journal.metadata(&binding)?;
        if journal.force_phase(&binding)?.as_deref() == Some("dispatched")
            && !m[if binding.protocol == "acp" {
                "acp_session_id"
            } else {
                "native_session_id"
            }]
            .is_string()
        {
            journal.fence(&binding, "dispatch_unknown", "unknown")?;
            continue;
        }
        let rebooted = m["boot_id"].as_str().is_some_and(|old| old != boot);
        if binding.protocol == "terminal" {
            if rebooted {
                journal.fence(&binding, "terminal_owner_lost", "confirmed")?;
            } else {
                live.push(binding);
            }
            continue;
        }
        // A previous server that crashed can leave its ACP agent running.
        let process = (binding.protocol == "acp" && !rebooted)
            .then(|| {
                Some(tower_pal::ProcessIdentity {
                    pid: m["pid"].as_u64().and_then(|v| u32::try_from(v).ok())?,
                    parent: 0,
                    start_time: m["pid_start_time"].as_u64()?,
                })
            })
            .flatten();
        if let Some(identity) = process {
            let _ = tower_pal::kill_protocol_group(identity);
        }
        processes.push((binding, process));
    }
    for _ in 0..40 {
        if processes.iter().all(|(_, process)| {
            process.is_none_or(|i| tower_pal::protocol_group_terminated(i).unwrap_or(false))
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for (binding, process) in processes {
        if process.is_some_and(|i| !tower_pal::protocol_group_terminated(i).unwrap_or(false)) {
            journal.fence(&binding, "owner_restarted", "unknown")?;
        } else {
            live.push(binding);
        }
    }
    Ok(live)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoTerminal;
    impl ManagedBackend for NoTerminal {
        fn protocol(&self) -> &str {
            "terminal"
        }
        fn prepare(
            &self,
            _: &str,
            _: &tower_protocol::SessionOpenParams,
        ) -> Result<tower_sessions::PreparedSession> {
            Err(error("unsupported", "No terminal backend"))
        }
        fn start(
            &self,
            _: &ManagedSession,
            _: &tower_protocol::Secret,
        ) -> Result<tower_sessions::Observation> {
            Err(error("unsupported", "No terminal backend"))
        }
        fn observe(&self, _: &[ManagedSession]) -> Result<Vec<tower_sessions::Observation>> {
            Ok(Vec::new())
        }
        fn stop(&self, _: &ManagedSession) -> Result<tower_sessions::Observation> {
            Err(error("unsupported", "No terminal backend"))
        }
    }
    fn host(path: &std::path::Path) -> Runtime {
        Runtime {
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
            terminal: Arc::new(NoTerminal),
            authority: Authority::default(),
            tasks: tokio::runtime::Handle::current(),
        }
    }
    fn binding() -> Binding {
        Binding {
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
        }
    }
    fn launch(b: &Binding) -> ExecutionCommand {
        ExecutionCommand {
            command_id: "command".into(),
            session_id: b.session_id.clone(),
            runtime_id: b.runtime_id.clone(),
            generation: b.generation.clone(),
            kind: "launch".into(),
            payload: serde_json::from_value(json!({})).unwrap(),
            expires_at: "2099-01-01T00:00:00Z".into(),
        }
    }

    #[tokio::test]
    async fn stop_after_the_owner_ended_settles_instead_of_failing() {
        let root = tempfile::tempdir().unwrap();
        let host = host(root.path());
        let b = binding();
        host.journal.admit(&b, &launch(&b)).unwrap();
        let (handle, normal, control) = engine::channels();
        host.owners.lock().unwrap().insert(key(&b), handle);
        drop((normal, control));
        host.journal
            .fence(&b, "native_exited", "confirmed")
            .unwrap();
        host.stop(&b).unwrap();
        let mut prompt = launch(&b);
        prompt.command_id = "prompt".into();
        prompt.kind = "prompt".into();
        let error = host.execute(&b, &prompt).unwrap_err();
        assert_eq!(error.data.code, "runtime_lost");
    }

    #[tokio::test]
    async fn receipt_ack_keeps_unsent_evidence_and_fences_unrecoverable_history() {
        let root = tempfile::tempdir().unwrap();
        let host = host(root.path());
        let b = binding();
        let c = launch(&b);
        let first = host.journal.admit(&b, &c).unwrap();
        host.journal.begin_dispatch(&b, &c.command_id).unwrap();
        let latest = host
            .journal
            .finish_dispatch(&b, &c.command_id, true, None, None)
            .unwrap();
        let ack = |sent, reported| {
            host.acknowledge(
                &b.namespace(),
                "receipt",
                serde_json::from_value(json!({
                    "command_id":c.command_id, "runtime_id":b.runtime_id, "generation":b.generation,
                    "receipt_revision":sent, "reported_revision":reported
                }))
                .unwrap(),
            )
        };
        ack(first.receipt_revision, latest.receipt_revision).unwrap();
        let updates = host.journal.receipt_updates(&b.namespace()).unwrap();
        assert_eq!(updates, vec![latest.clone()]);
        ack(latest.receipt_revision, latest.receipt_revision).unwrap();
        assert!(
            host.journal
                .receipt_updates(&b.namespace())
                .unwrap()
                .is_empty()
        );
        let error = ack(latest.receipt_revision, latest.receipt_revision + 1).unwrap_err();
        assert_eq!(error.data.code, "receipt_history_lost");
        assert!(host.journal.state(&b).unwrap().1);
        assert_eq!(
            host.journal.losses(&b.namespace()).unwrap()[0].reason,
            "receipt_history_lost"
        );
    }
}
