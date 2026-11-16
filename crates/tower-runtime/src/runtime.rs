use crate::{Binding, Journal, Limits, Result, engine, error, internal};
use serde_json::Value;
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
        })
        .await
    }
}

/// Owns managed runtimes inside the server: their durable journal, the
/// engine task of each live generation, and recovery after a restart.
pub struct Runtime {
    paths: tower_pal::Paths,
    acp: crate::proxy::Configuration,
    proxies: Mutex<BTreeMap<String, Arc<crate::proxy::Proxy>>>,
    journal: Arc<Journal>,
    owners: Mutex<BTreeMap<String, Arc<engine::Handle>>>,
    terminal: Arc<dyn ManagedBackend>,
    terminal_changes: tokio::sync::watch::Receiver<u64>,
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
        database: Arc<tower_store::Database>,
        terminal: Arc<dyn ManagedBackend>,
    ) -> Result<Arc<Self>> {
        #[derive(Default, serde::Deserialize)]
        #[serde(default)]
        struct Configuration {
            runtime: Limits,
            acp: crate::proxy::Configuration,
        }
        let configuration: Configuration =
            tower_pal::read_config(&paths.config_home.join("tower/config.toml"))
                .map_err(internal)?
                .map(|text| toml::from_str(&text))
                .transpose()
                .map_err(|_| error("invalid_config", "Invalid runtime limits"))?
                .unwrap_or_default();
        let limits = configuration.runtime;
        if limits.control_reserve_bytes < 65536
            || limits.receipt_count == 0
            || limits.receipt_retention_days == 0
        {
            return Err(error(
                "invalid_config",
                "Runtime limits must reserve lifecycle and receipt capacity",
            ));
        }
        let journal = Arc::new(Journal::new(database, limits)?);
        let live = recover(&journal).await?;
        journal.compact()?;
        let (changed, terminal_changes) = tokio::sync::watch::channel(0u64);
        terminal.watch(Arc::new(move || {
            changed.send_modify(|n| *n = n.wrapping_add(1))
        }));
        let runtime = Arc::new(Self {
            paths,
            acp: configuration.acp,
            proxies: Mutex::new(BTreeMap::new()),
            journal,
            owners: Mutex::new(BTreeMap::new()),
            terminal,
            terminal_changes,
            authority: Authority::default(),
            tasks: tokio::runtime::Handle::current(),
        });
        for binding in live {
            let (handle, normal, control) = engine::channels();
            runtime.owners()?.insert(key(&binding), handle.clone());
            runtime.spawn(binding, normal, control, engine::Start::Continue);
        }
        let weak = Arc::downgrade(&runtime);
        runtime.tasks.spawn(async move {
            let mut maintenance = tokio::time::interval(Duration::from_secs(60));
            maintenance.tick().await;
            loop {
                maintenance.tick().await;
                let Some(runtime) = weak.upgrade() else { break };
                if let Ok(mut proxies) = runtime.proxies.lock() {
                    proxies.retain(|_, proxy| !proxy.retired());
                }
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
    pub(crate) fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        self.acp.harnesses()
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
        self.spawn(b, normal, control, engine::Start::Launch);
        Ok(receipt)
    }
    fn spawn(
        &self,
        b: Binding,
        normal: tokio::sync::mpsc::Receiver<ExecutionCommand>,
        control: tokio::sync::mpsc::Receiver<engine::Control>,
        start: engine::Start,
    ) {
        let failed = (b.clone(), self.journal.clone());
        let task = self.tasks.spawn(crate::terminal::run(
            self.context(),
            b,
            self.terminal.clone(),
            self.terminal_changes.clone(),
            normal,
            control,
            start == engine::Start::Continue,
        ));
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
    /// Closes ACP connections before shutdown; clients explicitly reconnect.
    /// Terminal sessions live in the pool and need nothing.
    fn shutdown(&self) -> tower_sessions::execution::Future<()> {
        let proxies: Vec<_> = self
            .proxies
            .lock()
            .map(|proxies| proxies.values().cloned().collect())
            .unwrap_or_default();
        Box::pin(async move {
            for proxy in &proxies {
                proxy.stop();
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            while proxies.iter().any(|proxy| !proxy.closed())
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    }
    fn validate_new(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        self.journal.validate_new(b, c)
    }
    fn controls(&self) -> BTreeMap<String, HarnessControls> {
        self.harnesses()
            .into_iter()
            .map(|h| {
                (
                    h.harness,
                    HarnessControls {
                        protocol: h.protocol,
                        capabilities: vec![],
                        cancel_mode: Some("runtime".into()),
                    },
                )
            })
            .collect()
    }
    fn acp_send(
        &self,
        ns: &str,
        runtime: &str,
        generation: &str,
        sequence: u64,
        frame: &str,
    ) -> Result<()> {
        let b = self.journal.binding(ns, runtime, generation)?;
        if self.journal.state(&b)?.1 {
            return Err(error("acp_closed", "ACP connection is closed"));
        }
        self.proxies
            .lock()
            .map_err(internal)?
            .get(&key(&b))
            .ok_or_else(|| error("acp_closed", "ACP connection is unavailable"))?
            .send(sequence, frame)
    }
    fn acp_ack(&self, ns: &str, runtime: &str, generation: &str, sequence: u64) -> Result<()> {
        let b = self.journal.binding(ns, runtime, generation)?;
        self.proxies
            .lock()
            .map_err(internal)?
            .get(&key(&b))
            .ok_or_else(|| error("acp_closed", "ACP connection is unavailable"))?
            .acknowledge(sequence)
    }
    fn execute(&self, b: &Binding, c: &ExecutionCommand) -> Result<ExecutionReceipt> {
        if b.protocol == "acp" {
            if c.kind != "launch" {
                return Err(error(
                    "acp_required",
                    "Exchange native ACP frames through acp.send",
                ));
            }
            let argv = self
                .acp
                .agents
                .get(&b.harness)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| error("unsupported", "Configure an ACP agent command"))?
                .clone();
            let mut proxies = self.proxies.lock().map_err(internal)?;
            if self
                .journal
                .binding(&b.namespace(), &b.runtime_id, &b.generation)
                .is_ok()
                && !proxies.contains_key(&key(b))
            {
                return Err(error(
                    "acp_closed",
                    "Open a new ACP connection and load the native session",
                ));
            }
            let receipt = self.journal.admit(b, c)?;
            proxies
                .entry(key(b))
                .or_insert_with(|| crate::proxy::spawn(self.context(), b.clone(), c.clone(), argv));
            return Ok(receipt);
        }
        if b.protocol != "terminal" {
            return Err(error(
                "acp_required",
                "Legacy managed runtimes were removed; configure an ACP adapter",
            ));
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
        {
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
            if c.kind != "launch" {
                return Err(error("runtime_lost", "Runtime owner is unavailable"));
            }
            return self.spawn_owner(b.clone(), c.clone());
        };
        if c.kind != "cancel" {
            return Err(error(
                "runtime_conflict",
                "A terminal runtime already has a launch",
            ));
        }
        let permit = handle
            .control
            .clone()
            .try_reserve_owned()
            .map_err(|_| error("control_capacity", "Runtime control queue is full"))?;
        let receipt = self.journal.admit(b, c)?;
        permit.send(engine::Control::Command(c.clone()));
        Ok(receipt)
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
        let mut acp_frames = Vec::new();
        let proxies = self.proxies.lock().map_err(internal)?;
        for b in self.journal.bindings(Some(namespace))? {
            if let Some(proxy) = proxies.get(&key(&b))
                && let Some((sequence, frame)) = proxy.peek()?
            {
                acp_frames.push(tower_protocol::AcpFrame {
                    runtime_id: b.runtime_id,
                    generation: b.generation,
                    sequence,
                    frame,
                });
            }
        }
        let losses = self
            .journal
            .losses(namespace)?
            .into_iter()
            .filter(|loss| {
                !acp_frames
                    .iter()
                    .any(|frame| frame.runtime_id == loss.runtime_id)
            })
            .collect();
        Ok(RuntimeUpdates {
            receipts,
            losses,
            acp_frames,
        })
    }

    fn batch(&self, b: &Binding) -> Result<Option<RuntimeEvents>> {
        self.journal.batch(b)
    }
    fn cursor(&self, b: &Binding) -> Result<RuntimeCursor> {
        self.journal.cursor(b)
    }
    fn reannounce(&self, b: &Binding) -> Result<()> {
        let (_state, fenced, _) = self.journal.state(b)?;
        if fenced || self.owner(b)?.is_none() {
            return Ok(());
        }
        let m = self.journal.metadata(b)?;
        if b.protocol != "terminal"
            || m.pointer("/observation/terminal/running") != Some(&Value::Bool(true))
        {
            return Ok(());
        }
        let payload = crate::terminal::state_payload(b, &m, "running", false);
        self.journal
            .append_state(b, serde_json::from_value(payload).map_err(internal)?)?;
        Ok(())
    }
    fn acknowledge(&self, ns: &str, kind: &str, v: tower_protocol::Value) -> Result<()> {
        fn parse<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
            serde_json::from_value(v.clone())
                .map_err(|_| error("invalid_params", "Invalid runtime acknowledgement"))
        }
        let v = serde_json::to_value(v).map_err(internal)?;
        match kind {
            "acp" => {
                self.acp_ack(
                    ns,
                    v["runtime_id"].as_str().unwrap_or(""),
                    v["generation"].as_str().unwrap_or(""),
                    v["sequence"].as_u64().unwrap_or(0),
                )?;
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
        if let Some(proxy) = self.proxies.lock().map_err(internal)?.get(&key(b)) {
            proxy.stop();
        }
        if let Some(h) = self.owner(b)? {
            h.control
                .try_send(engine::Control::Stop(reason.into()))
                .map_err(|_| error("control_capacity", "Cannot enqueue runtime stop"))?;
        }
        Ok(())
    }
    fn stop(&self, b: &Binding) -> Result<()> {
        let (_, fenced, termination) = self.journal.state(b)?;
        if fenced && termination == "confirmed" {
            return Ok(());
        }
        let proxy = self.proxies.lock().map_err(internal)?.get(&key(b)).cloned();
        if let Some(proxy) = proxy {
            proxy.stop();
        } else {
            match self.owner(b)? {
                Some(h) => h
                    .control
                    .try_send(engine::Control::Stop("local_stop".into()))
                    .map_err(|_| error("control_capacity", "Cannot enqueue runtime stop"))?,
                None => {
                    self.journal.fence(b, "local_stop", "unknown")?;
                }
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
/// to continue: terminal generations whose pool sessions may still run.
async fn recover(journal: &Journal) -> Result<Vec<Binding>> {
    let boot = tower_pal::boot_id().map_err(internal)?;
    let mut live = Vec::new();
    for binding in journal.bindings(None)? {
        if journal.state(&binding)?.1 {
            continue;
        }
        let m = journal.metadata(&binding)?;
        let rebooted = m["boot_id"].as_str().is_some_and(|old| old != boot);
        if binding.protocol == "terminal" {
            if rebooted {
                journal.fence(&binding, "terminal_owner_lost", "confirmed")?;
            } else {
                live.push(binding);
            }
            continue;
        }
        let process = if binding.protocol == "acp" && !rebooted {
            m["pid"]
                .as_u64()
                .zip(m["pid_start_time"].as_u64())
                .and_then(|(pid, start_time)| {
                    u32::try_from(pid)
                        .ok()
                        .map(|pid| tower_pal::ProcessIdentity {
                            pid,
                            parent: 0,
                            start_time,
                        })
                })
        } else {
            None
        };
        if let Some(process) = process {
            let _ = tower_pal::kill_protocol_group(process);
        }
        let confirmed = rebooted
            || process.is_some_and(|p| tower_pal::protocol_group_terminated(p).unwrap_or(false));
        journal.fence(
            &binding,
            if m["transport"] == "acp-proxy.v1" {
                "acp_connection_lost"
            } else {
                "acp_migration_required"
            },
            if confirmed { "confirmed" } else { "unknown" },
        )?;
    }
    Ok(live)
}
