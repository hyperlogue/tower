use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tower_sessions::{Backends, ManagedBackend, ManagedSession, Observation, PreparedSession};

struct StructuredBackend {
    protocol: String,
    store: Arc<Store>,
    states: Mutex<BTreeMap<String, String>>,
    unavailable: AtomicBool,
    allow_stop: AtomicBool,
    injected_id: Mutex<Option<String>>,
    pool_restarts: AtomicUsize,
}
impl StructuredBackend {
    fn new(protocol: &str, store: Arc<Store>) -> Self {
        Self {
            protocol: protocol.into(),
            store,
            states: Mutex::new(BTreeMap::new()),
            unavailable: AtomicBool::new(false),
            allow_stop: AtomicBool::new(true),
            injected_id: Mutex::new(None),
            pool_restarts: AtomicUsize::new(0),
        }
    }
    fn observation(&self, id: &str, status: &str) -> Observation {
        Observation {
            metadata: Default::default(),
            id: id.into(),
            binding: None,
            status: status.into(),
            pid: None,
            pid_start_time: None,
            caps: Capabilities {
                kill: status != "exited" && self.allow_stop.load(Ordering::Relaxed),
                ..Capabilities::unavailable("not_implemented")
            },
            terminal: None,
            recovery_pending: false,
        }
    }
}
impl ManagedBackend for StructuredBackend {
    fn protocol(&self) -> &str {
        &self.protocol
    }
    fn prepare(
        &self,
        id: &str,
        request: &SessionOpenParams,
    ) -> tower_sessions::Result<PreparedSession> {
        Ok(PreparedSession {
            cwd: request.cwd.clone(),
            native_session_id: None,
            harness: request.harness.clone(),
            binding: value(
                serde_json::json!({"conversation":format!("remote:{id}"),"private":"backend-private-data"}),
            )?,
            launch: value(serde_json::json!({"structured":true}))?,
            mode: None,
            caps: self.observation(id, "starting").caps,
            terminal: None,
        })
    }
    fn start(&self, s: &ManagedSession, token: &Secret) -> tower_sessions::Result<Observation> {
        let saved = self.store.managed_session(&s.id).unwrap();
        assert_eq!(saved.binding, s.binding);
        assert_eq!(self.store.agent(&s.id).unwrap().token_hash, hash(token));
        assert_eq!(s.protocol, self.protocol);
        self.states
            .lock()
            .unwrap()
            .insert(s.id.clone(), "idle".into());
        let mut observation = self.observation(&s.id, "starting");
        observation.recovery_pending = true;
        observation.binding = Some(value(
            serde_json::json!({"conversation":"server-assigned-handle","private":"backend-private-data"}),
        )?);
        Ok(observation)
    }
    fn observe(&self, sessions: &[ManagedSession]) -> tower_sessions::Result<Vec<Observation>> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(app("backend_unavailable", "offline"));
        }
        let states = self.states.lock().unwrap();
        let mut observations = sessions
            .iter()
            .map(|s| {
                assert_eq!(
                    serde_json::to_value(&s.binding).unwrap()["conversation"],
                    "server-assigned-handle"
                );
                assert_eq!(
                    s.protocol, self.protocol,
                    "backend must only receive its own bindings"
                );
                self.observation(&s.id, states.get(&s.id).map_or("lost", String::as_str))
            })
            .collect::<Vec<_>>();
        if let Some(id) = &*self.injected_id.lock().unwrap() {
            observations.push(self.observation(id, "exited"));
        }
        Ok(observations)
    }
    fn stop(&self, s: &ManagedSession) -> tower_sessions::Result<Observation> {
        self.states
            .lock()
            .unwrap()
            .insert(s.id.clone(), "exited".into());
        Ok(self.observation(&s.id, "exited"))
    }
    fn terminal(&self) -> Option<&dyn tower_sessions::terminal::TerminalAccess> {
        (self.protocol == "terminal").then_some(self)
    }
}
impl tower_sessions::terminal::TerminalAccess for StructuredBackend {
    fn attach_info(&self, _: &ManagedSession) -> tower_sessions::Result<AttachInfo> {
        Err(app("unsupported", "test backend"))
    }
    fn snapshot(&self, _: &ManagedSession) -> tower_sessions::Result<SnapshotResult> {
        Err(app("unsupported", "test backend"))
    }
    fn pool_info(&self) -> PoolInfo {
        PoolInfo {
            running: true,
            proto_version: None,
        }
    }
    fn restart_pool(&self) -> tower_sessions::Result<()> {
        self.pool_restarts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
fn install(service: &mut Service, default: &str, drivers: Vec<Arc<dyn ManagedBackend>>) {
    service.backends = Backends::new(default, drivers).unwrap();
}
fn open(s: &Service, protocol: Option<&str>) -> String {
    let mut p = serde_json::json!({"harness":"structured-agent","cwd":"~/work"});
    if let Some(protocol) = protocol {
        p["protocol"] = protocol.into();
    }
    call(s, "session.open", p, human()).unwrap()["id"]
        .as_str()
        .unwrap()
        .into()
}
#[test]
fn managed_structured_session_needs_no_process_or_terminal_and_recovers_its_binding() {
    let (_dir, store, mut s) = setup();
    let backend = Arc::new(StructuredBackend::new("acp", store.clone()));
    install(&mut s, "acp", vec![backend.clone()]);
    let id = open(&s, None);
    let record = call(&s, "agent.get", serde_json::json!({"id":id}), human()).unwrap();
    assert_eq!(record["ownership"], "owned");
    assert_eq!(record["protocol"], "acp");
    assert_eq!(record["status"], "idle");
    assert_eq!(record["caps"]["kill"], true);
    assert_eq!(record["caps"]["attach"], false);
    assert_eq!(record["caps"]["snapshot"], false);
    assert!(record["pid"].is_null());
    assert!(record["pool_session"].is_null());
    assert!(!record.to_string().contains("backend-private-data"));
    for method in ["session.attach_info", "session.snapshot"] {
        assert_eq!(
            call(&s, method, serde_json::json!({"id":id}), human())
                .unwrap_err()
                .data
                .code,
            "unsupported"
        );
    }
    assert!(!s.paths.pool_socket().exists());
    assert_eq!(
        serde_json::to_value(store.managed_session(&id).unwrap().binding).unwrap()["conversation"],
        "server-assigned-handle"
    );
    let restored =
        Service::new(s.paths.clone(), store.clone(), "test", s.backends.clone()).unwrap();
    restored.recover_registry().unwrap();
    let record = call(
        &restored,
        "agent.get",
        serde_json::json!({"id":id}),
        human(),
    )
    .unwrap();
    assert_eq!(record["status"], "idle");
    assert_eq!(record["caps"]["attach"], false);
    assert_eq!(
        call(&restored, "daemon.info", serde_json::json!({}), human()).unwrap()["pool"]["running"],
        false
    );
    call(
        &restored,
        "session.kill",
        serde_json::json!({"id":id}),
        human(),
    )
    .unwrap();
    assert_eq!(store.agent(&id).unwrap().record.status, "exited");
    let saved = store.managed_session(&id).unwrap();
    assert!(!saved.auto_resume);
    assert!(saved.launch.is_some());
    assert!(
        serde_json::to_string(&saved.binding)
            .unwrap()
            .contains("server-assigned-handle")
    );
    restored.maintain().unwrap();
    assert_eq!(store.agent(&id).unwrap().record.status, "exited");
}
#[test]
fn reconciliation_is_scoped_to_the_backend_and_missing_backends_do_not_become_terminals() {
    let (_dir, store, mut s) = setup();
    let a = Arc::new(StructuredBackend::new("acp", store.clone()));
    let b = Arc::new(StructuredBackend::new("custom", store.clone()));
    install(&mut s, "acp", vec![a.clone(), b.clone()]);
    let a_id = open(&s, None);
    let b_id = open(&s, Some("custom"));
    *a.injected_id.lock().unwrap() = Some(b_id.clone());
    s.sync_managed().unwrap();
    assert_eq!(store.agent(&b_id).unwrap().record.status, "idle");
    a.unavailable.store(true, Ordering::Relaxed);
    b.states
        .lock()
        .unwrap()
        .insert(b_id.clone(), "working".into());
    s.sync_managed().unwrap();
    assert_eq!(store.agent(&b_id).unwrap().record.status, "working");
    let restored = Service::new(
        s.paths.clone(),
        store.clone(),
        "test",
        Backends::new("custom", vec![b]).unwrap(),
    )
    .unwrap();
    restored.recover_registry().unwrap();
    assert_eq!(store.agent(&a_id).unwrap().record.status, "unknown");
    assert_eq!(
        store.agent(&a_id).unwrap().record.protocol.as_deref(),
        Some("acp")
    );
    assert!(!store.agent(&a_id).unwrap().record.caps.attach);
    assert_eq!(
        call(
            &restored,
            "session.kill",
            serde_json::json!({"id":a_id}),
            human()
        )
        .unwrap_err()
        .data
        .code,
        "unsupported"
    );
    assert_eq!(store.agent(&b_id).unwrap().record.status, "working");
    assert!(!restored.paths.pool_socket().exists());
}
#[test]
fn unmanaged_protocol_metadata_does_not_grant_management_or_terminal_capabilities() {
    let (_dir, store, s) = setup();
    for protocol in ["acp", "terminal", "future"] {
        let r = call(
            &s,
            "agent.register",
            serde_json::json!({"harness":"external","cwd":"~/work","protocol":protocol}),
            human(),
        )
        .unwrap();
        let record = store.agent(r["id"].as_str().unwrap()).unwrap().record;
        assert_eq!(record.ownership, "registered");
        assert_eq!(record.protocol.as_deref(), Some(protocol));
        assert!(!record.caps.attach && !record.caps.snapshot && !record.caps.kill);
        assert_eq!(
            call(
                &s,
                "session.kill",
                serde_json::json!({"id":record.id}),
                human()
            )
            .unwrap_err()
            .data
            .code,
            "not_owned"
        );
    }
    let list = call(
        &s,
        "agent.list",
        serde_json::json!({"filter":{"protocol":"acp"}}),
        human(),
    )
    .unwrap();
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert!(!s.paths.pool_socket().exists());
}

#[test]
fn management_does_not_override_a_backends_stop_capability() {
    let (_dir, store, mut service) = setup();
    let backend = Arc::new(StructuredBackend::new("acp", store.clone()));
    install(&mut service, "acp", vec![backend.clone()]);
    let id = open(&service, None);
    backend.allow_stop.store(false, Ordering::Relaxed);
    assert_eq!(
        call(
            &service,
            "session.kill",
            serde_json::json!({"id":id}),
            human()
        )
        .unwrap_err()
        .data
        .code,
        "unsupported"
    );
    assert_eq!(store.agent(&id).unwrap().record.status, "idle");
    assert!(store.managed_session(&id).unwrap().launch.is_some());
}
#[test]
fn pool_upgrade_can_require_idle_terminal_sessions() {
    let (_dir, store, mut s) = setup();
    let terminal = Arc::new(StructuredBackend::new("terminal", store.clone()));
    install(&mut s, "terminal", vec![terminal.clone()]);
    let id = open(&s, None);
    let set = |state: &str| {
        terminal
            .states
            .lock()
            .unwrap()
            .insert(id.clone(), state.into());
        s.sync_managed().unwrap();
    };
    let upgrade = |require_idle: bool| {
        call(
            &s,
            "pool.upgrade",
            serde_json::json!({"require_idle":require_idle}),
            human(),
        )
    };
    set("working");
    assert_eq!(upgrade(true).unwrap_err().data.code, "sessions_busy");
    assert_eq!(terminal.pool_restarts.load(Ordering::Relaxed), 0);
    upgrade(false).unwrap();
    set("waiting_approval");
    upgrade(true).unwrap();
    assert_eq!(terminal.pool_restarts.load(Ordering::Relaxed), 2);
}
