use super::*;
use tower_sessions::{
    ManagedSession,
    execution::{ExecutionHost, RuntimeBinding, RuntimeUpdates, RuntimeView},
};
impl Service {
    fn execution_host(&self) -> std::result::Result<&dyn ExecutionHost, RpcError> {
        self.backends
            .execution()
            .ok_or_else(|| app("unsupported", "Managed structured runtimes are unavailable"))
    }
    pub(crate) fn execution_namespace(&self) -> std::result::Result<String, RpcError> {
        let url = String::from_utf8(
            tower_pal::read_private(&self.paths.state.join("allgather-server"))
                .map_err(|_| app("unauthorized", "Enrollment is unavailable"))?,
        )
        .map_err(|_| app("unauthorized", "Invalid enrollment"))?;
        let origin = crate::allgather::enrollment::origin(&url)
            .map_err(|_| app("unauthorized", "Invalid enrollment"))?;
        serde_json::to_string(&(origin, self.store.host().map_err(storage)?.node_name))
            .map_err(|_| app("internal_error", "Cannot encode enrollment identity"))
    }
    pub(crate) fn execution_views(
        &self,
        epoch: u64,
    ) -> std::result::Result<Vec<RuntimeView>, RpcError> {
        self.remote_actor(epoch)?;
        self.execution_host()?
            .list(Some(&self.execution_namespace()?))
    }
    pub(crate) fn execution_updates(
        &self,
        epoch: u64,
        connection: String,
    ) -> std::result::Result<RuntimeUpdates, RpcError> {
        self.remote_actor(epoch)?;
        self.execution_host()?
            .updates(&self.execution_namespace()?, &connection)
    }
    pub(crate) fn execution_reannounce(
        &self,
        b: RuntimeBinding,
        epoch: u64,
    ) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        if b.namespace() != self.execution_namespace()? {
            return Err(app("unauthorized", "Runtime belongs to another enrollment"));
        }
        self.execution_host()?.reannounce(&b)
    }
    pub(crate) fn execution_batch(
        &self,
        b: RuntimeBinding,
        epoch: u64,
    ) -> std::result::Result<Option<RuntimeEvents>, RpcError> {
        self.remote_actor(epoch)?;
        if b.namespace() != self.execution_namespace()? {
            return Err(app("unauthorized", "Runtime belongs to another enrollment"));
        }
        self.execution_host()?.batch(&b)
    }
    pub(crate) fn execution_cursor(
        &self,
        b: RuntimeBinding,
        epoch: u64,
    ) -> std::result::Result<RuntimeCursor, RpcError> {
        self.remote_actor(epoch)?;
        if b.namespace() != self.execution_namespace()? {
            return Err(app("unauthorized", "Runtime belongs to another enrollment"));
        }
        self.execution_host()?.cursor(&b)
    }
    pub(crate) fn execution_ack(
        &self,
        kind: String,
        v: Value,
        epoch: u64,
    ) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        self.execution_host()?
            .acknowledge(&self.execution_namespace()?, &kind, v)
    }
    pub(crate) fn execution_fence(
        &self,
        b: RuntimeBinding,
        reason: String,
        epoch: u64,
    ) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        if b.namespace() != self.execution_namespace()? {
            return Err(app("unauthorized", "Runtime belongs to another enrollment"));
        }
        self.execution_host()?.fence(&b, &reason)
    }
    pub(super) fn authorize_runtime(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<Value, RpcError> {
        #[derive(Deserialize)]
        struct Check {
            credential: Secret,
            binding: RuntimeBinding,
            kind: String,
            cleanup: bool,
            expires_at: String,
            command_id: String,
        }
        let p: Check = params(&request.params)?;
        if peer.uid != tower_pal::uid()
            || peer.pid.is_none()
            || request.context.bearer_token.is_some()
            || request.context.agent_token.is_some()
            || !tower_pal::read_private(&self.paths.state.join("runtime/owner.key"))
                .is_ok_and(|key| bool::from(key.as_slice().ct_eq(p.credential.0.as_bytes())))
        {
            return Err(app("unauthorized", "Runtime owner credential required"));
        }
        if p.command_id.is_empty() || p.expires_at.is_empty() {
            return Err(invalid("Command identity and deadline required"));
        }
        let session = self
            .store
            .managed_session(&p.binding.tower_session_id)
            .map_err(storage)?;
        let stored: RuntimeBinding = params(&session.binding)?;
        if serde_json::to_value(&stored).ok() != serde_json::to_value(&p.binding).ok() {
            return Err(app(
                "runtime_fenced",
                "Runtime binding is no longer current",
            ));
        }
        let cleanup = p.cleanup && matches!(p.kind.as_str(), "cancel" | "reply");
        if cleanup {
            return value(serde_json::json!({"authorized":true}));
        }
        let actor = if stored.origin == "local" {
            Actor::Human
        } else {
            if stored.namespace() != self.execution_namespace()? {
                return Err(app("unauthorized", "Enrollment changed"));
            }
            let actor = Actor::Allgather {
                scope: self.remote_scopes()?,
            };
            require(&actor, "acp:control")?;
            if !self.policy_ready() {
                return Err(app(
                    "denied",
                    "A current control connection and applied policy are required",
                ));
            }
            actor
        };
        let record = self.store.agent(&session.id).map_err(storage)?.record;
        self.session_policy(
            &actor,
            crate::policy::Subject {
                id: &record.id,
                harness: Some(&record.harness),
                cwd: Some(&record.cwd),
            },
        )?;
        value(serde_json::json!({"authorized":true}))
    }
    pub(super) fn execution_dispatch(
        &self,
        request: &Request,
        epoch: u64,
    ) -> std::result::Result<Value, RpcError> {
        if !self.allgather_config.managed {
            return Err(app("unsupported", "Managed execution is disabled"));
        }
        let actor = self.remote_actor(epoch)?;
        let host = self.execution_host()?;
        let namespace = self.execution_namespace()?;
        if request.method == "command.status" {
            require(&actor, "read")?;
            let p: ExecutionStatus = params(&request.params)?;
            return value(host.status(&namespace, &p)?);
        }
        require(&actor, "acp:control")?;
        let c: ExecutionCommand = params(&request.params)?;
        let payload: serde_json::Value =
            serde_json::to_value(&c.payload).map_err(|_| invalid("Invalid command payload"))?;
        let cleanup = c.kind == "cancel"
            || (c.kind == "reply"
                && (payload
                    .pointer("/response/outcome/optionId")
                    .and_then(serde_json::Value::as_str)
                    == Some("reject_once")
                    || payload
                        .pointer("/response/outcome/outcome")
                        .and_then(serde_json::Value::as_str)
                        == Some("cancelled")
                    || matches!(
                        payload
                            .pointer("/response/action")
                            .and_then(serde_json::Value::as_str),
                        Some("decline" | "cancel")
                    )));
        let views = host.list(Some(&namespace))?;
        let mut binding = views
            .iter()
            .find(|v| v.binding.runtime_id == c.runtime_id)
            .map(|v| v.binding.clone());
        if binding.is_none() {
            for s in self.store.managed_sessions().map_err(storage)? {
                if matches!(s.protocol.as_str(), "acp" | "codex-app-server")
                    && let Ok(b) = params::<RuntimeBinding>(&s.binding)
                    && b.namespace() == namespace
                    && b.runtime_id == c.runtime_id
                {
                    binding = Some(b);
                    break;
                }
            }
        }
        if let Some(b) = &binding {
            if b.generation != c.generation || b.session_id != c.session_id {
                return Err(app(
                    "runtime_conflict",
                    "Command execution identity changed",
                ));
            }
            let status = host.status(
                &namespace,
                &ExecutionStatus {
                    command_id: c.command_id.clone(),
                    runtime_id: c.runtime_id.clone(),
                    generation: c.generation.clone(),
                },
            )?;
            if status.state != "missing" {
                return value(host.execute(b, &c)?);
            }
        }
        if !cleanup && !self.policy_ready() {
            return Err(app(
                "denied",
                "Managed commands require a current control connection and applied policy",
            ));
        }
        let new_generation = binding.is_none();
        let b = if let Some(b) = binding {
            b
        } else {
            if !matches!(c.kind.as_str(), "launch" | "resume") {
                return Err(app("not_found", "Managed runtime is unknown"));
            }
            let harness = payload["harness"]
                .as_str()
                .ok_or_else(|| invalid("Harness is required"))?;
            let protocol = payload["protocol"]
                .as_str()
                .ok_or_else(|| invalid("Managed protocol is required"))?;
            if host
                .controls()
                .get(harness)
                .is_none_or(|profile| profile.protocol != protocol)
            {
                return Err(app("unsupported", "Harness adapter is unavailable"));
            }
            let cwd = self
                .paths
                .absolute_cwd(
                    payload["cwd"]
                        .as_str()
                        .ok_or_else(|| invalid("Working directory is required"))?,
                )
                .map_err(|_| invalid("Invalid working directory"))?
                .canonicalize()
                .map_err(|_| invalid("Working directory must exist"))?
                .to_string_lossy()
                .into_owned();
            let previous = views
                .iter()
                .rev()
                .find(|v| v.binding.session_id == c.session_id);
            let resume = payload["resume_session_id"].as_str().map(str::to_owned);
            if c.kind == "resume"
                && resume.as_ref().is_none_or(|s| {
                    s.is_empty() || s.len() > 160 || s.chars().any(char::is_control)
                })
            {
                return Err(invalid("Resume requires a known native session identity"));
            }
            let id = if c.kind == "resume" {
                let previous = views
                    .iter()
                    .rev()
                    .find(|v| {
                        v.binding.session_id == c.session_id
                            && serde_json::to_value(&v.metadata).ok().is_some_and(|m| {
                                m[if protocol == "acp" {
                                    "acp_session_id"
                                } else {
                                    "native_session_id"
                                }]
                                .as_str()
                                    == resume.as_deref()
                            })
                    })
                    .ok_or_else(|| {
                        app(
                            "resume_unavailable",
                            "No prior execution is bound to this logical session",
                        )
                    })?;
                let metadata: serde_json::Value = serde_json::to_value(&previous.metadata)
                    .map_err(|_| invalid("Invalid native metadata"))?;
                if previous.binding.harness != harness
                    || previous.binding.protocol != protocol
                    || !previous.fenced
                    || (previous.termination != "confirmed" && payload.get("takeover").is_none())
                    || resume.as_deref()
                        != metadata[if protocol == "acp" {
                            "acp_session_id"
                        } else {
                            "native_session_id"
                        }]
                        .as_str()
                {
                    return Err(app(
                        "resume_unavailable",
                        "Previous runtime or native history does not permit this resume",
                    ));
                }
                previous.binding.tower_session_id.clone()
            } else {
                if previous.is_some() || resume.is_some() {
                    return Err(app(
                        "runtime_conflict",
                        "Use resume for an existing logical session",
                    ));
                }
                format!(
                    "{}-{}",
                    self.store.host().map_err(storage)?.node_name,
                    base32(
                        &random(10)
                            .map_err(|_| app("internal_error", "Random source unavailable"))?
                    )
                )
            };
            let (origin, machine_id): (String, String) = serde_json::from_str(&namespace)
                .map_err(|_| invalid("Invalid enrollment namespace"))?;
            RuntimeBinding {
                origin,
                machine_id,
                session_id: c.session_id.clone(),
                runtime_id: c.runtime_id.clone(),
                generation: c.generation.clone(),
                tower_session_id: id,
                owner_id: format!(
                    "owner_{}",
                    base32(
                        &random(16)
                            .map_err(|_| app("internal_error", "Random source unavailable"))?
                    )
                ),
                protocol: protocol.into(),
                harness: harness.into(),
                cwd,
                resume_session_id: resume,
            }
        };
        if !cleanup {
            let cwd = self
                .paths
                .display_cwd(&b.cwd)
                .map_err(|_| invalid("Invalid working directory"))?;
            self.session_policy(
                &actor,
                crate::policy::Subject {
                    id: &b.tower_session_id,
                    harness: Some(&b.harness),
                    cwd: Some(&cwd),
                },
            )?;
        }
        if !new_generation {
            let current = self
                .store
                .managed_session(&b.tower_session_id)
                .map_err(storage)?;
            let current: RuntimeBinding = params(&current.binding)?;
            if current.namespace() != b.namespace()
                || current.runtime_id != b.runtime_id
                || current.generation != b.generation
            {
                return Err(app(
                    "runtime_fenced",
                    "This execution is no longer the current session owner",
                ));
            }
        }
        self.reserve_execution(&b)?;
        let receipt = host.execute(&b, &c)?;
        self.publish()?;
        value(receipt)
    }
    fn reserve_execution(&self, b: &RuntimeBinding) -> std::result::Result<(), RpcError> {
        let session = ManagedSession {
            id: b.tower_session_id.clone(),
            protocol: b.protocol.clone(),
            binding: value(b)?,
            launch: None,
            auto_resume: false,
        };
        match self.store.managed_session(&session.id) {
            Ok(old) => {
                if old.protocol != session.protocol {
                    return Err(app("runtime_conflict", "Managed session protocol changed"));
                }
                if serde_json::to_value(&old.binding).ok()
                    != serde_json::to_value(&session.binding).ok()
                {
                    self.store
                        .replace_managed(
                            &session,
                            if b.harness == "codex" {
                                Some("app-server")
                            } else {
                                None
                            },
                            now_ms(),
                        )
                        .map_err(storage)?;
                }
            }
            Err(tower_store::Error::NotFound) => {
                let host = self.store.host().map_err(storage)?;
                let token =
                    secret().map_err(|_| app("internal_error", "Random source unavailable"))?;
                let path = self
                    .paths
                    .token_path(&session.id)
                    .map_err(|_| invalid("Invalid Tower identity"))?;
                tower_pal::write_private(&path, token.0.as_bytes())
                    .map_err(|_| app("internal_error", "Cannot persist session credential"))?;
                let now = now_ms();
                let mut caps = Capabilities::unavailable("structured_control_required");
                caps.kill = true;
                let record = AgentRecord {
                    id: session.id.clone(),
                    display_name: None,
                    node: host.node_name,
                    harness: b.harness.clone(),
                    mode: if b.harness == "codex" {
                        Some("app-server".into())
                    } else {
                        None
                    },
                    ownership: "owned".into(),
                    protocol: Some(b.protocol.clone()),
                    trust: "strong".into(),
                    native_session_id: None,
                    pid: None,
                    pid_start_time: None,
                    shared_process: false,
                    cwd: self
                        .paths
                        .display_cwd(&b.cwd)
                        .map_err(|_| invalid("Invalid directory"))?,
                    status: "starting".into(),
                    status_detail: None,
                    caps,
                    pool_session: None,
                    attach_count: 0,
                    sources: vec!["launcher".into()],
                    registered_at: now,
                    updated_at: now,
                    last_seen: now,
                    revision: 0,
                    labels: BTreeMap::new(),
                    tags: Default::default(),
                    needs_attention: false,
                    task_refs: Vec::new(),
                    sync_state: "pending".into(),
                };
                if let Err(e) = self.store.create_managed(record, &hash(&token), &session) {
                    let _ = tower_pal::remove_private(&path);
                    return Err(storage(e));
                }
            }
            Err(e) => return Err(storage(e)),
        }
        Ok(())
    }
}
