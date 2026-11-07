use super::*;
use tower_sessions::{
    ManagedSession,
    execution::{ExecutionHost, RuntimeBinding, RuntimeUpdates, RuntimeView},
};
impl Service {
    fn execution_host(&self) -> std::result::Result<&dyn ExecutionHost, RpcError> {
        self.backends
            .execution()
            .map(|e| &**e)
            .ok_or_else(|| app("unsupported", "Managed runtimes are unavailable"))
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
    /// Policy check the runtime makes just before dispatching a command.
    pub(crate) fn authorize_execution(
        &self,
        p: &tower_sessions::execution::Authorization,
    ) -> std::result::Result<(), RpcError> {
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        if p.command_id.is_empty() {
            return Err(invalid("Command identity required"));
        }
        let session = self
            .store
            .managed_session(&p.binding.tower_session_id)
            .map_err(storage)?;
        let stored = RuntimeBinding::from_session(&session)?
            .ok_or_else(|| app("runtime_conflict", "Session has no execution owner"))?;
        if serde_json::to_value(&stored).ok() != serde_json::to_value(&p.binding).ok() {
            return Err(app(
                "runtime_fenced",
                "Runtime binding is no longer current",
            ));
        }
        let cleanup = stored.protocol != "terminal"
            && p.cleanup
            && matches!(p.kind.as_str(), "cancel" | "reply");
        if cleanup {
            return Ok(());
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
            require(&actor, stored.scope())?;
            if !self.policy_ready() {
                return Err(app(
                    "control_unavailable",
                    "A current control connection and applied policy are required",
                ));
            }
            actor
        };
        if let Some(target) = &p.target {
            if target.namespace() != stored.namespace()
                || target.session_id != stored.session_id
                || target.tower_session_id != stored.tower_session_id
                || target.protocol != stored.protocol
                || target.harness != stored.harness
            {
                return Err(app(
                    "cleanup_target_conflict",
                    "Cleanup target does not belong to this execution",
                ));
            }
            let cwd = self
                .paths
                .display_cwd(&target.cwd)
                .map_err(|_| invalid("Invalid cleanup directory"))?;
            self.session_policy(
                &actor,
                crate::policy::Subject {
                    id: &target.tower_session_id,
                    harness: Some(&target.harness),
                    cwd: Some(&cwd),
                },
            )?;
        }
        let cwd = self
            .paths
            .display_cwd(&stored.cwd)
            .map_err(|_| invalid("Invalid execution directory"))?;
        self.session_policy(
            &actor,
            crate::policy::Subject {
                id: &stored.tower_session_id,
                harness: Some(&stored.harness),
                cwd: Some(&cwd),
            },
        )
    }
    pub(super) fn execution_dispatch(
        &self,
        request: &Request,
        epoch: u64,
        protocols: crate::allgather::ExecutionProtocols,
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
            if let Some(view) = host
                .list(Some(&namespace))?
                .iter()
                .find(|v| v.binding.runtime_id == p.runtime_id)
                && !protocols.allows(&view.binding.protocol)
            {
                return Err(app(
                    "unsupported_control",
                    "Runtime protocol was not negotiated",
                ));
            }
            return value(host.status(&namespace, &p)?);
        }
        let c: ExecutionCommand = params(&request.params)?;
        let force = tower_sessions::execution::force_resume(&c)?;
        if force.is_some() && !protocols.force_resume {
            return Err(app(
                "unsupported_control",
                "Force resume was not negotiated",
            ));
        }
        host.status(
            &namespace,
            &ExecutionStatus {
                command_id: c.command_id.clone(),
                runtime_id: c.runtime_id.clone(),
                generation: c.generation.clone(),
            },
        )?;
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
                if matches!(s.protocol.as_str(), "acp" | "codex-app-server" | "terminal")
                    && let Some(b) = RuntimeBinding::from_session(&s)?
                    && b.namespace() == namespace
                    && b.runtime_id == c.runtime_id
                {
                    binding = Some(b);
                    break;
                }
            }
        }
        let terminal = binding
            .as_ref()
            .map_or(payload["protocol"] == "terminal", |b| {
                b.protocol == "terminal"
            });
        if !protocols.allows(if terminal { "terminal" } else { "acp" }) {
            return Err(app(
                "unsupported_control",
                "Runtime protocol was not negotiated",
            ));
        }
        require(
            &actor,
            if terminal {
                "terminal:control"
            } else {
                "acp:control"
            },
        )?;
        if terminal {
            validate_terminal_command(&c)?;
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
            let controls = if protocol == "terminal" {
                self.terminal_controls()
            } else {
                host.controls()
            };
            if controls
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
            self.vcs_directory_available(std::path::Path::new(&cwd))?;
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
                if protocol == "codex-app-server" && metadata["native_resumable"] != true {
                    return Err(app(
                        "resume_unavailable",
                        "Codex has not confirmed resumable conversation history; the old execution was not stopped",
                    ));
                }
                if previous.binding.harness != harness
                    || previous.binding.protocol != protocol
                    || (force.is_none()
                        && (!previous.fenced || previous.termination != "confirmed"))
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
                        "Stop the previous execution or use force resume; native history must match this session",
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
            let current = RuntimeBinding::from_session(&current)?
                .ok_or_else(|| app("runtime_conflict", "Session has no execution owner"))?;
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
        if new_generation {
            host.validate_new(&b, &c)?;
            if let Some(targets) = &force {
                for target in targets {
                    let old = views
                        .iter()
                        .find(|v| {
                            v.binding.runtime_id == target.runtime_id
                                && v.binding.generation == target.generation
                        })
                        .ok_or_else(|| {
                            app("cleanup_target_conflict", "Cleanup target unavailable")
                        })?;
                    let cwd = self
                        .paths
                        .display_cwd(&old.binding.cwd)
                        .map_err(|_| invalid("Invalid cleanup directory"))?;
                    self.session_policy(
                        &actor,
                        crate::policy::Subject {
                            id: &old.binding.tower_session_id,
                            harness: Some(&old.binding.harness),
                            cwd: Some(&cwd),
                        },
                    )?;
                }
            }
        }
        let previous = if new_generation && force.is_some() {
            Some(
                self.store
                    .managed_session(&b.tower_session_id)
                    .map_err(storage)?,
            )
        } else {
            None
        };
        self.reserve_execution(&b)?;
        let receipt = match host.execute(&b, &c) {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Some(previous) = previous
                    && host
                        .status(
                            &namespace,
                            &ExecutionStatus {
                                command_id: c.command_id.clone(),
                                runtime_id: c.runtime_id.clone(),
                                generation: c.generation.clone(),
                            },
                        )
                        .is_ok_and(|r| r.state == "missing")
                {
                    self.store
                        .replace_managed(
                            &previous,
                            if b.harness == "codex" {
                                Some("app-server")
                            } else {
                                None
                            },
                            now_ms(),
                        )
                        .map_err(storage)?;
                }
                return Err(error);
            }
        };
        self.publish()?;
        value(receipt)
    }
    fn reserve_execution(&self, b: &RuntimeBinding) -> std::result::Result<(), RpcError> {
        let session = ManagedSession {
            id: b.tower_session_id.clone(),
            protocol: b.protocol.clone(),
            binding: if b.protocol == "terminal" {
                value(serde_json::json!({"execution":b}))?
            } else {
                value(b)?
            },
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
                    checkout_id: None,
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
        self.vcs.link_session(&session.id)?;
        Ok(())
    }
}

fn validate_terminal_command(c: &ExecutionCommand) -> std::result::Result<(), RpcError> {
    let p: serde_json::Value =
        serde_json::to_value(&c.payload).map_err(|_| invalid("Invalid terminal payload"))?;
    match c.kind.as_str() {
        "launch"
            if p.as_object().is_some_and(|p| p.len() == 3)
                && p["protocol"] == "terminal"
                && p["harness"].is_string()
                && p["cwd"].is_string() =>
        {
            Ok(())
        }
        "cancel" if p.as_object().is_some_and(|p| p.is_empty()) => Ok(()),
        "launch" | "cancel" => Err(invalid("Invalid terminal command payload")),
        _ => Err(app(
            "unsupported_control",
            "Terminal runtimes support only launch and whole-runtime stop",
        )),
    }
}
