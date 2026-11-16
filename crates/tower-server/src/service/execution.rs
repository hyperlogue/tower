use super::*;
use tower_sessions::{
    ManagedSession,
    execution::{ExecutionHost, RuntimeBinding, RuntimeUpdates, RuntimeView},
};

pub(crate) struct AcpConnection {
    host: Arc<dyn ExecutionHost>,
    binding: RuntimeBinding,
}
impl Drop for AcpConnection {
    fn drop(&mut self) {
        let host = self.host.clone();
        let binding = self.binding.clone();
        // Cleanup must survive socket errors and revoked caller credentials.
        tokio::task::spawn_blocking(move || {
            if host.stop(&binding).is_err() {
                eprintln!("tower: disconnected ACP process termination was not confirmed");
            }
        });
    }
}

impl Service {
    pub(crate) fn connect_acp(
        &self,
        mut request: Request,
        peer: Peer,
    ) -> std::result::Result<(Value, AcpConnection), RpcError> {
        request.method = "acp.open".into();
        let result = self.dispatch(&request, peer)?;
        let opened = serde_json::to_value(&result).map_err(|_| invalid("Invalid ACP binding"))?;
        let session = self
            .store
            .managed_session(
                opened["tower_session_id"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing ACP identity"))?,
            )
            .map_err(storage)?;
        let binding = RuntimeBinding::from_session(&session)?
            .ok_or_else(|| invalid("Missing ACP binding"))?;
        let host = self
            .backends
            .execution()
            .cloned()
            .ok_or_else(|| app("unsupported", "ACP proxy unavailable"))?;
        Ok((result, AcpConnection { host, binding }))
    }

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
    ) -> std::result::Result<RuntimeUpdates, RpcError> {
        self.remote_actor(epoch)?;
        self.execution_host()?.updates(&self.execution_namespace()?)
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
        self.execution_dispatch_inner(request, epoch, protocols, false)
    }
    fn execution_dispatch_inner(
        &self,
        request: &Request,
        epoch: u64,
        protocols: crate::allgather::ExecutionProtocols,
        proxy: bool,
    ) -> std::result::Result<Value, RpcError> {
        if !self.allgather_config.managed {
            return Err(app("unsupported", "Managed execution is disabled"));
        }
        let actor = self.remote_actor(epoch)?;
        let host = self.execution_host()?;
        let namespace = self.execution_namespace()?;
        let views = host.list(Some(&namespace))?;
        if request.method == "command.status" {
            require(&actor, "read")?;
            let p: ExecutionStatus = params(&request.params)?;
            if views
                .iter()
                .any(|v| v.binding.runtime_id == p.runtime_id && v.binding.protocol != "terminal")
            {
                return Err(app(
                    "acp_required",
                    "Managed commands were removed; use ACP",
                ));
            }
            return value(host.status(&namespace, &p)?);
        }
        let c: ExecutionCommand = params(&request.params)?;
        let payload: serde_json::Value =
            serde_json::to_value(&c.payload).map_err(|_| invalid("Invalid command payload"))?;
        let binding = views
            .iter()
            .find(|v| v.binding.runtime_id == c.runtime_id)
            .map(|v| v.binding.clone());
        let protocol = binding
            .as_ref()
            .map(|b| b.protocol.as_str())
            .or(payload["protocol"].as_str())
            .unwrap_or("");
        if proxy {
            if protocol != "acp" || c.kind != "launch" {
                return Err(app(
                    "acp_required",
                    "Open an ACP connection and exchange native frames",
                ));
            }
            require(&actor, "acp:control")?;
        } else {
            if protocol != "terminal" {
                return Err(app(
                    "acp_required",
                    "Managed commands were removed; use acp.open and acp.send",
                ));
            }
            require(&actor, "terminal:control")?;
            validate_terminal_command(&c)?;
        }
        if !protocols.allows(protocol) {
            return Err(app(
                "unsupported_control",
                "Runtime protocol was not negotiated",
            ));
        }
        if let Some(b) = &binding {
            if b.generation != c.generation || b.session_id != c.session_id {
                return Err(app("runtime_conflict", "Execution identity changed"));
            }
            let retained = host.status(
                &namespace,
                &ExecutionStatus {
                    command_id: c.command_id.clone(),
                    runtime_id: c.runtime_id.clone(),
                    generation: c.generation.clone(),
                },
            )?;
            if retained.state != "missing" {
                return value(host.execute(b, &c)?);
            }
        }
        let cleanup = !proxy && c.kind == "cancel";
        if !cleanup && !self.policy_ready() {
            return Err(app("denied", "Execution requires current applied policy"));
        }
        let fresh = binding.is_none();
        let b = if let Some(b) = binding {
            b
        } else {
            if c.kind != "launch" {
                return Err(app("not_found", "Execution is unknown"));
            }
            let harness = payload["harness"]
                .as_str()
                .ok_or_else(|| invalid("Harness is required"))?;
            let controls = if proxy {
                host.controls()
            } else {
                self.terminal_controls()
            };
            if controls
                .get(harness)
                .is_none_or(|control| control.protocol != protocol)
            {
                return Err(app("unsupported", "Configured agent is unavailable"));
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
            let id = if let Some(previous) = previous {
                if !proxy
                    || !previous.fenced
                    || previous.termination != "confirmed"
                    || previous.binding.harness != harness
                    || previous.binding.protocol != "acp"
                {
                    return Err(app(
                        "runtime_conflict",
                        "Close the previous connection before replacing it",
                    ));
                }
                previous.binding.tower_session_id.clone()
            } else {
                format!(
                    "{}-{}",
                    self.store.host().map_err(storage)?.node_name,
                    base32(
                        &random(10)
                            .map_err(|_| app("internal_error", "Random source unavailable"))?
                    )
                )
            };
            let (origin, machine_id) =
                serde_json::from_str(&namespace).map_err(|_| invalid("Invalid namespace"))?;
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
                resume_session_id: None,
            }
        };
        if !cleanup {
            let cwd = self
                .paths
                .display_cwd(&b.cwd)
                .map_err(|_| invalid("Invalid directory"))?;
            self.session_policy(
                &actor,
                crate::policy::Subject {
                    id: &b.tower_session_id,
                    harness: Some(&b.harness),
                    cwd: Some(&cwd),
                },
            )?;
        }
        if fresh {
            host.validate_new(&b, &c)?;
        } else {
            let current = self
                .store
                .managed_session(&b.tower_session_id)
                .map_err(storage)?;
            let current = RuntimeBinding::from_session(&current)?
                .ok_or_else(|| app("runtime_fenced", "Execution has no owner"))?;
            if current.namespace() != b.namespace()
                || current.runtime_id != b.runtime_id
                || current.generation != b.generation
            {
                return Err(app("runtime_fenced", "Execution owner changed"));
            }
        }
        self.reserve_execution(&b)?;
        let receipt = host.execute(&b, &c)?;
        self.publish()?;
        value(receipt)
    }
    pub(super) fn local_acp_dispatch(
        &self,
        request: &Request,
        actor: Actor,
        peer: Peer,
    ) -> std::result::Result<Value, RpcError> {
        require(&actor, "acp:control")?;
        let mut p: serde_json::Value =
            serde_json::to_value(&request.params).map_err(|_| invalid("Invalid ACP request"))?;
        if request.method == "acp.open" {
            p["protocol"] = serde_json::json!("acp");
            let mut launch = request.clone();
            launch.method = "session.open".into();
            launch.params = value(p)?;
            let opened = self.dispatch_actor(&launch, actor, peer)?;
            let opened: serde_json::Value =
                serde_json::to_value(opened).map_err(|_| invalid("Invalid session"))?;
            let session = self
                .store
                .managed_session(
                    opened["id"]
                        .as_str()
                        .ok_or_else(|| invalid("Missing session identity"))?,
                )
                .map_err(storage)?;
            let b = RuntimeBinding::from_session(&session)?
                .ok_or_else(|| invalid("Missing ACP binding"))?;
            return value(
                serde_json::json!({"runtime_id":b.runtime_id,"generation":b.generation,"tower_session_id":b.tower_session_id,"owner_id":b.owner_id}),
            );
        }
        let namespace =
            serde_json::to_string(&("local", "local")).map_err(|_| invalid("Invalid namespace"))?;
        let host = self.execution_host()?;
        let view = host
            .list(Some(&namespace))?
            .into_iter()
            .find(|v| {
                v.binding.runtime_id == p["runtime_id"] && v.binding.generation == p["generation"]
            })
            .ok_or_else(|| app("not_found", "ACP connection is unavailable"))?;
        let b = &view.binding;
        if b.protocol != "acp" {
            return Err(app("acp_required", "This is not an ACP proxy"));
        }
        let cwd = self
            .paths
            .display_cwd(&b.cwd)
            .map_err(|_| invalid("Invalid directory"))?;
        self.session_policy(
            &actor,
            crate::policy::Subject {
                id: &b.tower_session_id,
                harness: Some(&b.harness),
                cwd: Some(&cwd),
            },
        )?;
        let sequence = p["sequence"].as_u64().unwrap_or(0);
        match request.method.as_str() {
            "acp.send" => {
                host.acp_send(
                    &namespace,
                    &b.runtime_id,
                    &b.generation,
                    sequence,
                    p["frame"]
                        .as_str()
                        .ok_or_else(|| invalid("ACP frame required"))?,
                )?;
                value(serde_json::json!({"sequence":sequence}))
            }
            "acp.ack" => {
                host.acp_ack(&namespace, &b.runtime_id, &b.generation, sequence)?;
                value(serde_json::json!({"sequence":sequence}))
            }
            "acp.poll" => {
                let frames = host
                    .updates(&namespace)?
                    .acp_frames
                    .into_iter()
                    .filter(|f| f.runtime_id == b.runtime_id)
                    .collect::<Vec<_>>();
                value(serde_json::json!({"frames":frames,"closed":view.fenced,"state":view.state}))
            }
            "acp.close" => {
                host.stop(b)?;
                value(serde_json::json!({"closed":true}))
            }
            _ => Err(app("unsupported", "Unknown ACP operation")),
        }
    }
    pub(super) fn acp_dispatch(
        &self,
        request: &Request,
        epoch: u64,
        protocols: crate::allgather::ExecutionProtocols,
    ) -> std::result::Result<Value, RpcError> {
        let actor = self.remote_actor(epoch)?;
        require(&actor, "acp:control")?;
        if !protocols.acp || !self.allgather_config.managed {
            return Err(app("acp_required", "Negotiate acp-proxy.v1"));
        }
        let p: serde_json::Value =
            serde_json::to_value(&request.params).map_err(|_| invalid("Invalid ACP request"))?;
        let namespace = self.execution_namespace()?;
        let host = self.execution_host()?;
        if request.method == "acp.open" {
            let mut launch = request.clone();
            launch.method = "command.execute".into();
            launch.params = value(serde_json::json!({
                "command_id":p["command_id"],"session_id":p["session_id"],"runtime_id":p["runtime_id"],
                "generation":p["generation"],"kind":"launch","expires_at":p["expires_at"],
                "payload":{"harness":p["harness"],"protocol":"acp","cwd":p["cwd"],"generation":p["generation"]}
            }))?;
            self.execution_dispatch_inner(&launch, epoch, protocols, true)?;
        }
        let b = host
            .list(Some(&namespace))?
            .into_iter()
            .find(|v| {
                v.binding.runtime_id == p["runtime_id"] && v.binding.generation == p["generation"]
            })
            .map(|v| v.binding)
            .ok_or_else(|| app("not_found", "ACP connection is unavailable"))?;
        if b.protocol != "acp" {
            return Err(app("acp_required", "This is not an ACP proxy connection"));
        }
        let current = self
            .store
            .managed_session(&b.tower_session_id)
            .map_err(storage)?;
        let current = RuntimeBinding::from_session(&current)?
            .ok_or_else(|| app("runtime_fenced", "ACP owner changed"))?;
        if request.method != "acp.close"
            && (current.runtime_id != b.runtime_id || current.generation != b.generation)
        {
            return Err(app("runtime_fenced", "ACP owner changed"));
        }
        if request.method != "acp.close" {
            if !self.policy_ready() {
                return Err(app("denied", "ACP forwarding requires current policy"));
            }
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
        match request.method.as_str() {
            "acp.open" => value(
                serde_json::json!({"runtime_id":b.runtime_id,"generation":b.generation,"tower_session_id":b.tower_session_id,"owner_id":b.owner_id}),
            ),
            "acp.send" => {
                let sequence = p["sequence"]
                    .as_u64()
                    .ok_or_else(|| invalid("ACP sequence required"))?;
                host.acp_send(
                    &namespace,
                    &b.runtime_id,
                    &b.generation,
                    sequence,
                    p["frame"]
                        .as_str()
                        .ok_or_else(|| invalid("ACP frame must be a string"))?,
                )?;
                value(serde_json::json!({"sequence":sequence}))
            }
            "acp.close" => {
                host.stop(&b)?;
                value(serde_json::json!({"closed":true}))
            }
            _ => Err(app("unsupported", "Unknown ACP transport operation")),
        }
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
                            if b.protocol == "terminal" && b.harness == "codex" {
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
                let mut caps = Capabilities::unavailable("protocol_control_required");
                caps.kill = true;
                let record = AgentRecord {
                    checkout_id: None,
                    id: session.id.clone(),
                    display_name: None,
                    node: host.node_name,
                    harness: b.harness.clone(),
                    mode: if b.protocol == "terminal" && b.harness == "codex" {
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
