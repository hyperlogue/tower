use super::*;
use tower_sessions::ManagedSession;

impl Service {
    fn session_token(&self, id: &str) -> std::result::Result<Secret, RpcError> {
        let path = self
            .paths
            .token_path(id)
            .map_err(|_| app("internal_error", "Invalid token path"))?;
        Ok(Secret(
            String::from_utf8(
                tower_pal::read_private(&path)
                    .map_err(|_| app("unauthorized", "Session credential unavailable"))?,
            )
            .map_err(|_| app("unauthorized", "Invalid session credential"))?,
        ))
    }
    pub(super) fn managed_restart(
        &self,
        p: SessionRestartParams,
        restart: bool,
    ) -> std::result::Result<Value, RpcError> {
        self.sync_managed().map_err(storage)?;
        let record = self.resolve(&p.id)?.record;
        if record.ownership != "owned" {
            return Err(app("not_owned", "Session lifecycle is externally managed"));
        }
        if !record.caps.resume {
            return Err(app(
                "resume_unavailable",
                "This session does not currently have a recoverable conversation",
            ));
        }
        let active = !matches!(record.status.as_str(), "exited" | "lost");
        if !restart && active {
            return Err(app("already_running", "Session is already running"));
        }
        if restart && active && !record.caps.kill {
            return Err(app(
                "unsupported",
                "This session cannot currently be stopped",
            ));
        }
        let old = self.store.managed_session(&record.id).map_err(storage)?;
        let backend = self.backends.select(Some(&old.protocol))?;
        if p.args.is_some() && !backend.features().contains(&"sessions.restart-options.v1") {
            return Err(app(
                "unsupported",
                "This backend cannot replace resume arguments",
            ));
        }
        let prepared = backend.prepare_resume(&old, p.mode.as_deref(), p.args.as_deref())?;
        if active {
            self.store
                .disable_managed_recovery(&record.id)
                .map_err(storage)?;
            let stopped = backend.stop(&old)?;
            if stopped.id != old.id || stopped.status != "exited" {
                return Err(app(
                    "stop_unconfirmed",
                    "Backend did not confirm termination",
                ));
            }
            self.store
                .observe_managed(&old.protocol, stopped, now_ms())
                .map_err(storage)?;
        }
        let next = ManagedSession {
            id: old.id.clone(),
            protocol: old.protocol,
            binding: prepared.binding,
            launch: Some(prepared.launch),
            auto_resume: true,
        };
        self.store
            .replace_managed(&next, prepared.mode.as_deref(), now_ms())
            .map_err(storage)?;
        let observation = backend.start(&next, &self.session_token(&next.id)?)?;
        self.store
            .observe_managed(backend.protocol(), observation, now_ms())
            .map_err(storage)?;
        let record = self.store.agent(&next.id).map_err(storage)?.record;
        value(SessionOpenResult {
            id: next.id,
            mode: record.mode,
            protocol: record.protocol,
            caps: Some(record.caps),
        })
    }
    pub(super) fn sync_managed(&self) -> tower_store::Result<()> {
        let sessions = self.store.managed_sessions()?;
        for backend in self.backends.iter() {
            let selected: Vec<_> = sessions
                .iter()
                .filter(|s| s.protocol == backend.protocol())
                .cloned()
                .collect();
            if selected.is_empty() {
                continue;
            }
            let Ok(observations) = backend.observe(&selected) else {
                continue;
            };
            for mut observation in observations {
                let Some(session) = selected.iter().find(|s| s.id == observation.id) else {
                    continue;
                };
                if observation.status == "lost" && session.auto_resume {
                    let record = self.store.agent(&session.id)?.record;
                    if self
                        .session_policy(
                            &Actor::Human,
                            crate::policy::Subject {
                                id: &record.id,
                                harness: Some(&record.harness),
                                cwd: Some(&record.cwd),
                            },
                        )
                        .is_err()
                    {
                        observation.status = "unknown".into();
                        observation.recovery_pending = true;
                        observation.metadata.detail = Some(
                            value(serde_json::json!({"error":"recovery_policy_required"}))
                                .map_err(|_| tower_store::Error::Schema)?,
                        );
                        self.store
                            .observe_managed(backend.protocol(), observation, now_ms())?;
                        continue;
                    }
                    match backend.recover(session) {
                        Ok(Some(prepared)) => {
                            let replacement = ManagedSession {
                                id: session.id.clone(),
                                protocol: session.protocol.clone(),
                                binding: prepared.binding,
                                launch: Some(prepared.launch),
                                auto_resume: true,
                            };
                            self.store.replace_managed(
                                &replacement,
                                prepared.mode.as_deref(),
                                now_ms(),
                            )?;
                            let Ok(token) = self.session_token(&session.id) else {
                                continue;
                            };
                            let Ok(started) = backend.start(&replacement, &token) else {
                                continue;
                            };
                            observation = started;
                        }
                        Err(_) => continue,
                        Ok(None) => {}
                    }
                }
                self.store
                    .observe_managed(backend.protocol(), observation, now_ms())?;
            }
        }
        Ok(())
    }
    pub(super) fn managed_open(
        &self,
        mut p: SessionOpenParams,
    ) -> std::result::Result<Value, RpcError> {
        if p.protocol
            .as_deref()
            .is_some_and(|p| !tower_sessions::valid_protocol(p))
        {
            return Err(invalid("Invalid session protocol"));
        }
        let backend = self.backends.select(p.protocol.as_deref())?;
        if p.resume.is_some() && !backend.features().contains(&"sessions.conversations.v1") {
            return Err(app(
                "unsupported",
                "This backend cannot import native conversations",
            ));
        }
        p.cwd = self
            .paths
            .absolute_cwd(&p.cwd)
            .map_err(|_| invalid("Invalid working directory"))?
            .to_string_lossy()
            .into_owned();
        let host = self.store.host().map_err(storage)?;
        let mut unique = None;
        for _ in 0..8 {
            let id = format!(
                "{}-{}",
                host.node_name,
                base32(
                    &random(10).map_err(|_| app("internal_error", "Random source unavailable"))?
                )
            );
            self.paths.token_path(&id).map_err(|_| {
                app(
                    "invalid_node_name",
                    "Node name cannot form a safe session identity",
                )
            })?;
            match self.store.agent(&id) {
                Err(tower_store::Error::NotFound) => {
                    unique = Some(id);
                    break;
                }
                Ok(_) => {}
                Err(e) => return Err(storage(e)),
            }
        }
        let id = unique.ok_or_else(|| {
            app(
                "internal_error",
                "Cannot allocate a unique session identity",
            )
        })?;
        let prepared = backend.prepare(&id, &p)?;
        if let Some(native) = &prepared.native_session_id {
            match self.store.agent_by_native_id(&prepared.harness, native) {
                Ok(existing) => {
                    let _ = backend.cleanup(&ManagedSession {
                        id: id.clone(),
                        protocol: backend.protocol().into(),
                        binding: prepared.binding,
                        launch: None,
                        auto_resume: false,
                    });
                    let mut error = app(
                        "already_managed",
                        "This conversation is already registered; attach or resume its existing Tower ID",
                    );
                    error
                        .data
                        .details
                        .insert("id".into(), Value::String(existing.record.id));
                    return Err(error);
                }
                Err(tower_store::Error::NotFound) => {}
                Err(e) => return Err(storage(e)),
            }
        }
        let token = secret().map_err(|_| app("internal_error", "Random source unavailable"))?;
        let path = self
            .paths
            .token_path(&id)
            .map_err(|_| app("internal_error", "Invalid token path"))?;
        tower_pal::write_private(&path, token.0.as_bytes())
            .map_err(|_| app("internal_error", "Cannot write session credential"))?;
        let now = now_ms();
        let record = AgentRecord {
            id: id.clone(),
            display_name: None,
            node: host.node_name,
            harness: prepared.harness,
            mode: prepared.mode,
            ownership: "owned".into(),
            protocol: Some(backend.protocol().into()),
            trust: "strong".into(),
            native_session_id: prepared.native_session_id,
            pid: None,
            pid_start_time: None,
            shared_process: false,
            cwd: self
                .paths
                .display_cwd(&prepared.cwd)
                .map_err(|_| invalid("Invalid working directory"))?,
            status: "starting".into(),
            status_detail: None,
            caps: prepared.caps,
            pool_session: prepared.terminal.as_ref().map(|t| t.pool_session.clone()),
            attach_count: prepared.terminal.as_ref().map_or(0, |t| t.attach_count),
            sources: vec!["launcher".into()],
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
        let session = ManagedSession {
            id: id.clone(),
            protocol: backend.protocol().into(),
            binding: prepared.binding,
            launch: Some(prepared.launch),
            auto_resume: true,
        };
        if let Err(error) = self.store.create_managed(record, &hash(&token), &session) {
            let _ = tower_pal::remove_private(&path);
            return Err(storage(error));
        }
        let observation = match backend.start(&session, &token) {
            Ok(observation) if observation.id == id => observation,
            result => {
                self.sync_managed().map_err(storage)?;
                let mut error = result.err().unwrap_or_else(|| {
                    app(
                        "backend_error",
                        "Backend returned a different session identity",
                    )
                });
                error.data.details.insert("id".into(), Value::String(id));
                return Err(error);
            }
        };
        self.store
            .observe_managed(backend.protocol(), observation, now_ms())
            .map_err(storage)?;
        let record = self.store.agent(&id).map_err(storage)?.record;
        value(SessionOpenResult {
            id,
            mode: record.mode,
            protocol: record.protocol,
            caps: Some(record.caps),
        })
    }
    pub(super) fn managed_kill(&self, id: &str) -> std::result::Result<Value, RpcError> {
        self.sync_managed().map_err(storage)?;
        let record = self.resolve(id)?.record;
        if record.ownership != "owned" {
            return Err(app("not_owned", "Session lifecycle is externally managed"));
        }
        if !record.caps.kill {
            return Err(app(
                "unsupported",
                "This session does not currently support stopping",
            ));
        }
        let session = self.store.managed_session(&record.id).map_err(storage)?;
        let backend = self.backends.select(Some(&session.protocol))?;
        self.store
            .disable_managed_recovery(&record.id)
            .map_err(storage)?;
        let observation = backend.stop(&session)?;
        if observation.id != session.id || observation.status != "exited" {
            return Err(app(
                "stop_unconfirmed",
                "Backend did not confirm session termination",
            ));
        }
        self.store
            .disable_managed_recovery(&record.id)
            .map_err(storage)?;
        self.store
            .observe_managed(backend.protocol(), observation, now_ms())
            .map_err(storage)?;
        value(serde_json::json!({"stopped":true}))
    }
}
