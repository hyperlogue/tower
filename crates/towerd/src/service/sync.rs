use super::*;
use std::sync::atomic::Ordering;

impl Service {
    pub(super) fn policy_ready(&self) -> bool {
        self.connected.load(Ordering::Acquire)
            && self.policy_epoch.load(Ordering::Acquire)
                == self.remote_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn sync_error(&self, key: &Secret, code: &str) -> std::result::Result<(), RpcError> {
        if self.store.host().map_err(storage)?.mode != "allgather"
            || tower_pal::read_private(&self.paths.state.join("allgather.key"))
                .ok()
                .as_deref()
                != Some(key.0.as_bytes())
        {
            return Ok(());
        }
        tower_pal::write_private(
            &self.paths.state.join("allgather-connection.json"),
            &serde_json::to_vec(&serde_json::json!({"code":code,"at":now_ms()}))
                .map_err(|_| app("internal_error", "Cannot encode connection status"))?,
        )
        .map_err(|_| app("internal_error", "Cannot persist connection status"))
    }

    pub(crate) fn sync_cursor(&self) -> std::result::Result<u64, RpcError> {
        Ok(self
            .published
            .lock()
            .map_err(|_| app("internal_error", "Event state unavailable"))?
            .seq)
    }
    pub(crate) fn sync_ack(
        &self,
        ids: &[String],
        rejected: &[(String, String)],
        epoch: u64,
    ) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        let sent: HashSet<_> = ids.iter().collect();
        let mut seen = HashSet::new();
        if rejected.len() > tower_protocol::MAX_SYNC_RECORDS
            || rejected
                .iter()
                .any(|(id, _)| !sent.contains(id) || !seen.insert(id))
        {
            return Err(invalid(
                "Sync rejection must identify distinct records from the acknowledged snapshot",
            ));
        }
        for (id, reason) in rejected {
            if self
                .store
                .agent(id)
                .is_ok_and(|a| a.record.sync_state != "rejected")
            {
                self.store.set_sync_state(id, "rejected").map_err(storage)?;
                self.emit("session.sync_rejected",id,serde_json::json!({"reason":reason.chars().filter(|c|!c.is_control()).take(200).collect::<String>()}))?;
            }
        }
        for id in ids {
            if !rejected.iter().any(|(rejected, _)| rejected == id) && self.store.agent(id).is_ok()
            {
                self.store.set_sync_state(id, "synced").map_err(storage)?;
            }
        }
        self.publish()
    }

    pub(crate) fn check_remote(&self, epoch: u64) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch).map(|_| ())
    }
    pub(super) fn remote_actor(&self, epoch: u64) -> std::result::Result<Actor, RpcError> {
        if !self.connected.load(Ordering::Acquire)
            || self.remote_epoch.load(Ordering::Acquire) != epoch
            || self.store.host().map_err(storage)?.mode != "allgather"
            || tower_pal::read_private(&self.paths.state.join("allgather.key")).is_err()
        {
            return Err(app("unauthorized", "Allgather connection is not enrolled"));
        }
        Ok(Actor::Allgather {
            scope: self.remote_scopes()?,
        })
    }
    pub(super) fn remote_scopes(&self) -> std::result::Result<Vec<String>, RpcError> {
        let consent = tower_pal::read_private(&self.paths.state.join("allgather-consent.json"))
            .ok()
            .and_then(|bytes| {
                serde_json::from_slice::<crate::allgather::enrollment::Consent>(&bytes).ok()
            });
        if let Some(consent) = consent {
            let bound = tower_pal::read_private(&self.paths.state.join("allgather-server"))
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .and_then(|url| crate::allgather::enrollment::origin(&url).ok());
            let host = self.store.host().map_err(storage)?;
            if bound.as_deref() == Some(&consent.origin) && host.node_name == consent.machine_id {
                return Ok(consent
                    .scopes
                    .into_iter()
                    .filter(|scope| self.allgather_config.permits_scope(scope))
                    .collect());
            }
        }
        Ok(self
            .allgather_config
            .scopes
            .clone()
            .unwrap_or_else(|| vec!["read".into(), "send".into()]))
    }
    pub(crate) fn remote_dispatch(
        &self,
        request: &Request,
        epoch: u64,
    ) -> std::result::Result<Value, RpcError> {
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        let actor = self.remote_actor(epoch)?;
        if matches!(
            request.method.as_str(),
            "command.execute" | "command.status"
        ) {
            return self.execution_dispatch(request, epoch);
        }
        if request.method != "daemon.info" {
            return Err(RpcError::protocol(
                -32601,
                "method_not_found",
                "Method is not part of the Allgather interface",
            ));
        }
        let result = self.dispatch_actor(
            request,
            actor,
            Peer {
                uid: tower_pal::uid(),
                pid: None,
            },
        );
        if result.is_ok() {
            self.publish()?;
        }
        result
    }
    pub(crate) fn sync_activate(
        &self,
        key: &Secret,
        node: &str,
    ) -> std::result::Result<u64, RpcError> {
        let _guard = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        if tower_pal::read_private(&self.paths.state.join("allgather.key"))
            .ok()
            .as_deref()
            != Some(key.0.as_bytes())
        {
            return Err(app("unauthorized", "Enrollment changed during handshake"));
        }
        self.sync_node(node)?;
        self.sync_connected(true)
    }
    pub(crate) fn sync_connected(&self, connected: bool) -> std::result::Result<u64, RpcError> {
        if connected {
            tower_pal::remove_private(&self.paths.state.join("allgather-connection.json"))
                .map_err(|_| app("internal_error", "Cannot clear connection status"))?;
        }
        let epoch = self
            .remote_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        if self.connected.swap(connected, Ordering::AcqRel) != connected {
            self.emit(
                if connected {
                    "allgather.connected"
                } else {
                    "allgather.disconnected"
                },
                "allgather",
                serde_json::json!({}),
            )?;
        }
        Ok(epoch)
    }
    pub(crate) fn sync_hello(&self) -> std::result::Result<Option<AllgatherHello>, RpcError> {
        let host = self.store.host().map_err(storage)?;
        if host.mode != "allgather" {
            return Ok(None);
        }
        let harness_controls = if self.allgather_config.managed {
            self.backends
                .execution()
                .map(|h| h.controls())
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let acp_harnesses = harness_controls.keys().cloned().collect();
        let mut capabilities = vec![
            "policy-sync.v1".into(),
            "session-status.v1".into(),
            "session-status-delta.v1".into(),
            "policy-delta.v1".into(),
            "repository-catalog.v1".into(),
            "repository-catalog-delta.v1".into(),
            "checkout-sync.v1".into(),
        ];
        if !harness_controls.is_empty() {
            capabilities.extend(
                [
                    "managed-acp.v1",
                    "command-receipts.v1",
                    "session-stream.v1",
                    "session-resume.v1",
                ]
                .map(str::to_owned),
            );
        }
        Ok(Some(AllgatherHello {
            protocol_version: PROTOCOL_VERSION,
            tower_version: env!("CARGO_PKG_VERSION").into(),
            host_id: host.host_id,
            policy_revision: host.policy_revision,
            capabilities,
            acp_harnesses,
            harness_controls,
        }))
    }
    pub(crate) fn sync_node(&self, node: &str) -> std::result::Result<(), RpcError> {
        if !tower_sessions::valid_protocol(node) {
            return Err(invalid(
                "Node names require 1 to 64 ASCII letters, digits, underscores, or hyphens",
            ));
        }
        if self.store.host().map_err(storage)?.mode != "allgather" {
            return Err(app("unauthorized", "Not enrolled"));
        }
        self.store
            .set_enrollment("allgather", Some(node))
            .map_err(storage)
    }
    pub(crate) fn sync_policy(
        &self,
        p: PolicyPush,
        epoch: u64,
    ) -> std::result::Result<PolicyAck, RpcError> {
        self.sync_policy_pending(epoch)?;
        let previous = self.store.rules().map_err(storage)?;
        let result = (|| {
            if p.policy_id != self.allgather_config.policy_id {
                return Err(invalid("Policy namespace does not match this enrollment"));
            }
            if p.rules.len() > 1024 {
                return Err(invalid("Policy exceeds the rule limit"));
            }
            let mut ids = HashSet::new();
            for rule in &p.rules {
                validate_rule(rule)?;
                if !ids.insert(&rule.id) {
                    return Err(invalid("Duplicate rule ID"));
                }
            }
            self.store
                .apply_policy(p.revision, &p.rules)
                .map_err(storage)
        })();
        self.policy_report(p.policy_id, p.revision, previous, result, epoch)
    }
    pub(crate) fn sync_policy_delta(
        &self,
        p: PolicyDelta,
        epoch: u64,
    ) -> std::result::Result<PolicyAck, RpcError> {
        self.sync_policy_pending(epoch)?;
        let previous = self.store.rules().map_err(storage)?;
        let result = (|| {
            if p.policy_id != self.allgather_config.policy_id {
                return Err(invalid("Policy namespace does not match this enrollment"));
            }
            for rule in &p.upserts {
                validate_rule(rule)?;
            }
            self.store
                .apply_policy_delta(&p)
                .map_err(|error| match error {
                    tower_store::Error::Conflict("policy delta base revision mismatch") => {
                        app("policy_base_mismatch", "A full policy snapshot is required")
                    }
                    other => storage(other),
                })
        })();
        self.policy_report(p.policy_id, p.revision, previous, result, epoch)
    }
    pub(crate) fn sync_policy_pending(&self, epoch: u64) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        self.policy_epoch.store(0, Ordering::Release);
        Ok(())
    }
    fn policy_report(
        &self,
        policy_id: String,
        revision: u64,
        previous: RulesView,
        result: std::result::Result<bool, RpcError>,
        epoch: u64,
    ) -> std::result::Result<PolicyAck, RpcError> {
        let old = previous.policy_revision;
        let rules = if result.is_ok() {
            self.store.rules().map_err(storage)?.allgather_rules
        } else {
            Vec::new()
        };
        let mut changed = Vec::new();
        if result.is_ok() {
            for r in &rules {
                if !previous.allgather_rules.contains(r) {
                    changed.push(r.id.clone());
                }
            }
            for r in &previous.allgather_rules {
                if !rules.iter().any(|next| next.id == r.id) {
                    changed.push(r.id.clone());
                }
            }
        }
        if result.is_ok() {
            self.policy_epoch.store(epoch, Ordering::Release);
            self.refresh_approvals()?;
        }
        let ack = PolicyAck {
            received_at: now_ms(),
            policy_id,
            previous_revision: old,
            applied_revision: if result.is_ok() { Some(revision) } else { old },
            changed_rule_ids: changed,
            outcome: if result.is_ok() {
                "applied"
            } else {
                "rejected"
            }
            .into(),
            reason: result.err().map(|e| e.data.code),
        };
        tower_pal::write_private(
            &self.paths.state.join("allgather-policy-report.json"),
            &serde_json::to_vec(&ack)
                .map_err(|_| app("internal_error", "Cannot encode policy report"))?,
        )
        .map_err(|_| app("internal_error", "Cannot persist policy report"))?;
        self.emit(
            if ack.outcome == "applied" {
                "sync.policy_applied"
            } else {
                "sync.policy_rejected"
            },
            "allgather",
            serde_json::to_value(&ack)
                .map_err(|_| app("internal_error", "Cannot encode policy report"))?,
        )?;
        Ok(ack)
    }
    pub(crate) fn sync_state(&self) -> std::result::Result<Vec<AgentRecord>, RpcError> {
        let records = self.all()?;
        if records.len() > tower_protocol::MAX_SYNC_RECORDS {
            return Err(app(
                "sync_record_limit",
                "Full status projection exceeds 5,000 records; no records were sent",
            ));
        }
        Ok(records)
    }
    pub(super) fn enrollment(
        &self,
        request: &Request,
        actor: &Actor,
    ) -> std::result::Result<Value, RpcError> {
        if request.method == "allgather.status" {
            require(actor, "read")?;
            let host = self.store.host().map_err(storage)?;
            let report =
                tower_pal::read_private(&self.paths.state.join("allgather-policy-report.json"))
                    .ok()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
            return value(
                serde_json::json!({"mode":host.mode,"connected":self.connected.load(Ordering::Relaxed),"policy_revision":host.policy_revision,"policy_stale":host.mode=="allgather" && !self.policy_ready(),"last_application":report,"connection_error":tower_pal::read_private(&self.paths.state.join("allgather-connection.json")).ok().and_then(|b|serde_json::from_slice::<serde_json::Value>(&b).ok())}),
            );
        }
        require(actor, "admin")?;
        if request.method == "allgather.logout" {
            self.sync_logout()?;
            return value(serde_json::json!({"mode":"standalone"}));
        }
        #[derive(Deserialize)]
        struct Login {
            key: Secret,
            url: Option<String>,
        }
        let p: Login = params(&request.params)?;
        let url = p.url.unwrap_or_else(|| self.allgather_config.url.clone());
        self.save_enrollment(&p.key, &url)?;
        self.pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?
            .clear();
        tower_pal::remove_private(&self.paths.state.join("allgather-enrollment.json"))
            .map_err(|_| app("internal_error", "Cannot clear pending enrollment"))?;
        value(
            serde_json::json!({"mode":"allgather","connected":false,"policy_pending":!self.policy_ready()}),
        )
    }
    pub(super) fn check_enrollment_origin(&self, url: &str) -> std::result::Result<(), RpcError> {
        let mut config = self.allgather_config.clone();
        config.url = url.into();
        config
            .validate()
            .map_err(|_| invalid("Use a secure Allgather endpoint"))?;
        if let Ok(previous) = tower_pal::read_private(&self.paths.state.join("allgather-server"))
            && previous != url.as_bytes()
        {
            return Err(app(
                "conflict",
                "Log out before changing the enrolled Allgather server",
            ));
        }
        Ok(())
    }
    pub(super) fn save_enrollment(
        &self,
        key: &Secret,
        url: &str,
    ) -> std::result::Result<(), RpcError> {
        self.check_enrollment_origin(url)?;
        if key.0.is_empty() || key.0.len() > 4096 || key.0.chars().any(char::is_control) {
            return Err(invalid("Invalid machine credential"));
        }
        let standalone = self.store.host().map_err(storage)?.mode != "allgather";
        self.sync_connected(false)?;
        self.store
            .set_enrollment("allgather", None)
            .map_err(storage)?;
        if standalone {
            self.store.clear_policy().map_err(storage)?;
        }
        tower_pal::write_private(&self.paths.state.join("allgather-server"), url.as_bytes())
            .map_err(|_| app("internal_error", "Cannot bind enrollment to server"))?;
        tower_pal::write_private(&self.paths.state.join("allgather.key"), key.0.as_bytes())
            .map_err(|_| app("internal_error", "Cannot store machine credential"))?;
        Ok(())
    }
    pub(crate) fn sync_logout(&self) -> std::result::Result<(), RpcError> {
        self.pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?
            .clear();
        tower_pal::remove_private(&self.paths.state.join("allgather-enrollment.json"))
            .map_err(|_| app("internal_error", "Cannot clear pending enrollment"))?;
        tower_pal::remove_private(&self.paths.state.join("allgather.key"))
            .map_err(|_| app("internal_error", "Cannot remove access key"))?;
        self.sync_connected(false)?;
        tower_pal::remove_private(&self.paths.state.join("allgather-connection.json"))
            .map_err(|_| app("internal_error", "Cannot clear connection status"))?;
        self.store
            .set_enrollment("standalone", None)
            .map_err(storage)?;
        self.store.clear_policy().map_err(storage)?;
        self.store.catalog_pairing("").map_err(storage)?;
        self.refresh_approvals()?;
        tower_pal::remove_private(&self.paths.state.join("allgather-server"))
            .map_err(|_| app("internal_error", "Cannot remove enrollment binding"))?;
        Ok(())
    }
}
