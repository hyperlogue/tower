use super::*;
use crate::allgather::enrollment::{self, Confirmation, Consent, Pending, Plan, Poll, Started};
use std::time::{Duration, Instant};

fn enrollment_storage(_: impl std::fmt::Display) -> RpcError {
    app("internal_error", "Cannot access enrollment state")
}

impl Service {
    fn enrollment_actor(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<String, RpcError> {
        let actor = self.actor(&request.context, peer)?;
        match actor {
            Actor::Token(token) => {
                require(&Actor::Token(token.clone()), "admin")?;
                Ok(format!("token:{}", token.name))
            }
            Actor::Agent {
                record,
                strong: true,
            } => Ok(format!("agent:{}", record.id)),
            Actor::Allgather { .. } => Err(app("denied", "Enrollment is a local operation")),
            _ => Ok(format!("local:{}:{:?}", peer.uid, peer.pid)),
        }
    }
    pub(crate) fn enrollment_plan(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<Plan, RpcError> {
        #[derive(Deserialize)]
        struct Input {
            url: Option<String>,
            node_label: Option<String>,
            #[serde(default)]
            read_only: bool,
        }
        let p: Input = params(&request.params)?;
        let actor = self.enrollment_actor(request, peer)?;
        let origin = enrollment::configured_origin(&self.paths, p.url.as_deref())
            .map_err(|_| invalid("Supply the Allgather HTTP(S) origin"))?;
        self.check_enrollment_origin(&enrollment::socket_url(&origin))?;
        if p.node_label.as_ref().is_some_and(|s| {
            s.is_empty() || s.chars().count() > 160 || s.chars().any(char::is_control)
        }) {
            return Err(invalid("Invalid node label"));
        }
        let scopes = self.allgather_config.enrollment_scopes(p.read_only);
        if scopes.is_empty() {
            return Err(invalid(
                "No enrollment scopes are enabled by the local configuration",
            ));
        }
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        pending.retain(|_, p| p.deadline > Instant::now());
        if pending.len() >= 8 {
            return Err(app("rate_limited", "Too many pending enrollments"));
        }
        Ok(Plan {
            actor,
            origin,
            host_id: self.store.host().map_err(enrollment_storage)?.host_id,
            scopes,
            label: p.node_label,
        })
    }
    pub(crate) fn remember_enrollment(
        &self,
        plan: Plan,
        started: Started,
    ) -> std::result::Result<Value, RpcError> {
        self.check_enrollment_origin(&enrollment::socket_url(&plan.origin))?;
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        if pending.len() >= 8 || pending.contains_key(&started.enrollment_id) {
            return Err(app("rate_limited", "Enrollment cannot be admitted"));
        }
        let result = value(
            serde_json::json!({"id":started.enrollment_id,"authorization_url":started.authorization_url,"expires_at":started.expires_at,"interval":started.interval}),
        )?;
        let now = Instant::now();
        pending.insert(
            started.enrollment_id.clone(),
            Pending {
                plan,
                started,
                deadline: now + Duration::from_secs(600),
                next_poll: now,
                result: None,
            },
        );
        Ok(result)
    }
    pub(crate) fn claim_enrollment_poll(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<std::result::Result<Pending, Value>, RpcError> {
        let id: IdParams = params(&request.params)?;
        let actor = self.enrollment_actor(request, peer)?;
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        let p = pending.get_mut(&id.id).ok_or_else(|| {
            app(
                "not_found",
                "Enrollment expired or daemon restarted; start login again",
            )
        })?;
        if p.plan.actor != actor {
            return Err(app("denied", "Enrollment belongs to another local caller"));
        }
        if p.deadline <= Instant::now() {
            return Err(app("expired", "Enrollment expired; start login again"));
        }
        if let Some(result) = p.result.as_mut() {
            if result["state"] == "authorized"
                && tower_pal::read_private(&self.paths.state.join("allgather-enrollment.json"))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            {
                result["state"] = "complete".into();
                p.started.polling_token.0.clear();
            }
            return Ok(Err(value(result)?));
        }
        if p.next_poll > Instant::now() {
            return Ok(Err(value(
                serde_json::json!({"state":"pending","interval":p.started.interval}),
            )?));
        }
        p.next_poll = Instant::now() + Duration::from_secs(30);
        Ok(Ok(p.clone()))
    }
    pub(crate) fn finish_enrollment_poll(
        &self,
        id: &str,
        response: std::result::Result<Poll, crate::Error>,
    ) -> std::result::Result<(Value, Option<Confirmation>), RpcError> {
        let _mutation = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        let mut all = self
            .pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        let p = all
            .get_mut(id)
            .ok_or_else(|| app("not_found", "Enrollment was cancelled"))?;
        if p.deadline <= Instant::now() {
            return Err(app("expired", "Enrollment expired"));
        }
        if let Some(result) = &p.result {
            return Ok((value(result)?, None));
        }
        p.next_poll = Instant::now() + Duration::from_secs(p.started.interval);
        let response = response.map_err(|_| {
            app(
                "enrollment_unavailable",
                "Enrollment polling failed; retry login polling",
            )
        })?;
        if let Some(interval) = response.interval {
            p.started.interval = interval.clamp(1, 30);
            p.next_poll = Instant::now() + Duration::from_secs(p.started.interval);
        }
        if response.state != "approved" {
            if !matches!(response.state.as_str(), "pending" | "denied" | "expired") {
                return Err(app("enrollment_failed", "Unexpected enrollment state"));
            }
            let result = serde_json::json!({"state":response.state,"interval":p.started.interval});
            if response.state != "pending" {
                p.result = Some(result.clone());
                p.started.polling_token.0.clear();
            }
            return Ok((value(result)?, None));
        }
        let key = response.access_key.ok_or_else(|| {
            app(
                "enrollment_failed",
                "Approved enrollment has no machine credential",
            )
        })?;
        let machine_id = response
            .machine_id
            .filter(|s| tower_sessions::valid_protocol(s))
            .ok_or_else(|| app("enrollment_failed", "Invalid enrolled machine identity"))?;
        let credential_id = response
            .credential_id
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 160
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            })
            .ok_or_else(|| app("enrollment_failed", "Invalid credential identity"))?;
        let scopes = response
            .scopes
            .ok_or_else(|| app("enrollment_failed", "Enrollment omitted approved scopes"))?;
        if scopes.is_empty()
            || scopes.len() > 4
            || scopes.iter().any(|s| !p.plan.scopes.contains(s))
        {
            return Err(app(
                "enrollment_failed",
                "Enrollment granted unrequested scopes",
            ));
        }
        let url = enrollment::socket_url(&p.plan.origin);
        self.check_enrollment_origin(&url)?;
        let confirmation = Confirmation {
            origin: p.plan.origin.clone(),
            enrollment_id: id.into(),
            polling_token: p.started.polling_token.clone(),
            credential_id,
            access_key_hash: enrollment::key_hash(&key),
            expires_at_ms: now_ms()
                + p.deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64,
        };
        tower_pal::write_private(
            &self.paths.state.join("allgather-enrollment.json"),
            &serde_json::to_vec(&confirmation).map_err(enrollment_storage)?,
        )
        .map_err(enrollment_storage)?;
        let consent = Consent {
            origin: p.plan.origin.clone(),
            machine_id: machine_id.clone(),
            scopes,
        };
        tower_pal::write_private(
            &self.paths.state.join("allgather-consent.json"),
            &serde_json::to_vec(&consent).map_err(enrollment_storage)?,
        )
        .map_err(enrollment_storage)?;
        self.save_enrollment(&key, &url)?;
        let result = serde_json::json!({"state":"authorized","mode":"allgather","machine_id":machine_id,"node_label":response.node_label,"interval":p.started.interval});
        p.result = Some(result.clone());
        Ok((value(result)?, Some(confirmation)))
    }
    pub(crate) fn confirm_enrollment(
        &self,
        confirmation: &Confirmation,
    ) -> std::result::Result<Value, RpcError> {
        let mut all = self
            .pending_enrollments
            .lock()
            .map_err(|_| app("internal_error", "Enrollment state unavailable"))?;
        let p = all
            .get_mut(&confirmation.enrollment_id)
            .ok_or_else(|| app("not_found", "Enrollment was cancelled"))?;
        let Some(result) = p.result.as_mut() else {
            return Err(app("conflict", "Enrollment is not yet saved"));
        };
        result["state"] = "complete".into();
        p.started.polling_token.0.clear();
        tower_pal::remove_private(&self.paths.state.join("allgather-enrollment.json"))
            .map_err(enrollment_storage)?;
        value(result)
    }
}
