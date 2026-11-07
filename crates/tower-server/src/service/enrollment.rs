use super::*;
use crate::allgather::enrollment::{
    self, Attempts, Confirmation, Consent, Pending, Plan, Poll, Started,
};
use std::time::{Duration, Instant};

fn enrollment_storage(_: impl std::fmt::Display) -> RpcError {
    app("internal_error", "Cannot access pairing state")
}

impl Service {
    fn enrollment_actor(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<String, RpcError> {
        match self.actor(&request.context, peer)? {
            Actor::Token(token) => {
                require(&Actor::Token(token.clone()), "admin")?;
                Ok(format!("token:{}", token.name))
            }
            Actor::Agent {
                record,
                strong: true,
            } => Ok(format!("agent:{}", record.id)),
            Actor::Allgather { .. } => Err(app("denied", "Pairing is a local operation")),
            // Weak hints do not authenticate a caller; same-user retries may use a new process.
            Actor::Human | Actor::Unregistered | Actor::Agent { .. } => {
                Ok(format!("local:{}", peer.uid))
            }
        }
    }
    fn save_attempts(
        &self,
        current: &mut Attempts,
        next: Attempts,
    ) -> std::result::Result<(), RpcError> {
        enrollment::persist(&self.paths, &next).map_err(enrollment_storage)?;
        *current = next;
        Ok(())
    }
    fn prune_attempts(&self, attempts: &mut Attempts) -> std::result::Result<(), RpcError> {
        let now = now_ms();
        if attempts.values().any(|p| p.deadline_ms <= now) {
            let mut next = attempts.clone();
            next.retain(|_, p| p.deadline_ms > now);
            self.save_attempts(attempts, next)?;
        }
        Ok(())
    }
    pub(super) fn clear_enrollment_attempts(&self) -> std::result::Result<(), RpcError> {
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        self.save_attempts(&mut pending, Attempts::new())
    }
    pub(crate) fn enrollment_plan(
        &self,
        request: &Request,
        peer: Peer,
    ) -> std::result::Result<std::result::Result<Plan, Value>, RpcError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            url: Option<String>,
            pairing_code: Secret,
            node_label: Option<String>,
            #[serde(default)]
            read_only: bool,
        }
        let p: Input = params(&request.params)?;
        if !enrollment::valid_pairing_secret(&p.pairing_code.0) {
            return Err(invalid(
                "Pairing code must be an unpadded base64url encoding of 32 bytes",
            ));
        }
        let actor = self.enrollment_actor(request, peer)?;
        let origin = enrollment::configured_origin(&self.paths, p.url.as_deref())
            .map_err(|_| invalid("Supply the private Allgather HTTP(S) origin"))?;
        let _mutation = self.mutation.lock().map_err(enrollment_storage)?;
        self.check_enrollment_origin(&enrollment::socket_url(&origin))?;
        if p.node_label.as_ref().is_some_and(|s| {
            s.is_empty() || s.chars().count() > 160 || s.chars().any(char::is_control)
        }) {
            return Err(invalid("Invalid node label"));
        }
        let scopes = self.allgather_config.enrollment_scopes(p.read_only);
        if scopes.is_empty() {
            return Err(invalid(
                "No pairing scopes are enabled by the local configuration",
            ));
        }
        let host_id = self.store.host().map_err(enrollment_storage)?.host_id;
        let code_hash = enrollment::key_hash(&p.pairing_code);
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        self.prune_attempts(&mut pending)?;
        if let Some(existing) = pending.values().find(|a| a.code_hash == code_hash) {
            if existing.plan.actor != actor {
                return Err(app("denied", "Pairing belongs to another local caller"));
            }
            if existing.plan.origin != origin
                || existing.plan.host_id != host_id
                || existing.plan.scopes != scopes
                || existing.plan.label != p.node_label
            {
                return Err(app(
                    "conflict",
                    "Pairing code is already bound to different host or request options",
                ));
            }
            return match existing.public_start() {
                Some(started) => Ok(Err(value(started)?)),
                None => Ok(Ok(existing.plan.clone())),
            };
        }
        if pending.len() >= enrollment::MAX_ATTEMPTS {
            return Err(app("rate_limited", "Too many pending pairing attempts"));
        }
        let request_id = loop {
            let id = enrollment::request_id().map_err(enrollment_storage)?;
            if !pending.contains_key(&id) {
                break id;
            }
        };
        let plan = Plan {
            actor,
            origin,
            host_id,
            scopes,
            label: p.node_label,
            pairing_code: p.pairing_code,
            request_id: request_id.clone(),
            tower_version: env!("CARGO_PKG_VERSION").into(),
        };
        let mut next = pending.clone();
        next.insert(
            request_id,
            Pending {
                plan: plan.clone(),
                code_hash,
                started: None,
                deadline_ms: now_ms().saturating_add(enrollment::ATTEMPT_LIFETIME_MS),
                next_poll: None,
                result: None,
                confirmation: None,
            },
        );
        self.save_attempts(&mut pending, next)?;
        Ok(Ok(plan))
    }
    pub(crate) fn remember_enrollment(
        &self,
        plan: Plan,
        started: Started,
    ) -> std::result::Result<Value, RpcError> {
        let _mutation = self.mutation.lock().map_err(enrollment_storage)?;
        self.check_enrollment_origin(&enrollment::socket_url(&plan.origin))?;
        let mut pending = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        let current = pending
            .get(&plan.request_id)
            .ok_or_else(|| app("not_found", "Pairing was cancelled"))?;
        if current.deadline_ms <= now_ms() {
            return Err(app("expired", "Pairing attempt expired"));
        }
        if let Some(existing) = &current.started {
            if existing.enrollment_id != started.enrollment_id
                || (current.result.is_none() && existing.polling_token.0 != started.polling_token.0)
            {
                return Err(app(
                    "enrollment_failed",
                    "Pairing retry returned a different enrollment",
                ));
            }
            return value(current.public_start().unwrap());
        }
        if current.plan.actor != plan.actor
            || current.plan.body() != plan.body()
            || current.plan.origin != plan.origin
        {
            return Err(app(
                "conflict",
                "Pairing request changed before its response",
            ));
        }
        if pending.values().any(|p| {
            p.started
                .as_ref()
                .is_some_and(|s| s.enrollment_id == started.enrollment_id)
        }) {
            return Err(app(
                "enrollment_failed",
                "Enrollment identity is already bound to another request",
            ));
        }
        let mut next = pending.clone();
        let p = next.get_mut(&plan.request_id).unwrap();
        p.deadline_ms = p
            .deadline_ms
            .min(enrollment::expires_at(&started.expires_at).map_err(enrollment_storage)?);
        if p.deadline_ms <= now_ms() {
            return Err(app("expired", "Pairing invitation expired"));
        }
        p.started = Some(started);
        let result = value(p.public_start().unwrap())?;
        self.save_attempts(&mut pending, next)?;
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
            .map_err(enrollment_storage)?;
        let p = pending
            .values_mut()
            .find(|p| p.started.as_ref().is_some_and(|s| s.enrollment_id == id.id))
            .ok_or_else(|| {
                app(
                    "not_found",
                    "Pairing attempt is unavailable; retry login with the same code",
                )
            })?;
        if p.plan.actor != actor {
            return Err(app("denied", "Pairing belongs to another local caller"));
        }
        if p.deadline_ms <= now_ms() {
            return Err(app("expired", "Pairing attempt expired"));
        }
        if let Some(result) = &p.result {
            return Ok(Err(value(result)?));
        }
        let started = p.started.as_ref().unwrap();
        if p.next_poll.is_some_and(|at| at > Instant::now()) {
            return Ok(Err(value(
                serde_json::json!({"state":"pending","interval":started.interval}),
            )?));
        }
        p.next_poll = Some(Instant::now() + Duration::from_secs(30));
        Ok(Ok(p.clone()))
    }
    pub(crate) fn finish_enrollment_poll(
        &self,
        id: &str,
        response: std::result::Result<Poll, crate::Error>,
    ) -> std::result::Result<(Value, Option<Confirmation>), RpcError> {
        let _mutation = self.mutation.lock().map_err(enrollment_storage)?;
        let mut all = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        let request_id = all
            .iter()
            .find(|(_, p)| p.started.as_ref().is_some_and(|s| s.enrollment_id == id))
            .map(|(key, _)| key.clone())
            .ok_or_else(|| app("not_found", "Pairing was cancelled"))?;
        let p = all.get_mut(&request_id).unwrap();
        if p.deadline_ms <= now_ms() {
            return Err(app("expired", "Pairing attempt expired"));
        }
        if let Some(result) = &p.result {
            return Ok((value(result)?, p.confirmation.clone()));
        }
        p.next_poll =
            Some(Instant::now() + Duration::from_secs(p.started.as_ref().unwrap().interval));
        let response = response.map_err(|_| {
            app(
                "enrollment_unavailable",
                "Pairing polling failed; retry polling",
            )
        })?;
        let mut next = all.clone();
        let p = next.get_mut(&request_id).unwrap();
        let started = p.started.as_mut().unwrap();
        if let Some(interval) = response.interval {
            started.interval = interval.clamp(1, 30);
            p.next_poll = Some(Instant::now() + Duration::from_secs(started.interval));
        }
        if response.state != "approved" {
            if !matches!(response.state.as_str(), "pending" | "denied" | "expired") {
                return Err(app("enrollment_failed", "Unexpected pairing state"));
            }
            let result = serde_json::json!({"state":response.state,"interval":started.interval});
            if response.state != "pending" {
                p.result = Some(result.clone());
                p.clear_secrets();
            }
            self.save_attempts(&mut all, next)?;
            return Ok((value(result)?, None));
        }
        let key = response.access_key.ok_or_else(|| {
            app(
                "enrollment_failed",
                "Pairing approval has no machine credential",
            )
        })?;
        if key.0.len() < 16 || key.0.len() > 4096 || key.0.chars().any(char::is_control) {
            return Err(app(
                "enrollment_failed",
                "Invalid paired machine credential",
            ));
        }
        let machine_id = response
            .machine_id
            .filter(|s| tower_sessions::valid_protocol(s))
            .ok_or_else(|| app("enrollment_failed", "Invalid paired machine identity"))?;
        let credential_id = response
            .credential_id
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 160
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            })
            .ok_or_else(|| app("enrollment_failed", "Invalid credential identity"))?;
        if response
            .node_label
            .as_ref()
            .is_some_and(|label| label.chars().count() > 160 || label.chars().any(char::is_control))
            || std::iter::once(machine_id.as_str())
                .chain(response.node_label.as_deref())
                .any(|text| {
                    [&key.0, &p.plan.pairing_code.0, &started.polling_token.0]
                        .iter()
                        .any(|secret| !secret.is_empty() && text.contains(secret.as_str()))
                })
        {
            return Err(app("enrollment_failed", "Unsafe paired machine identity"));
        }
        let scopes = response
            .scopes
            .ok_or_else(|| app("enrollment_failed", "Pairing approval omitted scopes"))?;
        if scopes.is_empty()
            || scopes.len() > 4
            || scopes.iter().collect::<HashSet<_>>().len() != scopes.len()
            || scopes
                .iter()
                .any(|s| !p.plan.scopes.contains(s) || !self.allgather_config.permits_scope(s))
        {
            return Err(app(
                "enrollment_failed",
                "Pairing granted unrequested or locally disabled scopes",
            ));
        }
        let url = enrollment::socket_url(&p.plan.origin);
        self.check_enrollment_origin(&url)?;
        let confirmation = Confirmation {
            request_id: request_id.clone(),
            origin: p.plan.origin.clone(),
            enrollment_id: id.into(),
            polling_token: started.polling_token.clone(),
            credential_id,
            machine_id: machine_id.clone(),
            access_key_hash: enrollment::key_hash(&key),
            expires_at_ms: p.deadline_ms,
        };
        let consent = Consent {
            origin: p.plan.origin.clone(),
            machine_id: machine_id.clone(),
            scopes,
        };
        let result = serde_json::json!({"state":"authorized","mode":"allgather","machine_id":machine_id,"node_label":response.node_label,"scopes":consent.scopes,"interval":started.interval});
        p.result = Some(result.clone());
        p.confirmation = Some(confirmation.clone());
        enrollment::check_capacity(&next).map_err(enrollment_storage)?;
        tower_pal::write_private(
            &self.paths.state.join("allgather-consent.json"),
            &serde_json::to_vec(&consent).map_err(enrollment_storage)?,
        )
        .map_err(enrollment_storage)?;
        self.save_enrollment(&key, &url)?;
        self.save_attempts(&mut all, next)?;
        Ok((value(result)?, Some(confirmation)))
    }
    fn saved_credential_matches(&self, c: &Confirmation) -> bool {
        let key = tower_pal::read_private(&self.paths.state.join("allgather.key"))
            .ok()
            .and_then(|v| String::from_utf8(v).ok());
        let consent = tower_pal::read_private(&self.paths.state.join("allgather-consent.json"))
            .ok()
            .and_then(|v| serde_json::from_slice::<Consent>(&v).ok());
        key.is_some_and(|key| enrollment::key_hash(&Secret(key)) == c.access_key_hash)
            && tower_pal::read_private(&self.paths.state.join("allgather-server"))
                .is_ok_and(|v| v == enrollment::socket_url(&c.origin).as_bytes())
            && consent.is_some_and(|consent| {
                consent.origin == c.origin
                    && consent.machine_id == c.machine_id
                    && consent
                        .scopes
                        .iter()
                        .all(|scope| self.allgather_config.permits_scope(scope))
            })
    }
    pub(crate) fn pending_enrollment_confirmation(
        &self,
    ) -> std::result::Result<Option<Confirmation>, RpcError> {
        let _mutation = self.mutation.lock().map_err(enrollment_storage)?;
        let mut all = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        self.prune_attempts(&mut all)?;
        Ok(all
            .values()
            .filter_map(|p| p.confirmation.as_ref())
            .find(|c| self.saved_credential_matches(c))
            .cloned())
    }
    pub(crate) fn confirm_enrollment(
        &self,
        confirmation: &Confirmation,
    ) -> std::result::Result<Value, RpcError> {
        let _mutation = self.mutation.lock().map_err(enrollment_storage)?;
        let mut all = self
            .pending_enrollments
            .lock()
            .map_err(enrollment_storage)?;
        let p = all
            .get(&confirmation.request_id)
            .ok_or_else(|| app("not_found", "Pairing was cancelled"))?;
        if p.deadline_ms <= now_ms() {
            return Err(app("expired", "Pairing attempt expired"));
        }
        if p.plan.origin != confirmation.origin
            || p.started
                .as_ref()
                .is_none_or(|s| s.enrollment_id != confirmation.enrollment_id)
            || p.result
                .as_ref()
                .is_none_or(|r| r["machine_id"] != confirmation.machine_id)
        {
            return Err(app(
                "conflict",
                "Pairing confirmation identity does not match",
            ));
        }
        if p.result.as_ref().is_some_and(|r| r["state"] == "complete") {
            return value(p.result.as_ref().unwrap());
        }
        if !p.confirmation.as_ref().is_some_and(|c| {
            c.enrollment_id == confirmation.enrollment_id
                && c.credential_id == confirmation.credential_id
                && c.access_key_hash == confirmation.access_key_hash
        }) || !self.saved_credential_matches(confirmation)
        {
            return Err(app(
                "conflict",
                "Pairing confirmation does not match the saved credential",
            ));
        }
        let mut next = all.clone();
        let p = next.get_mut(&confirmation.request_id).unwrap();
        let result = p
            .result
            .as_mut()
            .ok_or_else(|| app("conflict", "Pairing has no saved credential"))?;
        result["state"] = "complete".into();
        let result = value(result)?;
        p.clear_secrets();
        self.save_attempts(&mut all, next)?;
        Ok(result)
    }
}
