use super::*;
use tower_protocol::RuntimeIdentity;

pub(super) fn validate_new(
    tx: &Transaction<'_>,
    b: &Binding,
    c: &ExecutionCommand,
) -> Result<Option<Vec<Binding>>> {
    let requested = tower_sessions::execution::force_resume(c)?;
    let mut targets = Vec::new();
    if let Some(requested) = &requested {
        if b.protocol == "terminal" || b.resume_session_id.is_none() {
            return Err(error(
                "unsupported_control",
                "Force resume requires structured native history",
            ));
        }
        for id in requested {
            let body: Option<String> = tx.query_row("SELECT binding FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3", params![b.namespace(),id.runtime_id,id.generation], |r| r.get(0)).optional().map_err(internal)?;
            let target: Binding = serde_json::from_str(&body.ok_or_else(|| {
                error(
                    "cleanup_target_conflict",
                    "Cleanup target identity is unavailable",
                )
            })?)
            .map_err(internal)?;
            if target.session_id != b.session_id
                || target.namespace() != b.namespace()
                || target.protocol != b.protocol
                || target.harness != b.harness
                || target.tower_session_id != b.tower_session_id
            {
                return Err(error(
                    "cleanup_target_conflict",
                    "Cleanup targets must belong to this logical session and backend",
                ));
            }
            targets.push(target);
        }
    }
    let mut q = tx.prepare("SELECT runtime_id,generation FROM runtimes WHERE namespace=?1 AND logical_session=?2 AND runtime_id!=?3 AND (fenced=0 OR termination!='confirmed')").map_err(internal)?;
    let unresolved = q
        .query_map(params![b.namespace(), b.session_id, b.runtime_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(internal)?;
    for old in unresolved {
        let (id, generation) = old.map_err(internal)?;
        if !targets
            .iter()
            .any(|t| t.runtime_id == id && t.generation == generation)
        {
            return Err(error(
                if requested.is_some() {
                    "cleanup_target_conflict"
                } else {
                    "runtime_overlap"
                },
                "An old execution is still active or uncertain; stop it or include it in force resume",
            ));
        }
    }
    Ok(requested.map(|_| targets))
}
pub(super) fn record(
    tx: &Transaction<'_>,
    b: &Binding,
    c: &ExecutionCommand,
    targets: &[Binding],
) -> Result<()> {
    let ids: Vec<_> = targets
        .iter()
        .map(|t| RuntimeIdentity {
            runtime_id: t.runtime_id.clone(),
            generation: t.generation.clone(),
        })
        .collect();
    tx.execute(
        "INSERT INTO force_resumes(namespace,command_id,targets,phase) VALUES(?1,?2,?3,'cleanup')",
        params![
            b.namespace(),
            c.command_id,
            serde_json::to_string(&ids).map_err(internal)?
        ],
    )
    .map_err(internal)?;
    for target in targets {
        fence_tx(tx, target, "force_resume", "unknown")?;
    }
    Ok(())
}
fn check_stopped(tx: &Transaction<'_>, b: &Binding, id: &str) -> Result<()> {
    let body: String = tx
        .query_row(
            "SELECT targets FROM force_resumes WHERE namespace=?1 AND command_id=?2",
            params![b.namespace(), id],
            |r| r.get(0),
        )
        .map_err(internal)?;
    let targets: Vec<RuntimeIdentity> = serde_json::from_str(&body).map_err(internal)?;
    for target in targets {
        let stopped: bool = tx.query_row("SELECT fenced=1 AND termination='confirmed' FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3", params![b.namespace(),target.runtime_id,target.generation], |r| r.get(0)).optional().map_err(internal)?.unwrap_or(false);
        if !stopped {
            return Err(error(
                "cleanup_unconfirmed",
                "An exact cleanup target is not confirmed stopped",
            ));
        }
    }
    let unresolved: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM runtimes WHERE namespace=?1 AND logical_session=?2 AND runtime_id!=?3 AND (fenced=0 OR termination!='confirmed'))",params![b.namespace(),b.session_id,b.runtime_id],|r|r.get(0)).map_err(internal)?;
    if unresolved {
        return Err(error(
            "cleanup_target_conflict",
            "Another unresolved execution blocks replacement",
        ));
    }
    Ok(())
}
pub(super) fn before_dispatch(tx: &Transaction<'_>, b: &Binding, id: &str) -> Result<()> {
    let phase: Option<String> = tx
        .query_row(
            "SELECT phase FROM force_resumes WHERE namespace=?1 AND command_id=?2",
            params![b.namespace(), id],
            |r| r.get(0),
        )
        .optional()
        .map_err(internal)?;
    if let Some(phase) = phase {
        if phase != "ready" {
            return Err(error(
                "cleanup_unconfirmed",
                "Cleanup must finish before replacement dispatch",
            ));
        }
        check_stopped(tx, b, id)?;
    }
    Ok(())
}
impl Journal {
    pub fn validate_new(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        validate(b, c)?;
        if parse_time(&c.expires_at)? <= millis() {
            return Err(error(
                "command_expired",
                "Command admission deadline expired",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let count: i64 = tx
            .query_row("SELECT count(*) FROM commands", [], |r| r.get(0))
            .map_err(internal)?;
        if count as u64 >= self.limits.receipt_count {
            return Err(error(
                "runtime_capacity",
                "Command receipt capacity exhausted",
            ));
        }
        validate_new(&tx, b, c).map(|_| ())
    }
    pub fn force_phase(&self, b: &Binding) -> Result<Option<String>> {
        self.lock()?.query_row("SELECT f.phase FROM force_resumes f JOIN commands c USING(namespace,command_id) WHERE c.namespace=?1 AND c.runtime_id=?2 AND c.generation=?3", params![b.namespace(),b.runtime_id,b.generation],|r|r.get(0)).optional().map_err(internal)
    }
    pub fn force_command(&self, b: &Binding) -> Result<Option<ExecutionCommand>> {
        let conn = self.lock()?;
        let row: Option<(String,String,i64)> = conn.query_row("SELECT c.command_id,c.payload,c.expires_at FROM force_resumes f JOIN commands c USING(namespace,command_id) JOIN runtimes r ON r.namespace=c.namespace AND r.runtime_id=c.runtime_id WHERE c.namespace=?1 AND c.runtime_id=?2 AND c.generation=?3 AND r.fenced=0 AND f.phase IN ('cleanup','ready') AND c.payload IS NOT NULL",params![b.namespace(),b.runtime_id,b.generation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(internal)?;
        row.map(|(command_id, payload, expires)| {
            Ok(ExecutionCommand {
                command_id,
                session_id: b.session_id.clone(),
                runtime_id: b.runtime_id.clone(),
                generation: b.generation.clone(),
                kind: "resume".into(),
                payload: serde_json::from_str(&payload).map_err(internal)?,
                expires_at: crate::format_time(expires as u64),
            })
        })
        .transpose()
    }
    pub fn force_targets(&self, b: &Binding, id: &str) -> Result<Vec<Binding>> {
        let text: String = self
            .lock()?
            .query_row(
                "SELECT targets FROM force_resumes WHERE namespace=?1 AND command_id=?2",
                params![b.namespace(), id],
                |r| r.get(0),
            )
            .map_err(internal)?;
        let ids: Vec<RuntimeIdentity> = serde_json::from_str(&text).map_err(internal)?;
        ids.into_iter()
            .map(|t| self.binding(&b.namespace(), &t.runtime_id, &t.generation))
            .collect()
    }
    pub fn force_step(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        ensure_live(&tx, b)?;
        if parse_time(&c.expires_at)? <= millis() {
            return Err(error(
                "command_expired",
                "Force resume expired before cleanup",
            ));
        }
        let changed=tx.execute("UPDATE force_resumes SET cleanup_started=1 WHERE namespace=?1 AND command_id=?2 AND phase='cleanup'",params![b.namespace(),c.command_id]).map_err(internal)?;
        if changed != 1 {
            return Err(error(
                "runtime_conflict",
                "Cleanup phase is no longer current",
            ));
        }
        tx.commit().map_err(internal)
    }
    pub fn force_ready(&self, b: &Binding, c: &ExecutionCommand) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        ensure_live(&tx, b)?;
        check_stopped(&tx, b, &c.command_id)?;
        tx.execute("UPDATE force_resumes SET phase='ready' WHERE namespace=?1 AND command_id=?2 AND phase IN ('cleanup','ready')",params![b.namespace(),c.command_id]).map_err(internal)?;
        tx.commit().map_err(internal)
    }
    pub fn force_failed(&self, b: &Binding, c: &ExecutionCommand, code: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let phase:String=tx.query_row("SELECT f.phase FROM force_resumes f JOIN commands c USING(namespace,command_id) WHERE c.namespace=?1 AND c.command_id=?2 AND c.runtime_id=?3 AND c.generation=?4",params![b.namespace(),c.command_id,b.runtime_id,b.generation],|r|r.get(0)).map_err(internal)?;
        if phase == "dispatched" {
            fence_tx(&tx, b, "dispatch_unknown", "unknown")?;
            return tx.commit().map_err(internal);
        }
        tx.execute(
            "UPDATE force_resumes SET phase='failed' WHERE namespace=?1 AND command_id=?2",
            params![b.namespace(), c.command_id],
        )
        .map_err(internal)?;
        finish_dispatch_tx(
            &tx,
            b,
            &c.command_id,
            false,
            None,
            Some(ExecutionError {
                code: code.into(),
                message: "Force resume did not complete; no replacement was dispatched".into(),
                stage: "execution".into(),
                may_have_effect: true,
            }),
        )?;
        fence_tx(&tx, b, code, "confirmed")?;
        tx.commit().map_err(internal)
    }
    pub fn never_dispatched(&self, b: &Binding) -> Result<bool> {
        let conn = self.lock()?;
        let mut q=conn.prepare("SELECT receipt FROM commands WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND kind IN ('launch','resume')").map_err(internal)?;
        let rows = q
            .query_map(params![b.namespace(), b.runtime_id, b.generation], |r| {
                r.get::<_, String>(0)
            })
            .map_err(internal)?;
        let mut found = false;
        for row in rows {
            found = true;
            let r: ExecutionReceipt =
                serde_json::from_str(&row.map_err(internal)?).map_err(internal)?;
            if r.dispatch.state != "not_started"
                && !(r.dispatch.state == "finished"
                    && r.error.is_some_and(|e| !e.may_have_effect)
                    && r.result.is_none())
            {
                return Ok(false);
            }
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn binding(runtime: &str) -> Binding {
        Binding {
            origin: "https://allgather.example".into(),
            machine_id: "machine".into(),
            session_id: "logical".into(),
            runtime_id: runtime.into(),
            generation: format!("generation-{runtime}"),
            tower_session_id: "tower-id".into(),
            owner_id: format!("owner-{runtime}"),
            protocol: "codex-app-server".into(),
            harness: "codex".into(),
            cwd: "/work/project".into(),
            resume_session_id: None,
        }
    }
    fn command(b: &Binding, id: &str, kind: &str, payload: serde_json::Value) -> ExecutionCommand {
        let mut c = crate::command(b, kind, payload).unwrap();
        c.command_id = id.into();
        c
    }
    fn replacement(old: &Binding) -> (Binding, ExecutionCommand) {
        let mut b = binding("new");
        b.resume_session_id = Some("native".into());
        let c = command(
            &b,
            "force",
            "resume",
            json!({"protocol":"codex-app-server","harness":"codex","cwd":b.cwd,"resume_session_id":"native","force":true,"cleanup":[{"runtime_id":old.runtime_id,"generation":old.generation}]}),
        );
        (b, c)
    }
    #[test]
    fn empty_cleanup_is_rejected_and_stopped_sessions_resume_without_force() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        let old = binding("old");
        j.admit(&old, &command(&old, "first", "launch", json!({})))
            .unwrap();
        let (b, mut c) = replacement(&old);
        let mut payload = serde_json::to_value(&c.payload).unwrap();
        payload["cleanup"] = json!([]);
        c.payload = serde_json::from_value(payload).unwrap();
        for termination in [None, Some("unknown"), Some("confirmed")] {
            if let Some(termination) = termination {
                j.fence(&old, "owner_lost", termination).unwrap();
            }
            assert_eq!(
                j.validate_new(&b, &c).unwrap_err().data.code,
                "invalid_params"
            );
            assert_eq!(j.admit(&b, &c).unwrap_err().data.code, "invalid_params");
            assert_eq!(j.state(&old).unwrap().1, termination.is_some());
            assert!(
                j.binding(&b.namespace(), &b.runtime_id, &b.generation)
                    .is_err()
            );
            assert!(j.force_phase(&b).unwrap().is_none());
        }
        j.validate_new(&b, &replacement(&old).1).unwrap();
        let mut payload = serde_json::to_value(&c.payload).unwrap();
        payload.as_object_mut().unwrap().remove("force");
        payload.as_object_mut().unwrap().remove("cleanup");
        c.payload = serde_json::from_value(payload).unwrap();
        j.validate_new(&b, &c).unwrap();
        j.admit(&b, &c).unwrap();
        drop(j);
        let j = Journal::open(&path, Limits::default()).unwrap();
        assert!(j.force_phase(&b).unwrap().is_none());
        assert!(j.begin_dispatch(&b, &c.command_id).unwrap());
        assert!(!j.begin_dispatch(&b, &c.command_id).unwrap());
        assert_eq!(j.state(&old).unwrap().2, "confirmed");
    }
    #[test]
    fn malformed_force_commands_cannot_admit_or_fence() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(&root.path().join("runtime.db"), Limits::default()).unwrap();
        let old = binding("old");
        j.admit(&old, &command(&old, "first", "launch", json!({})))
            .unwrap();
        let target = json!({"runtime_id":old.runtime_id,"generation":old.generation});
        let oversized: Vec<_> = (0..33)
            .map(|i| json!({"runtime_id":format!("old-{i}"),"generation":"generation"}))
            .collect();
        for (kind, payload) in [
            ("resume", json!({"force":false})),
            ("resume", json!({"cleanup":[target]})),
            ("resume", json!({"force":false,"cleanup":[target]})),
            ("resume", json!({"force":true})),
            ("resume", json!({"force":true,"cleanup":[target,target]})),
            ("resume", json!({"force":true,"cleanup":oversized})),
            (
                "resume",
                json!({"force":true,"cleanup":[{"runtime_id":"new","generation":"generation-new"}]}),
            ),
            (
                "resume",
                json!({"protocol":"terminal","force":true,"cleanup":[target]}),
            ),
            ("launch", json!({"force":true,"cleanup":[target]})),
        ] {
            let (b, _) = replacement(&old);
            let c = command(&b, "invalid-force", kind, payload);
            assert_eq!(j.admit(&b, &c).unwrap_err().data.code, "invalid_params");
            assert!(!j.state(&old).unwrap().1);
            assert!(
                j.binding(&b.namespace(), &b.runtime_id, &b.generation)
                    .is_err()
            );
        }
    }
    #[test]
    fn cleanup_is_atomic_recoverable_and_required_before_launch() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        let old = binding("old");
        let first = command(&old, "first", "launch", json!({}));
        j.admit(&old, &first).unwrap();
        j.begin_dispatch(&old, &first.command_id).unwrap();
        let (b, c) = replacement(&old);
        let accepted = j.admit(&b, &c).unwrap();
        let original_loss = j.losses(&old.namespace()).unwrap().pop().unwrap();
        assert!(j.state(&old).unwrap().1);
        assert_eq!(j.state(&old).unwrap().2, "unknown");
        assert_eq!(j.admit(&b, &c).unwrap(), accepted);
        assert_eq!(
            j.begin_dispatch(&b, &c.command_id).unwrap_err().data.code,
            "cleanup_unconfirmed"
        );
        drop(j);
        let j = Journal::open(&path, Limits::default()).unwrap();
        let restored = j.force_command(&b).unwrap().unwrap();
        assert_eq!(restored.command_id, c.command_id);
        assert_eq!(
            serde_json::to_value(&restored.payload).unwrap(),
            serde_json::to_value(&c.payload).unwrap()
        );
        assert_eq!(
            j.force_targets(&b, &c.command_id).unwrap()[0].runtime_id,
            old.runtime_id
        );
        j.force_step(&b, &c).unwrap();
        let loss = j.fence(&old, "force_resume", "confirmed").unwrap();
        assert_eq!(loss.loss_id, original_loss.loss_id);
        assert_eq!(loss.report_revision, 2);
        assert_eq!(j.admit(&b, &c).unwrap(), accepted);
        j.force_ready(&b, &c).unwrap();
        assert!(j.begin_dispatch(&b, &c.command_id).unwrap());
        assert!(!j.begin_dispatch(&b, &c.command_id).unwrap());
        assert_eq!(j.force_phase(&b).unwrap().as_deref(), Some("dispatched"));
        assert!(j.force_command(&b).unwrap().is_none());
        j.append(
            &b,
            "runtime_state",
            serde_json::from_value(json!({"state":"idle"})).unwrap(),
            None,
            true,
        )
        .unwrap();
        assert!(j.batch(&b).unwrap().is_none());
        j.acknowledge_loss(&old, &loss.loss_id, loss.report_revision)
            .unwrap();
        assert!(j.batch(&b).unwrap().is_some());
        j.force_failed(&b, &c, "cleanup_unconfirmed").unwrap();
        assert_eq!(
            j.state(&b).unwrap().2,
            "unknown",
            "A crash after dispatch cannot claim that the replacement never ran"
        );
        let uncertain = j.admit(&b, &c).unwrap();
        assert_eq!(uncertain.state, "accepted");
        assert_eq!(uncertain.acceptance, accepted.acceptance);
        assert_eq!(uncertain.dispatch.state, "unknown");
        assert_eq!(uncertain.error.as_ref().unwrap().code, "dispatch_unknown");
        assert_eq!(uncertain.error.as_ref().unwrap().stage, "recovery");
        assert!(uncertain.error.as_ref().unwrap().may_have_effect);
        let mut changed = c.clone();
        changed.payload = serde_json::from_value(
            json!({"force":true,"cleanup":[{"runtime_id":"different","generation":"different"}]}),
        )
        .unwrap();
        assert_eq!(
            j.admit(&b, &changed).unwrap_err().data.code,
            "command_conflict"
        );
    }
    #[test]
    fn invalid_targets_cannot_fence_or_reserve_any_execution() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(&root.path().join("runtime.db"), Limits::default()).unwrap();
        let old = binding("old");
        j.admit(&old, &command(&old, "first", "launch", json!({})))
            .unwrap();
        let mut foreign = binding("foreign");
        foreign.session_id = "other-logical".into();
        foreign.tower_session_id = "other-tower".into();
        j.admit(&foreign, &command(&foreign, "foreign", "launch", json!({})))
            .unwrap();
        for target in [
            json!({"runtime_id":"foreign","generation":foreign.generation}),
            json!({"runtime_id":"old","generation":"wrong"}),
        ] {
            let (b, mut c) = replacement(&old);
            c.payload = serde_json::from_value(json!({"force":true,"cleanup":[target]})).unwrap();
            assert_eq!(
                j.admit(&b, &c).unwrap_err().data.code,
                "cleanup_target_conflict"
            );
            assert!(!j.state(&old).unwrap().1);
            assert!(!j.state(&foreign).unwrap().1);
            assert!(
                j.binding(&b.namespace(), &b.runtime_id, &b.generation)
                    .is_err()
            );
        }
        let (b, _) = replacement(&old);
        assert_eq!(
            j.admit(&b, &command(&b, "ordinary", "resume", json!({})))
                .unwrap_err()
                .data
                .code,
            "runtime_overlap"
        );
    }
    #[test]
    fn partial_cleanup_retains_history_and_blocks_replacement() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        let old = binding("old");
        j.admit(&old, &command(&old, "first", "launch", json!({})))
            .unwrap();
        j.fence(&old, "done", "confirmed").unwrap();
        let second = binding("second");
        j.admit(&second, &command(&second, "second", "launch", json!({})))
            .unwrap();
        // A retained generation from the retired overlap prototype may still be uncertain.
        j.lock()
            .unwrap()
            .execute(
                "UPDATE runtimes SET termination='unknown',loss=NULL WHERE runtime_id='old'",
                [],
            )
            .unwrap();
        let (b, mut c) = replacement(&old);
        assert_eq!(
            j.admit(&b, &c).unwrap_err().data.code,
            "cleanup_target_conflict"
        );
        c.payload=serde_json::from_value(json!({"force":true,"cleanup":[{"runtime_id":old.runtime_id,"generation":old.generation},{"runtime_id":second.runtime_id,"generation":second.generation}]})).unwrap();
        j.admit(&b, &c).unwrap();
        j.force_step(&b, &c).unwrap();
        j.fence(&old, "force_resume", "confirmed").unwrap();
        drop(j);
        let j = Journal::open(&path, Limits::default()).unwrap();
        assert_eq!(j.state(&old).unwrap().2, "confirmed");
        assert_eq!(j.state(&second).unwrap().2, "unknown");
        assert_eq!(
            j.force_ready(&b, &c).unwrap_err().data.code,
            "cleanup_unconfirmed"
        );
        let before = j.admit(&b, &c).unwrap();
        j.lock().unwrap().execute_batch("CREATE TRIGGER reject_replacement_loss BEFORE UPDATE OF fenced ON runtimes WHEN NEW.runtime_id='new' BEGIN SELECT RAISE(ABORT, 'interrupted finalization'); END;").unwrap();
        assert!(j.force_failed(&b, &c, "cleanup_unconfirmed").is_err());
        assert_eq!(j.force_phase(&b).unwrap().as_deref(), Some("cleanup"));
        assert_eq!(j.admit(&b, &c).unwrap(), before);
        assert!(j.force_command(&b).unwrap().is_some());
        assert!(!j.state(&b).unwrap().1);
        j.lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_replacement_loss")
            .unwrap();
        j.force_failed(&b, &c, "cleanup_unconfirmed").unwrap();
        assert_eq!(j.state(&b).unwrap().2, "confirmed");
        assert!(j.force_command(&b).unwrap().is_none());
        assert_eq!(j.force_targets(&b, &c.command_id).unwrap().len(), 2);
        let final_receipt = j.admit(&b, &c).unwrap();
        assert_eq!(
            serde_json::to_value(&final_receipt).unwrap(),
            json!({
                "command_id":c.command_id,"runtime_id":b.runtime_id,"generation":b.generation,
                "receipt_revision":before.receipt_revision+1,"state":"accepted",
                "acceptance":before.acceptance,
                "dispatch":{"state":"finished","transport_written":false},
                "error":{"code":"cleanup_unconfirmed","message":"Force resume did not complete; no replacement was dispatched","stage":"execution","may_have_effect":true}
            })
        );
        let loss = j
            .losses(&b.namespace())
            .unwrap()
            .into_iter()
            .find(|loss| loss.runtime_id == b.runtime_id)
            .unwrap();
        assert_eq!(loss.owner_id, b.owner_id);
        assert_eq!(loss.generation, b.generation);
        assert_eq!(loss.report_revision, 1);
        assert_eq!(loss.termination, "confirmed");
        assert_eq!(loss.reason, "cleanup_unconfirmed");
        assert_eq!(j.state(&old).unwrap().2, "confirmed");
        assert_eq!(j.state(&second).unwrap().2, "unknown");
        drop(j);
        let j = Journal::open(&path, Limits::default()).unwrap();
        j.force_failed(&b, &c, "cleanup_unconfirmed").unwrap();
        assert_eq!(j.admit(&b, &c).unwrap(), final_receipt);
        let replay = j
            .losses(&b.namespace())
            .unwrap()
            .into_iter()
            .find(|loss| loss.runtime_id == b.runtime_id)
            .unwrap();
        assert_eq!(
            serde_json::to_value(replay).unwrap(),
            serde_json::to_value(loss).unwrap()
        );
        assert!(j.force_command(&b).unwrap().is_none());
        assert_eq!(
            j.begin_dispatch(&b, &c.command_id).unwrap_err().data.code,
            "runtime_fenced"
        );
    }
}
