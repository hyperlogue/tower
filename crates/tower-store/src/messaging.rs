use super::*;
use tower_protocol::{Approval, Capabilities, DeliveryEndpoint};

#[derive(Clone)]
pub struct StoredApproval {
    pub record: Approval,
    pub body_hash: Vec<u8>,
    pub consumed: bool,
}

pub trait MessageStore: Send + Sync {
    fn set_sync_state(&self, id: &str, state: &str) -> Result<()>;
    fn delivery(&self, id: &str) -> Result<DeliveryEndpoint>;
    fn set_delivery(
        &self,
        id: &str,
        endpoint: &DeliveryEndpoint,
        caps: Capabilities,
        now: u64,
    ) -> Result<()>;
    fn approvals(&self) -> Result<Vec<StoredApproval>>;
    fn approval(&self, id: &str) -> Result<StoredApproval>;
    fn matching_approvals(
        &self,
        from: &str,
        to: &str,
        digest: &[u8],
    ) -> Result<Vec<StoredApproval>>;
    fn approval_count(&self) -> Result<usize>;
    fn put_approval(&self, approval: StoredApproval, rule: Option<Rule>) -> Result<()>;
    fn expire_approvals(&self, now: u64) -> Result<Vec<Approval>>;
}
impl MessageStore for LocalStore {
    fn set_sync_state(&self, id: &str, state: &str) -> Result<()> {
        if !matches!(state, "pending" | "synced" | "rejected") {
            return Err(Error::Conflict("Invalid sync state"));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut record = stored_agent(&tx, id)?.record;
        if record.sync_state != state {
            record.sync_state = state.into();
            record.revision = next_revision(&tx)?;
            save_record(&tx, &record)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn delivery(&self, id: &str) -> Result<DeliveryEndpoint> {
        let json: Option<String> = self
            .lock()?
            .query_row("SELECT data FROM delivery WHERE id=?1", [id], |r| r.get(0))
            .optional()?;
        Ok(json
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default())
    }
    fn set_delivery(
        &self,
        id: &str,
        endpoint: &DeliveryEndpoint,
        caps: Capabilities,
        now: u64,
    ) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?.record;
        if agent.ownership == "owned" {
            return Err(Error::Conflict("managed delivery belongs to its backend"));
        }
        apply_delivery(&tx, &mut agent, endpoint, caps)?;
        agent.updated_at = now;
        agent.revision = next_revision(&tx)?;
        save_record(&tx, &agent)?;
        tx.commit()?;
        Ok(())
    }
    fn approvals(&self) -> Result<Vec<StoredApproval>> {
        let conn = self.lock()?;
        let mut query =
            conn.prepare("SELECT data,body_hash,consumed FROM approvals ORDER BY created_at DESC")?;
        let rows = query
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(s, body_hash, consumed)| {
                Ok(StoredApproval {
                    record: serde_json::from_str(&s)?,
                    body_hash,
                    consumed,
                })
            })
            .collect()
    }
    fn approval(&self, id: &str) -> Result<StoredApproval> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT data,body_hash,consumed FROM approvals WHERE id=?1",
                [id],
                |r| Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        Ok(StoredApproval {
            record: serde_json::from_str(&row.0)?,
            body_hash: row.1,
            consumed: row.2,
        })
    }
    fn approval_count(&self) -> Result<usize> {
        let count: i64 = self
            .lock()?
            .query_row("SELECT COUNT(*) FROM approvals", [], |r| r.get(0))?;
        usize::try_from(count).map_err(|_| Error::Schema)
    }
    fn matching_approvals(
        &self,
        from: &str,
        to: &str,
        digest: &[u8],
    ) -> Result<Vec<StoredApproval>> {
        let conn = self.lock()?;
        let mut query=conn.prepare("SELECT data,body_hash,consumed FROM approvals WHERE body_hash=?1 AND json_extract(data,'$.from')=?2 AND json_extract(data,'$.to')=?3 ORDER BY created_at DESC")?;
        let rows = query
            .query_map(params![digest, from, to], |r| {
                Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(json, body_hash, consumed)| {
                Ok(StoredApproval {
                    record: serde_json::from_str(&json)?,
                    body_hash,
                    consumed,
                })
            })
            .collect()
    }
    fn put_approval(&self, approval: StoredApproval, rule: Option<Rule>) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if let Some(rule) = rule {
            let mut rules = read_rules(&tx, "local_rules")?;
            if rules.len() >= 1024 {
                return Err(Error::Conflict("rule limit reached"));
            }
            let i = rules
                .iter()
                .rposition(|r| r.from == "*" && r.to == "*" && r.action == "ask")
                .unwrap_or(rules.len());
            rules.insert(i, rule);
            write_rules(&tx, "local_rules", &rules)?;
        }
        let a = &approval.record;
        tx.execute("INSERT INTO approvals(id,data,body_hash,consumed,created_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET data=excluded.data,consumed=excluded.consumed", params![a.id,serde_json::to_string(a)?,approval.body_hash,approval.consumed,i64::try_from(a.created_at).map_err(|_|Error::Schema)?])?;
        tx.commit()?;
        Ok(())
    }
    fn expire_approvals(&self, now: u64) -> Result<Vec<Approval>> {
        let mut expired = Vec::new();
        let due: Vec<StoredApproval> = {
            let conn = self.lock()?;
            let mut query=conn.prepare("SELECT data,body_hash,consumed FROM approvals WHERE json_extract(data,'$.state')='pending' AND json_extract(data,'$.expires_at')<=?1")?;
            let rows = query
                .query_map([i64::try_from(now).map_err(|_| Error::Schema)?], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, bool>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows.into_iter()
                .map(|(s, body_hash, consumed)| {
                    Ok(StoredApproval {
                        record: serde_json::from_str(&s)?,
                        body_hash,
                        consumed,
                    })
                })
                .collect::<Result<_>>()?
        };
        for mut a in due {
            if a.record.state == "pending" && a.record.expires_at <= now {
                a.record.state = "denied".into();
                a.record.reason = Some("expired".into());
                a.record.resolved_at = Some(now);
                expired.push(a.record.clone());
                self.put_approval(a, None)?;
            }
        }
        self.lock()?.execute(
            "DELETE FROM approvals WHERE created_at < ?1",
            [i64::try_from(now.saturating_sub(90 * 86_400_000)).map_err(|_| Error::Schema)?],
        )?;
        Ok(expired)
    }
}

pub struct DeliveryState {
    pub endpoint: DeliveryEndpoint,
    pub caps: Capabilities,
}
pub(super) fn apply_delivery(
    tx: &Transaction<'_>,
    agent: &mut AgentRecord,
    endpoint: &DeliveryEndpoint,
    mut caps: Capabilities,
) -> Result<()> {
    tx.execute("INSERT INTO delivery(id,data) VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET data=excluded.data", params![agent.id,serde_json::to_string(endpoint)?])?;
    if endpoint.app_server.is_some() {
        agent.shared_process = true;
    }
    caps.kill = agent.caps.kill && !agent.shared_process && endpoint.app_server.is_none();
    agent.caps = caps;
    if agent.harness == "codex" {
        agent.mode = Some(
            if endpoint.app_server.is_some()
                || agent.shared_process
                || agent.mode.as_deref() == Some("app-server")
            {
                "app-server"
            } else {
                "native"
            }
            .into(),
        );
    }
    if agent.sources == ["scan"] {
        agent.sources.push("cli".into());
    }
    Ok(())
}
