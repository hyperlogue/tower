//! Transactional local persistence. Runtime observations never contain message bodies.

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};
use tower_protocol::{
    AgentFilter, AgentRecord, Patch, Rule, RulesView, TokenInfo, UpdateParams, UpdateResult, Value,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage failure: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("invalid stored data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage lock poisoned")]
    Poisoned,
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    Conflict(&'static str),
    #[error("unsupported database schema version")]
    Schema,
}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug)]
pub struct Host {
    pub host_id: String,
    pub node_name: String,
    pub mode: String,
    pub policy_revision: Option<u64>,
}
#[derive(Clone, Debug)]
pub struct StoredAgent {
    pub record: AgentRecord,
    pub token_hash: Vec<u8>,
    pub seq: u64,
    pub ended_at: Option<u64>,
    pub recovery_pending: bool,
}
#[derive(Clone, Debug)]
pub struct Registration {
    pub agent: StoredAgent,
    pub created: bool,
}
#[derive(Clone, Debug)]
pub struct StoredToken {
    pub info: TokenInfo,
    pub hash: Vec<u8>,
}

/// Each mutation commits its identity, revision and metadata together.
/// Callers never receive a raw database connection.
pub trait Store: Send + Sync {
    fn initialize(&self, host_id: &str, node_name: &str) -> Result<Host>;
    fn host(&self) -> Result<Host>;
    fn registration_target(&self, candidate: &AgentRecord) -> Result<Option<StoredAgent>>;
    /// Commit only to the identity validated by the caller, including for a new record.
    fn register(
        &self,
        candidate: AgentRecord,
        hash: &[u8],
        expected_id: &str,
    ) -> Result<Registration>;
    fn agent(&self, id: &str) -> Result<StoredAgent>;
    fn agent_by_token_hash(&self, hash: &[u8]) -> Result<StoredAgent>;
    fn agent_by_native_id(&self, harness: &str, native_id: &str) -> Result<StoredAgent>;
    fn agents(&self, filter: &AgentFilter) -> Result<Vec<AgentRecord>>;
    fn update(&self, update: &UpdateParams, now: u64) -> Result<UpdateResult>;
    fn heartbeat(&self, id: &str, now: u64) -> Result<()>;
    fn unregister(&self, id: &str, now: u64) -> Result<AgentRecord>;
    fn mark_lost(&self, id: &str, now: u64) -> Result<()>;
    fn mark_unconfirmed(&self, now: u64) -> Result<()>;
    fn expire(&self, now: u64, owned_retention_ms: u64) -> Result<Vec<String>>;
    fn rules(&self) -> Result<RulesView>;
    fn put_rule(&self, rule: Rule) -> Result<()>;
    fn remove_rule(&self, id: &str) -> Result<()>;
    fn replace_rules(&self, rules: &[Rule]) -> Result<()>;
    fn issue_token(&self, info: TokenInfo, hash: &[u8]) -> Result<()>;
    fn tokens(&self) -> Result<Vec<StoredToken>>;
    fn token_by_hash(&self, hash: &[u8]) -> Result<StoredToken>;
    fn revoke_token(&self, name: &str) -> Result<()>;
}

pub struct LocalStore {
    conn: Mutex<Connection>,
}
impl LocalStore {
    pub fn open(path: &Path) -> Result<Self> {
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let version: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > 2 {
            return Err(Error::Schema);
        }
        if version == 0 {
            let tx = conn.transaction()?;
            tx.execute_batch(include_str!("schema.sql"))?;
            tx.pragma_update(None, "user_version", 1)?;
            tx.commit()?;
        }
        if version < 2 {
            let tx = conn.transaction()?;
            tx.execute_batch("CREATE INDEX agent_token_hash ON agents(token_hash); CREATE INDEX named_token_hash ON tokens(hash);")?;
            tx.pragma_update(None, "user_version", 2)?;
            tx.commit()?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| Error::Poisoned)
    }

    /// Internal enrollment seam. Not exposed as an unauthenticated RPC.
    pub fn set_enrollment(&self, mode: &str, node: Option<&str>) -> Result<()> {
        if !matches!(mode, "standalone" | "allgather") {
            return Err(Error::Conflict("invalid operating mode"));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute("UPDATE host SET mode=?1, node_name=CASE WHEN ?1='standalone' THEN standalone_node ELSE COALESCE(?2,node_name) END WHERE singleton=1", params![mode, node])?;
        tx.commit()?;
        Ok(())
    }
    /// A complete received snapshot changes only the Allgather-owned table.
    pub fn apply_policy(&self, revision: u64, rules: &[Rule]) -> Result<bool> {
        let payload = serde_json::to_string(rules)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let old: Option<(String, String)> = tx
            .query_row(
                "SELECT revision,payload FROM policy_snapshot WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((v, previous)) = old {
            let v: u64 = v.parse().map_err(|_| Error::Schema)?;
            if revision < v {
                return Err(Error::Conflict("stale policy revision"));
            }
            if revision == v {
                return if previous == payload {
                    Ok(false)
                } else {
                    Err(Error::Conflict("conflicting policy revision"))
                };
            }
        }
        write_rules(&tx, "allgather_rules", rules)?;
        tx.execute("INSERT INTO policy_snapshot(singleton,revision,payload) VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET revision=excluded.revision,payload=excluded.payload", params![revision.to_string(), payload])?;
        tx.commit()?;
        Ok(true)
    }
    /// Used by the owned-session supervisor before spawn/recovery, not by discovery.
    pub fn set_recovery_pending(&self, id: &str, pending: bool) -> Result<()> {
        let conn = self.lock()?;
        if conn.execute(
            "UPDATE agents SET recovery_pending=?2 WHERE id=?1 AND ownership='owned'",
            params![id, pending],
        )? == 0
        {
            return Err(Error::NotFound);
        }
        Ok(())
    }
}

fn next_revision(tx: &Transaction<'_>) -> Result<u64> {
    let n: i64 = tx.query_row(
        "UPDATE counters SET value=value+1 WHERE name='registry' RETURNING value",
        [],
        |r| r.get(0),
    )?;
    u64::try_from(n).map_err(|_| Error::Schema)
}
fn stored_agent(conn: &Connection, id: &str) -> Result<StoredAgent> {
    let row = conn.query_row("SELECT public_json,token_hash,source_seq,ended_at,recovery_pending FROM agents WHERE id=?1", [id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, String>(2)?, r.get::<_, Option<i64>>(3)?, r.get::<_, bool>(4)?))
    }).optional()?.ok_or(Error::NotFound)?;
    Ok(StoredAgent {
        record: serde_json::from_str(&row.0)?,
        token_hash: row.1,
        seq: row.2.parse().map_err(|_| Error::Schema)?,
        ended_at: row
            .3
            .map(|n| u64::try_from(n).map_err(|_| Error::Schema))
            .transpose()?,
        recovery_pending: row.4,
    })
}
fn registration_target(conn: &Connection, candidate: &AgentRecord) -> Result<Option<StoredAgent>> {
    let native: Option<String> = if let Some(n) = &candidate.native_session_id {
        conn.query_row(
            "SELECT id FROM agents WHERE harness=?1 AND native_id=?2",
            params![candidate.harness, n],
            |r| r.get(0),
        )
        .optional()?
    } else {
        None
    };
    let process: Option<String> =
        if let (Some(pid), Some(start)) = (candidate.pid, candidate.pid_start_time) {
            conn.query_row(
                "SELECT id FROM agents WHERE pid=?1 AND pid_start=?2",
                params![pid, start.to_string()],
                |r| r.get(0),
            )
            .optional()?
        } else {
            None
        };
    if native.is_some() && process.is_some() && native != process {
        return Err(Error::Conflict(
            "native and process identities identify different sessions",
        ));
    }
    let Some(id) = native.or(process) else {
        return Ok(None);
    };
    let existing = stored_agent(conn, &id)?;
    if existing.record.harness != candidate.harness {
        return Err(Error::Conflict("process belongs to a different harness"));
    }
    if existing.record.native_session_id.is_some()
        && candidate.native_session_id.is_some()
        && existing.record.native_session_id != candidate.native_session_id
    {
        return Err(Error::Conflict(
            "process has a different native session identity",
        ));
    }
    Ok(Some(existing))
}

fn save_record(tx: &Transaction<'_>, record: &AgentRecord) -> Result<()> {
    tx.execute(
        "UPDATE agents SET public_json=?2 WHERE id=?1",
        params![record.id, serde_json::to_string(record)?],
    )?;
    Ok(())
}
fn all_records(conn: &Connection) -> Result<Vec<AgentRecord>> {
    let mut q = conn.prepare("SELECT public_json FROM agents ORDER BY id")?;
    let data = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    data.iter()
        .map(|s| serde_json::from_str(s).map_err(Error::from))
        .collect()
}
fn read_rules(conn: &Connection, table: &str) -> Result<Vec<Rule>> {
    // table names originate only from static call sites, never request input.
    let mut q = conn.prepare(&format!("SELECT data FROM {table} ORDER BY position"))?;
    let data = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    data.iter()
        .map(|s| serde_json::from_str(s).map_err(Error::from))
        .collect()
}
fn write_rules(tx: &Transaction<'_>, table: &str, rules: &[Rule]) -> Result<()> {
    tx.execute(&format!("DELETE FROM {table}"), [])?;
    for (i, r) in rules.iter().enumerate() {
        tx.execute(
            &format!("INSERT INTO {table}(id,position,data) VALUES(?1,?2,?3)"),
            params![r.id, i as i64, serde_json::to_string(r)?],
        )?;
    }
    Ok(())
}
fn host_info(conn: &Connection) -> Result<Host> {
    let row = conn
        .query_row(
            "SELECT host_id,node_name,mode FROM host WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
        .ok_or(Error::NotFound)?;
    let revision: Option<String> = conn
        .query_row(
            "SELECT revision FROM policy_snapshot WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(Host {
        host_id: row.0,
        node_name: row.1,
        mode: row.2,
        policy_revision: revision
            .map(|v| v.parse().map_err(|_| Error::Schema))
            .transpose()?,
    })
}
impl Store for LocalStore {
    fn initialize(&self, host_id: &str, node_name: &str) -> Result<Host> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute("INSERT OR IGNORE INTO host(singleton,host_id,standalone_node,node_name,mode) VALUES(1,?1,?2,?2,'standalone')",params![host_id,node_name])?;
        let host = host_info(&tx)?;
        if host.host_id != host_id {
            return Err(Error::Conflict("host_id file and database disagree"));
        }
        tx.commit()?;
        Ok(host)
    }
    fn host(&self) -> Result<Host> {
        host_info(&*self.lock()?)
    }
    fn registration_target(&self, candidate: &AgentRecord) -> Result<Option<StoredAgent>> {
        registration_target(&*self.lock()?, candidate)
    }
    fn register(
        &self,
        mut candidate: AgentRecord,
        hash: &[u8],
        expected_id: &str,
    ) -> Result<Registration> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let target = registration_target(&tx, &candidate)?;
        let resolved_id = target
            .as_ref()
            .map_or(candidate.id.as_str(), |a| &a.record.id);
        if resolved_id != expected_id {
            return Err(Error::Conflict(
                "registration target changed; retry registration",
            ));
        }
        if let Some(mut existing) = target {
            let id = existing.record.id.clone();
            if existing.record.ownership == "owned" {
                return Ok(Registration {
                    agent: existing,
                    created: false,
                });
            }
            existing.record.native_session_id = candidate
                .native_session_id
                .or(existing.record.native_session_id);
            // The caller supplies a newly observed or explicitly validated binding.
            existing.record.pid = candidate.pid;
            existing.record.pid_start_time = candidate.pid_start_time;
            if candidate.display_name.is_some() {
                existing.record.display_name = candidate.display_name;
            }
            existing.record.cwd = candidate.cwd;
            existing.record.last_seen = candidate.last_seen;
            existing.record.updated_at = candidate.updated_at;
            // Re-registration must not claim the previous process is still idle.
            existing.record.status = "unknown".into();
            existing.record.status_detail = None;
            for source in candidate.sources {
                if !existing.record.sources.contains(&source) {
                    existing.record.sources.push(source);
                }
            }
            existing.record.revision = next_revision(&tx)?;
            tx.execute(
                "UPDATE agents SET native_id=?2,pid=?3,pid_start=?4,ended_at=NULL WHERE id=?1",
                params![
                    id,
                    existing.record.native_session_id,
                    existing.record.pid,
                    existing.record.pid_start_time.map(|v| v.to_string())
                ],
            )?;
            existing.ended_at = None;
            save_record(&tx, &existing.record)?;
            tx.commit()?;
            return Ok(Registration {
                agent: existing,
                created: false,
            });
        }
        candidate.revision = next_revision(&tx)?;
        tx.execute("INSERT INTO agents(id,harness,native_id,pid,pid_start,ownership,public_json,token_hash,source_seq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'0')",params![candidate.id,candidate.harness,candidate.native_session_id,candidate.pid,candidate.pid_start_time.map(|v|v.to_string()),candidate.ownership,serde_json::to_string(&candidate)?,hash])?;
        tx.commit()?;
        Ok(Registration {
            agent: StoredAgent {
                record: candidate,
                token_hash: hash.into(),
                seq: 0,
                ended_at: None,
                recovery_pending: false,
            },
            created: true,
        })
    }
    fn agent(&self, id: &str) -> Result<StoredAgent> {
        stored_agent(&*self.lock()?, id)
    }
    fn agent_by_token_hash(&self, hash: &[u8]) -> Result<StoredAgent> {
        let conn = self.lock()?;
        let id: String = conn
            .query_row("SELECT id FROM agents WHERE token_hash=?1", [hash], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or(Error::NotFound)?;
        stored_agent(&conn, &id)
    }
    fn agent_by_native_id(&self, harness: &str, native_id: &str) -> Result<StoredAgent> {
        let conn = self.lock()?;
        let id: String = conn
            .query_row(
                "SELECT id FROM agents WHERE harness=?1 AND native_id=?2",
                params![harness, native_id],
                |r| r.get(0),
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        stored_agent(&conn, &id)
    }
    fn agents(&self, filter: &AgentFilter) -> Result<Vec<AgentRecord>> {
        Ok(all_records(&*self.lock()?)?
            .into_iter()
            .filter(|r| {
                (filter.include_exited
                    || filter.status.is_some()
                    || !matches!(r.status.as_str(), "exited" | "lost"))
                    && filter.harness.as_ref().is_none_or(|v| v == &r.harness)
                    && filter.status.as_ref().is_none_or(|v| v == &r.status)
                    && filter.ownership.as_ref().is_none_or(|v| v == &r.ownership)
                    && filter.cwd_prefix.as_ref().is_none_or(|p| {
                        r.cwd == *p
                            || r.cwd
                                .strip_prefix(p)
                                .is_some_and(|tail| p.ends_with('/') || tail.starts_with('/'))
                    })
            })
            .collect())
    }
    fn update(&self, update: &UpdateParams, now: u64) -> Result<UpdateResult> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, &update.id)?;
        if agent.ended_at.is_some() {
            return Err(Error::Conflict(
                "ended session requires re-registration or resume",
            ));
        }
        agent.record.last_seen = now;
        if update.seq <= agent.seq {
            save_record(&tx, &agent.record)?;
            tx.commit()?;
            return Ok(UpdateResult {
                applied: false,
                revision: agent.record.revision,
            });
        }
        if let Some(status) = &update.status {
            agent.record.status = status.clone();
        }
        match &update.display_name {
            Patch::Missing => {}
            Patch::Clear => agent.record.display_name = None,
            Patch::Set(v) => agent.record.display_name = Some(v.clone()),
        }
        match &update.detail {
            Patch::Missing => {}
            Patch::Clear => agent.record.status_detail = None,
            Patch::Set(Value::Object(fields)) => {
                let previous = agent
                    .record
                    .status_detail
                    .get_or_insert_with(|| Value::Object(Default::default()));
                if let Value::Object(out) = previous {
                    out.extend(fields.clone());
                } else {
                    *previous = Value::Object(fields.clone());
                }
            }
            Patch::Set(_) => return Err(Error::Conflict("detail must be an object or null")),
        }
        agent.record.updated_at = now;
        agent.record.revision = next_revision(&tx)?;
        let ended = matches!(agent.record.status.as_str(), "exited" | "lost").then_some(now);
        tx.execute(
            "UPDATE agents SET source_seq=?2,ended_at=?3 WHERE id=?1",
            params![
                update.id,
                update.seq.to_string(),
                ended
                    .map(|n| i64::try_from(n).map_err(|_| Error::Schema))
                    .transpose()?
            ],
        )?;
        save_record(&tx, &agent.record)?;
        tx.commit()?;
        Ok(UpdateResult {
            applied: true,
            revision: agent.record.revision,
        })
    }
    fn heartbeat(&self, id: &str, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        if agent.ended_at.is_some() {
            return Err(Error::Conflict("ended session requires re-registration"));
        }
        agent.record.last_seen = now;
        save_record(&tx, &agent.record)?;
        tx.commit()?;
        Ok(())
    }
    fn unregister(&self, id: &str, now: u64) -> Result<AgentRecord> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        if agent.ended_at.is_none() {
            agent.record.status = "exited".into();
            agent.record.updated_at = now;
            agent.record.last_seen = now;
            agent.record.revision = next_revision(&tx)?;
            tx.execute(
                "UPDATE agents SET ended_at=?2 WHERE id=?1",
                params![id, i64::try_from(now).map_err(|_| Error::Schema)?],
            )?;
            save_record(&tx, &agent.record)?;
        }
        tx.commit()?;
        Ok(agent.record)
    }
    fn mark_lost(&self, id: &str, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        if agent.ended_at.is_none() {
            agent.record.status = "lost".into();
            agent.record.updated_at = now;
            agent.record.revision = next_revision(&tx)?;
            tx.execute(
                "UPDATE agents SET ended_at=?2 WHERE id=?1",
                params![id, i64::try_from(now).map_err(|_| Error::Schema)?],
            )?;
            save_record(&tx, &agent.record)?;
        }
        tx.commit()?;
        Ok(())
    }
    fn mark_unconfirmed(&self, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        for mut r in all_records(&tx)? {
            if r.ownership == "registered" && !matches!(r.status.as_str(), "exited" | "lost") {
                r.status = "unknown".into();
                r.updated_at = now;
                r.revision = next_revision(&tx)?;
                save_record(&tx, &r)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    fn expire(&self, now: u64, owned_retention_ms: u64) -> Result<Vec<String>> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut removed = Vec::new();
        for r in all_records(&tx)? {
            let a = stored_agent(&tx, &r.id)?;
            let Some(ended) = a.ended_at else { continue };
            if a.recovery_pending || !matches!(r.status.as_str(), "exited" | "lost") {
                continue;
            }
            let retention = if r.ownership == "owned" {
                owned_retention_ms
            } else if r.status == "lost" {
                600_000
            } else {
                60_000
            };
            if now.saturating_sub(ended) >= retention {
                tx.execute("DELETE FROM agents WHERE id=?1", [&r.id])?;
                removed.push(r.id);
            }
        }
        tx.commit()?;
        Ok(removed)
    }
    fn rules(&self) -> Result<RulesView> {
        let conn = self.lock()?;
        Ok(RulesView {
            local_rules: read_rules(&conn, "local_rules")?,
            allgather_rules: read_rules(&conn, "allgather_rules")?,
            policy_revision: host_info(&conn)?.policy_revision,
        })
    }
    fn put_rule(&self, rule: Rule) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut rules = read_rules(&tx, "local_rules")?;
        if let Some(i) = rules.iter().position(|r| r.id == rule.id) {
            rules[i] = rule;
        } else {
            let i = rules
                .iter()
                .rposition(|r| r.from == "*" && r.to == "*" && r.action == "ask")
                .unwrap_or(rules.len());
            rules.insert(i, rule);
        }
        write_rules(&tx, "local_rules", &rules)?;
        tx.commit()?;
        Ok(())
    }
    fn remove_rule(&self, id: &str) -> Result<()> {
        let conn = self.lock()?;
        if conn.execute("DELETE FROM local_rules WHERE id=?1", [id])? == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
    fn replace_rules(&self, rules: &[Rule]) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        write_rules(&tx, "local_rules", rules)?;
        tx.commit()?;
        Ok(())
    }
    fn issue_token(&self, info: TokenInfo, hash: &[u8]) -> Result<()> {
        let conn = self.lock()?;
        if conn
            .query_row("SELECT 1 FROM tokens WHERE name=?1", [&info.name], |r| {
                r.get::<_, i32>(0)
            })
            .optional()?
            .is_some()
        {
            return Err(Error::Conflict(
                "token name already exists; revoke it first",
            ));
        }
        conn.execute(
            "INSERT INTO tokens(name,data,hash) VALUES(?1,?2,?3)",
            params![info.name, serde_json::to_string(&info)?, hash],
        )?;
        Ok(())
    }
    fn tokens(&self) -> Result<Vec<StoredToken>> {
        let conn = self.lock()?;
        let mut q = conn.prepare("SELECT data,hash FROM tokens ORDER BY name")?;
        let data = q
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        data.into_iter()
            .map(|(s, hash)| {
                Ok(StoredToken {
                    info: serde_json::from_str(&s)?,
                    hash,
                })
            })
            .collect()
    }
    fn revoke_token(&self, name: &str) -> Result<()> {
        let conn = self.lock()?;
        if conn.execute("DELETE FROM tokens WHERE name=?1", [name])? == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
    fn token_by_hash(&self, hash: &[u8]) -> Result<StoredToken> {
        let conn = self.lock()?;
        let (data, hash): (String, Vec<u8>) = conn
            .query_row("SELECT data,hash FROM tokens WHERE hash=?1", [hash], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?
            .ok_or(Error::NotFound)?;
        Ok(StoredToken {
            info: serde_json::from_str(&data)?,
            hash,
        })
    }
}
#[cfg(test)]
mod tests;
