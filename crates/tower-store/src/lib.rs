//! Shared SQLite storage with separate registry and execution persistence APIs.

mod database;
pub use database::{Database, changes, table_changes, touch};
mod messaging;
mod vcs;
pub use messaging::{DeliveryState, StoredApproval};
pub use vcs::{StoredCheckout, StoredVcsOperation};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, MutexGuard},
};
use tower_protocol::{
    AgentFilter, AgentRecord, Patch, Rule, RulesView, TokenInfo, UpdateParams, UpdateResult, Value,
};
use tower_sessions::{ManagedSession, Observation};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage I/O failure: {0}")]
    Io(#[from] std::io::Error),
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
pub struct Store {
    database: Arc<Database>,
}

/// Returns whether the stored observation changed.
fn observe_status(tx: &Transaction<'_>, record: &mut AgentRecord, status: &str) -> Result<bool> {
    let (previous, stopping): (Option<String>, bool) = tx.query_row(
        "SELECT observed_status,stop_requested FROM agents WHERE id=?1",
        [&record.id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let previous = previous.as_deref().unwrap_or(&record.status);
    let observed = status != "unknown" && previous != status;
    if observed {
        if let Some(needed) = tower_sessions::attention::on_status(previous, status, stopping) {
            record.needs_attention = needed;
        }
        tx.execute(
            "UPDATE agents SET observed_status=?2 WHERE id=?1",
            params![record.id, status],
        )?;
    }
    record.status = status.into();
    Ok(observed)
}
fn new_error(before: &AgentRecord, after: &AgentRecord) -> bool {
    fn error(record: &AgentRecord) -> Option<&str> {
        if let Some(Value::Object(fields)) = &record.status_detail
            && let Some(Value::String(error)) = fields.get("error")
        {
            return Some(error);
        }
        None
    }
    tower_sessions::attention::on_error(error(before), error(after))
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::new(Arc::new(Database::open(path)?)))
    }
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.database.lock()
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
        tx.execute("INSERT INTO policy_snapshot(singleton,revision,payload) VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,delta_hash=NULL", params![revision.to_string(), payload])?;
        tx.commit()?;
        Ok(true)
    }
    pub fn apply_policy_delta(&self, delta: &tower_protocol::PolicyDelta) -> Result<bool> {
        use std::collections::{BTreeMap, BTreeSet};
        if delta.revision <= delta.base_revision
            || delta.upserts.len() > 1024
            || delta.removed.len() > 1024
            || delta.order.len() > 1024
        {
            return Err(Error::Conflict("invalid policy delta bounds"));
        }
        let digest = Sha256::digest(serde_json::to_vec(delta)?).to_vec();
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let current: Option<(String, String, Option<Vec<u8>>)> = tx
            .query_row(
                "SELECT revision,payload,delta_hash FROM policy_snapshot WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((revision, payload, previous_digest)) = current else {
            return Err(Error::Conflict("policy delta base revision mismatch"));
        };
        let revision: u64 = revision.parse().map_err(|_| Error::Schema)?;
        if revision == delta.revision {
            return if previous_digest.as_ref() == Some(&digest) {
                Ok(false)
            } else {
                Err(Error::Conflict("conflicting policy revision"))
            };
        }
        if revision != delta.base_revision {
            return Err(Error::Conflict("policy delta base revision mismatch"));
        }
        let previous: Vec<Rule> = serde_json::from_str(&payload)?;
        let mut rules: BTreeMap<_, _> = previous.into_iter().map(|r| (r.id.clone(), r)).collect();
        let mut changed = BTreeSet::new();
        for id in &delta.removed {
            if !changed.insert(id) {
                return Err(Error::Conflict("duplicate policy delta identity"));
            }
            rules.remove(id);
        }
        for rule in &delta.upserts {
            if !changed.insert(&rule.id) {
                return Err(Error::Conflict("duplicate policy delta identity"));
            }
            rules.insert(rule.id.clone(), rule.clone());
        }
        let mut ordered = Vec::new();
        for id in &delta.order {
            ordered.push(
                rules
                    .remove(id)
                    .ok_or(Error::Conflict("invalid policy delta order"))?,
            );
        }
        if !rules.is_empty() {
            return Err(Error::Conflict("incomplete policy delta order"));
        }
        write_rules(&tx, "allgather_rules", &ordered)?;
        tx.execute(
            "UPDATE policy_snapshot SET revision=?1,payload=?2,delta_hash=?3 WHERE singleton=1",
            params![
                delta.revision.to_string(),
                serde_json::to_string(&ordered)?,
                digest
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }
}

fn disable_delivery(caps: &mut tower_protocol::Capabilities, reason: &str) {
    caps.kill = false;
    caps.message = "none".into();
    caps.message_reason = Some(reason.into());
    caps.wake_when_idle = false;
    caps.reply_path = false;
    caps.steer = false;
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
            let mut query =
                conn.prepare("SELECT id FROM agents WHERE pid=?1 AND pid_start=?2 ORDER BY id")?;
            let ids = query
                .query_map(params![pid, start.to_string()], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut matches = Vec::new();
            for id in ids {
                let record = stored_agent(conn, &id)?.record;
                if candidate.shared_process
                    && record.shared_process
                    && candidate.harness == record.harness
                    && candidate.native_session_id.is_some()
                    && record.native_session_id.is_some()
                    && candidate.native_session_id != record.native_session_id
                {
                    continue;
                }
                matches.push(id);
            }
            if matches.len() > 1 {
                return Err(Error::Conflict(
                    "process hosts multiple sessions; provide a native session identity",
                ));
            }
            matches.pop()
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
impl Store {
    pub fn update(&self, update: &UpdateParams, now: u64) -> Result<UpdateResult> {
        self.apply_update(update, now, None, None)
    }
    /// As `update`, for a source that repeats what it said before, such as a
    /// hook on every tool call. A report that changes nothing is not written.
    /// Once the record has not been seen for `heartbeat` milliseconds only
    /// its `last_seen` is, without a new revision. Such a report is not
    /// applied and its sequence number stays unused.
    pub fn update_repeating(
        &self,
        update: &UpdateParams,
        now: u64,
        heartbeat: u64,
    ) -> Result<UpdateResult> {
        self.apply_update(update, now, None, Some(heartbeat))
    }
    pub fn update_with_delivery(
        &self,
        update: &UpdateParams,
        now: u64,
        delivery: Option<DeliveryState>,
    ) -> Result<UpdateResult> {
        self.apply_update(update, now, delivery, None)
    }
    pub fn clear_policy(&self) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM allgather_rules", [])?;
        tx.execute("DELETE FROM policy_snapshot", [])?;
        tx.commit()?;
        Ok(())
    }

    pub fn create_managed(
        &self,
        mut record: AgentRecord,
        hash: &[u8],
        session: &ManagedSession,
    ) -> Result<()> {
        if record.ownership != "owned"
            || record.id != session.id
            || record.protocol.as_deref() != Some(&session.protocol)
        {
            return Err(Error::Conflict(
                "managed reservation and backend binding disagree",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        record.revision = next_revision(&tx)?;
        tx.execute("INSERT INTO agents(id,harness,ownership,public_json,token_hash,source_seq,recovery_pending,native_id) VALUES(?1,?2,'owned',?3,?4,'0',1,?5)", params![record.id, record.harness, serde_json::to_string(&record)?, hash,record.native_session_id])?;
        tx.execute(
            "INSERT INTO managed_sessions(id,protocol,binding,launch) VALUES(?1,?2,?3,?4)",
            params![
                session.id,
                session.protocol,
                serde_json::to_string(&session.binding)?,
                session
                    .launch
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn managed_session(&self, id: &str) -> Result<ManagedSession> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT protocol,binding,launch,auto_resume FROM managed_sessions WHERE id=?1",
                [id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        Ok(ManagedSession {
            id: id.into(),
            protocol: row.0,
            binding: serde_json::from_str(&row.1)?,
            launch: row.2.map(|v| serde_json::from_str(&v)).transpose()?,
            auto_resume: row.3,
        })
    }
    pub fn managed_sessions(&self) -> Result<Vec<ManagedSession>> {
        let conn = self.lock()?;
        let mut query = conn.prepare("SELECT m.id,m.protocol,m.binding,m.launch,m.auto_resume FROM managed_sessions m JOIN agents a ON a.id=m.id WHERE a.ended_at IS NULL")?;
        let rows = query.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, bool>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (id, protocol, binding, launch, auto_resume) = row?;
            Ok(ManagedSession {
                id,
                protocol,
                binding: serde_json::from_str(&binding)?,
                launch: launch.map(|v| serde_json::from_str(&v)).transpose()?,
                auto_resume,
            })
        })
        .collect()
    }
    pub fn observe_managed(&self, protocol: &str, o: Observation, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let id = &o.id;
        let mut stored = stored_agent(&tx, id)?;
        if stored.record.ownership != "owned" || stored.record.protocol.as_deref() != Some(protocol)
        {
            return Err(Error::Conflict("observation belongs to another backend"));
        }
        if !matches!(
            o.status.as_str(),
            "starting"
                | "idle"
                | "working"
                | "waiting_input"
                | "waiting_approval"
                | "unknown"
                | "exited"
                | "lost"
        ) {
            return Err(Error::Conflict("invalid session status"));
        }
        if stored.ended_at.is_some() {
            return Ok(());
        }
        let ended = matches!(o.status.as_str(), "exited" | "lost");
        let before = stored.record.clone();
        let record = &mut stored.record;
        if !ended || o.pid.is_some() {
            record.pid = o.pid;
            record.pid_start_time = o.pid_start_time;
        }
        record.pool_session = o.terminal.as_ref().map(|t| t.pool_session.clone());
        record.attach_count = o.terminal.as_ref().map_or(0, |t| t.attach_count);
        record.caps = o.caps;
        if let Some(shared) = o.metadata.shared_process {
            record.shared_process = shared;
        }
        observe_status(&tx, record, &o.status)?;
        let mut native_conflict = false;
        if let Some(native) = o.metadata.native_session_id {
            let other: Option<String> = tx
                .query_row(
                    "SELECT id FROM agents WHERE harness=?1 AND native_id=?2 AND id<>?3",
                    params![record.harness, native, id],
                    |r| r.get(0),
                )
                .optional()?;
            if other.is_some() {
                native_conflict = true;
                record.native_session_id = None;
            } else {
                record.native_session_id = Some(native);
            }
        }
        match o.metadata.display_name {
            Patch::Missing => {}
            Patch::Clear => record.display_name = None,
            Patch::Set(name) => record.display_name = Some(name),
        }
        if let Some(cwd) = o.metadata.cwd {
            record.cwd = cwd;
        }
        if let Some(detail) = o.metadata.detail {
            record.status_detail = Some(detail);
        }
        if native_conflict {
            record.caps.resume = false;
            tx.execute(
                "UPDATE managed_sessions SET auto_resume=0 WHERE id=?1",
                [id],
            )?;
            let mut detail = match record.status_detail.take() {
                Some(tower_protocol::Value::Object(map)) => map,
                _ => Default::default(),
            };
            detail.insert(
                "error".into(),
                tower_protocol::Value::String("native_identity_conflict".into()),
            );
            record.status_detail = Some(tower_protocol::Value::Object(detail));
        }
        if new_error(&before, record) {
            record.needs_attention = true;
        }
        if *record != before {
            record.updated_at = now;
            record.revision = next_revision(&tx)?;
        } else if !ended
            && o.binding.is_none()
            && stored.recovery_pending == o.recovery_pending
            && now.saturating_sub(record.last_seen) < 10_000
        {
            return Ok(());
        }
        if !ended {
            record.last_seen = now;
        }
        tx.execute(
            "UPDATE agents SET pid=?2,pid_start=?3,ended_at=?4,recovery_pending=?5,native_id=?6 WHERE id=?1",
            params![
                id,
                record.pid,
                record.pid_start_time.map(|n| n.to_string()),
                ended.then_some(i64::try_from(now).map_err(|_| Error::Schema)?),
                o.recovery_pending,
                record.native_session_id
            ],
        )?;
        if let Some(binding) = o.binding {
            tx.execute(
                "UPDATE managed_sessions SET binding=?2 WHERE id=?1 AND protocol=?3",
                params![id, serde_json::to_string(&binding)?, protocol],
            )?;
        }
        save_record(&tx, record)?;
        tx.commit()?;
        Ok(())
    }
    pub fn disable_managed_recovery(&self, id: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE managed_sessions SET auto_resume=0 WHERE id=?1",
            [id],
        )?;
        tx.execute(
            "UPDATE agents SET recovery_pending=0,stop_requested=1 WHERE id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn replace_managed(
        &self,
        session: &ManagedSession,
        mode: Option<&str>,
        now: u64,
    ) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut stored = stored_agent(&tx, &session.id)?;
        if stored.record.ownership != "owned"
            || stored.record.protocol.as_deref() != Some(&session.protocol)
        {
            return Err(Error::Conflict("backend identity mismatch"));
        }
        tx.execute(
            "UPDATE managed_sessions SET binding=?2,launch=?3,auto_resume=?4 WHERE id=?1",
            params![
                session.id,
                serde_json::to_string(&session.binding)?,
                session
                    .launch
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                session.auto_resume
            ],
        )?;
        tx.execute(
            "UPDATE agents SET stop_requested=0 WHERE id=?1",
            [&session.id],
        )?;
        observe_status(&tx, &mut stored.record, "starting")?;
        stored.record.pid = None;
        stored.record.pid_start_time = None;
        stored.record.mode = mode.map(str::to_owned);
        stored.record.caps = tower_protocol::Capabilities::unavailable("starting");
        stored.record.updated_at = now;
        stored.record.revision = next_revision(&tx)?;
        tx.execute("UPDATE agents SET pid=NULL,pid_start=NULL,ended_at=NULL,recovery_pending=1 WHERE id=?1",[&session.id])?;
        save_record(&tx, &stored.record)?;
        tx.commit()?;
        Ok(())
    }
    pub fn initialize(&self, host_id: &str, node_name: &str) -> Result<Host> {
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
    pub fn host(&self) -> Result<Host> {
        host_info(&*self.lock()?)
    }
    pub fn registration_target(&self, candidate: &AgentRecord) -> Result<Option<StoredAgent>> {
        registration_target(&*self.lock()?, candidate)
    }
    pub fn register(
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
            existing.record.caps.kill = candidate.caps.kill;
            existing.record.shared_process = candidate.shared_process;
            existing.record.mode = candidate.mode.or(existing.record.mode);
            if candidate.display_name.is_some() {
                existing.record.display_name = candidate.display_name;
            }
            existing.record.protocol = candidate.protocol.or(existing.record.protocol);
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
                "UPDATE agents SET native_id=?2,pid=?3,pid_start=?4,ended_at=NULL,stop_requested=0,observed_status=NULL WHERE id=?1",
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
    pub fn agent(&self, id: &str) -> Result<StoredAgent> {
        stored_agent(&*self.lock()?, id)
    }
    pub fn agent_by_token_hash(&self, hash: &[u8]) -> Result<StoredAgent> {
        let conn = self.lock()?;
        let id: String = conn
            .query_row("SELECT id FROM agents WHERE token_hash=?1", [hash], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or(Error::NotFound)?;
        stored_agent(&conn, &id)
    }
    pub fn agent_by_native_id(&self, harness: &str, native_id: &str) -> Result<StoredAgent> {
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
    pub fn agents(&self, filter: &AgentFilter) -> Result<Vec<AgentRecord>> {
        Ok(all_records(&*self.lock()?)?
            .into_iter()
            .filter(|r| {
                (filter.include_exited
                    || filter.status.is_some()
                    || !matches!(r.status.as_str(), "exited" | "lost"))
                    && filter.harness.as_ref().is_none_or(|v| v == &r.harness)
                    && filter.status.as_ref().is_none_or(|v| v == &r.status)
                    && filter.ownership.as_ref().is_none_or(|v| v == &r.ownership)
                    && filter
                        .protocol
                        .as_ref()
                        .is_none_or(|v| r.protocol.as_ref() == Some(v))
                    && filter.cwd_prefix.as_ref().is_none_or(|p| {
                        r.cwd == *p
                            || r.cwd
                                .strip_prefix(p)
                                .is_some_and(|tail| p.ends_with('/') || tail.starts_with('/'))
                    })
            })
            .collect())
    }
    fn apply_update(
        &self,
        update: &UpdateParams,
        now: u64,
        delivery: Option<DeliveryState>,
        heartbeat: Option<u64>,
    ) -> Result<UpdateResult> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, &update.id)?;
        if agent.ended_at.is_some() {
            return Err(Error::Conflict(
                "ended session requires re-registration or resume",
            ));
        }
        let seen = agent.record.last_seen;
        agent.record.last_seen = now;
        if update.seq <= agent.seq {
            save_record(&tx, &agent.record)?;
            tx.commit()?;
            return Ok(UpdateResult {
                applied: false,
                revision: agent.record.revision,
            });
        }
        if !update.delivery.is_missing() && delivery.is_none() {
            return Err(Error::Conflict(
                "delivery update requires adapter validation",
            ));
        }
        if let Some(delivery) = delivery {
            messaging::apply_delivery(&tx, &mut agent.record, &delivery.endpoint, delivery.caps)?;
        }
        let before = agent.record.clone();
        let mut observed = false;
        if let Some(status) = &update.status {
            observed = observe_status(&tx, &mut agent.record, status)?;
            if matches!(status.as_str(), "exited" | "lost") {
                disable_delivery(&mut agent.record.caps, "session_ended");
            }
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
        if new_error(&before, &agent.record) {
            agent.record.needs_attention = true;
        }
        if let Some(heartbeat) = heartbeat
            && !observed
            && agent.record == before
        {
            // Dropping the transaction leaves the record as it was.
            if now.saturating_sub(seen) >= heartbeat {
                save_record(&tx, &agent.record)?;
                tx.commit()?;
            }
            return Ok(UpdateResult {
                applied: false,
                revision: agent.record.revision,
            });
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
    pub fn edit_session(
        &self,
        id: &str,
        add: &[String],
        remove: &[String],
        clear_attention: bool,
        now: u64,
    ) -> Result<AgentRecord> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut record = stored_agent(&tx, id)?.record;
        let before = record.clone();
        tower_sessions::tags::patch(&mut record.tags, add, remove).map_err(Error::Conflict)?;
        if clear_attention {
            record.needs_attention = false;
        }
        if record != before {
            record.updated_at = now;
            record.revision = next_revision(&tx)?;
            save_record(&tx, &record)?;
        }
        tx.commit()?;
        Ok(record)
    }
    pub fn expect_stop(&self, id: &str) -> Result<()> {
        self.lock()?
            .execute("UPDATE agents SET stop_requested=1 WHERE id=?1", [id])?;
        Ok(())
    }
    pub fn heartbeat(&self, id: &str, now: u64) -> Result<()> {
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
    pub fn unregister(&self, id: &str, now: u64) -> Result<AgentRecord> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        // The credential holder's report outranks exit detection that marked it lost first.
        if agent.ended_at.is_none() || agent.record.status == "lost" {
            tx.execute("UPDATE agents SET stop_requested=1 WHERE id=?1", [id])?;
            observe_status(&tx, &mut agent.record, "exited")?;
            disable_delivery(&mut agent.record.caps, "session_ended");
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
    /// Moves a live registered process to a new native session, as when the
    /// harness starts a fresh conversation without restarting.
    pub fn rebind_native(&self, id: &str, native: &str, now: u64) -> Result<AgentRecord> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        if agent.ended_at.is_some() || agent.record.ownership == "owned" {
            return Err(Error::Conflict("only a live registered session can rebind"));
        }
        let taken: Option<String> = tx
            .query_row(
                "SELECT id FROM agents WHERE harness=?1 AND native_id=?2",
                params![agent.record.harness, native],
                |r| r.get(0),
            )
            .optional()?;
        if taken.is_some_and(|other| other != id) {
            return Err(Error::Conflict(
                "native identity belongs to another session",
            ));
        }
        agent.record.native_session_id = Some(native.into());
        agent.record.updated_at = now;
        agent.record.revision = next_revision(&tx)?;
        tx.execute(
            "UPDATE agents SET native_id=?2 WHERE id=?1",
            params![id, native],
        )?;
        save_record(&tx, &agent.record)?;
        tx.commit()?;
        Ok(agent.record)
    }
    pub fn mark_lost(&self, id: &str, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let mut agent = stored_agent(&tx, id)?;
        if agent.ended_at.is_none() {
            observe_status(&tx, &mut agent.record, "lost")?;
            disable_delivery(&mut agent.record.caps, "session_lost");
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
    pub fn mark_unconfirmed(&self, now: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        for mut r in all_records(&tx)? {
            if !matches!(r.status.as_str(), "exited" | "lost") {
                if r.ownership == "owned" {
                    r.caps = tower_protocol::Capabilities::unavailable("backend_unavailable");
                }
                r.status = "unknown".into();
                r.updated_at = now;
                r.revision = next_revision(&tx)?;
                save_record(&tx, &r)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn expire(&self, now: u64, owned_retention_ms: u64) -> Result<Vec<String>> {
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
    /// The earliest time at which `expire`, `expiring_managed`, `expire_approvals`,
    /// `expire_vcs_plans`, or the unseen-registration rule has work to do.
    pub fn next_deadline(&self, owned_retention_ms: u64) -> Result<Option<u64>> {
        let conn = self.lock()?;
        let mut next: Option<u64> = None;
        let mut consider = |at: u64| next = Some(next.map_or(at, |n| n.min(at)));
        for r in all_records(&conn)? {
            let a = stored_agent(&conn, &r.id)?;
            match a.ended_at {
                Some(ended) if !a.recovery_pending => {
                    if matches!(r.status.as_str(), "exited" | "lost") {
                        consider(ended.saturating_add(if r.ownership == "owned" {
                            owned_retention_ms
                        } else if r.status == "lost" {
                            600_000
                        } else {
                            60_000
                        }));
                    }
                }
                Some(_) => {}
                None => {
                    if r.ownership == "registered"
                        && (r.pid.is_none() || r.pid_start_time.is_none())
                    {
                        consider(r.last_seen.saturating_add(180_000));
                    }
                }
            }
        }
        let min = |sql: &str| -> Result<Option<u64>> {
            Ok(conn
                .query_row(sql, [], |r| r.get::<_, Option<i64>>(0))?
                .and_then(|n| u64::try_from(n).ok()))
        };
        for at in [
            min("SELECT MIN(json_extract(data,'$.expires_at')) FROM approvals WHERE json_extract(data,'$.state')='pending'")?,
            min("SELECT MIN(json_extract(record,'$.expires_at')) FROM vcs_operations WHERE state='prepared'")?,
            min("SELECT MIN(json_extract(record,'$.created_at')) FROM vcs_operations WHERE state IN ('completed','failed')")?
                .map(|at| at.saturating_add(30 * 86_400_000)),
        ]
        .into_iter()
        .flatten()
        {
            consider(at);
        }
        Ok(next)
    }
    pub fn expiring_managed(&self, now: u64, retention_ms: u64) -> Result<Vec<ManagedSession>> {
        if now < retention_ms {
            return Ok(Vec::new());
        }
        let cutoff = now.saturating_sub(retention_ms);
        let ids = {
            let conn = self.lock()?;
            let mut query=conn.prepare("SELECT a.id FROM agents a JOIN managed_sessions m ON a.id=m.id WHERE a.ended_at IS NOT NULL AND a.ended_at<=?1 AND a.recovery_pending=0")?;
            query
                .query_map([i64::try_from(cutoff).map_err(|_| Error::Schema)?], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        ids.iter().map(|id| self.managed_session(id)).collect()
    }
    pub fn rules(&self) -> Result<RulesView> {
        let conn = self.lock()?;
        Ok(RulesView {
            local_rules: read_rules(&conn, "local_rules")?,
            allgather_rules: read_rules(&conn, "allgather_rules")?,
            policy_revision: host_info(&conn)?.policy_revision,
        })
    }
    pub fn put_rule(&self, rule: Rule) -> Result<()> {
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
    pub fn remove_rule(&self, id: &str) -> Result<()> {
        let conn = self.lock()?;
        if conn.execute("DELETE FROM local_rules WHERE id=?1", [id])? == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
    pub fn replace_rules(&self, rules: &[Rule]) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        write_rules(&tx, "local_rules", rules)?;
        tx.commit()?;
        Ok(())
    }
    pub fn issue_token(&self, info: TokenInfo, hash: &[u8]) -> Result<()> {
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
    pub fn tokens(&self) -> Result<Vec<StoredToken>> {
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
    pub fn revoke_token(&self, name: &str) -> Result<()> {
        let conn = self.lock()?;
        if conn.execute("DELETE FROM tokens WHERE name=?1", [name])? == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
    pub fn token_by_hash(&self, hash: &[u8]) -> Result<StoredToken> {
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
