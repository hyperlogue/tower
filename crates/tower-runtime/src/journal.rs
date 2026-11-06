use crate::{Result, error, identifier, internal, millis, parse_time, random_id, timestamp};
use hmac::{Hmac, KeyInit, Mac};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};
use tower_protocol::{
    ExecutionAcceptance, ExecutionCommand, ExecutionDispatch, ExecutionError, ExecutionReceipt,
    ExecutionResult, ExecutionStatus, RuntimeCursor, RuntimeEvent, RuntimeEvents, RuntimeLoss,
    Value,
};

pub use tower_sessions::execution::RuntimeBinding as Binding;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub runtime_spool_bytes: u64,
    pub host_spool_bytes: u64,
    pub control_reserve_bytes: u64,
    pub receipt_count: u64,
    pub receipt_retention_days: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            runtime_spool_bytes: 64 * 1024 * 1024,
            host_spool_bytes: 256 * 1024 * 1024,
            control_reserve_bytes: 1024 * 1024,
            receipt_count: 1_000_000,
            receipt_retention_days: 30,
        }
    }
}
pub struct Journal {
    conn: Mutex<Connection>,
    digest_key: Vec<u8>,
    limits: Limits,
}
impl Journal {
    pub fn bulk_capacity(&self, binding: &Binding, bytes: usize) -> Result<bool> {
        let conn = self.lock()?;
        let used:i64=conn.query_row("SELECT spool_bytes FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation],|r|r.get(0)).map_err(internal)?;
        let host: i64 = conn
            .query_row("SELECT spool_bytes FROM counters WHERE id=1", [], |r| {
                r.get(0)
            })
            .map_err(internal)?;
        Ok(
            (used as u64).saturating_add(bytes as u64) <= self.limits.runtime_spool_bytes
                && (host as u64).saturating_add(bytes as u64) <= self.limits.host_spool_bytes,
        )
    }
    pub fn open(path: &Path, limits: Limits) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| error("runtime_storage", "Invalid runtime journal path"))?;
        tower_pal::private_dir(parent).map_err(internal)?;
        let existing = path.metadata().is_ok_and(|m| m.len() > 0);
        let marker_path = parent.join("journal.id");
        let marker = match tower_pal::read_private(&marker_path) {
            Ok(v) => Some(v),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(internal(e)),
        };
        if marker.is_some() && !existing {
            return Err(error(
                "journal_lost",
                "Restore the runtime journal; its identity cannot be reset",
            ));
        }
        tower_pal::private_file(path, true).map_err(internal)?;
        let key_file = parent.join("fingerprint.key");
        let key = match tower_pal::read_private(&key_file) {
            Ok(key) => key,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if existing {
                    return Err(error(
                        "runtime_storage",
                        "Restore the runtime fingerprint key with its journal",
                    ));
                }
                let mut key = vec![0; 32];
                getrandom::fill(&mut key).map_err(internal)?;
                tower_pal::write_private(&key_file, &key).map_err(internal)?;
                key
            }
            Err(e) => return Err(internal(e)),
        };
        if key.len() != 32 {
            return Err(error("runtime_storage", "Invalid fingerprint key"));
        }
        let conn = Connection::open(path).map_err(internal)?;
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(internal)?;
        if version > 1 {
            return Err(error(
                "runtime_storage",
                "Runtime journal belongs to a newer version",
            ));
        }

        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(internal)?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS runtimes(namespace TEXT NOT NULL,runtime_id TEXT NOT NULL,generation TEXT NOT NULL,logical_session TEXT NOT NULL,tower_id TEXT NOT NULL,owner_id TEXT NOT NULL,binding TEXT NOT NULL,state TEXT NOT NULL,metadata TEXT NOT NULL DEFAULT '{}',fenced INTEGER NOT NULL DEFAULT 0,termination TEXT NOT NULL DEFAULT 'unknown',next_sequence INTEGER NOT NULL DEFAULT 1,ack_sequence INTEGER NOT NULL DEFAULT 0,ack_key TEXT,coverage TEXT NOT NULL DEFAULT 'complete',spool_bytes INTEGER NOT NULL DEFAULT 0,control_bytes INTEGER NOT NULL DEFAULT 0,loss TEXT,loss_ack INTEGER NOT NULL DEFAULT 0,ended_at INTEGER,PRIMARY KEY(namespace,runtime_id),UNIQUE(namespace,runtime_id,generation));
CREATE TABLE IF NOT EXISTS commands(namespace TEXT NOT NULL,command_id TEXT NOT NULL,runtime_id TEXT NOT NULL,generation TEXT NOT NULL,fingerprint BLOB NOT NULL,kind TEXT NOT NULL,payload TEXT,expires_at INTEGER NOT NULL,receipt TEXT NOT NULL,receipt_revision INTEGER NOT NULL,ack_revision INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,PRIMARY KEY(namespace,command_id),FOREIGN KEY(namespace,runtime_id,generation) REFERENCES runtimes(namespace,runtime_id,generation));
CREATE TABLE IF NOT EXISTS events(namespace TEXT NOT NULL,runtime_id TEXT NOT NULL,generation TEXT NOT NULL,sequence INTEGER NOT NULL,event_key TEXT NOT NULL,body TEXT NOT NULL,bytes INTEGER NOT NULL,control INTEGER NOT NULL,PRIMARY KEY(namespace,runtime_id,generation,sequence),UNIQUE(namespace,runtime_id,generation,event_key),FOREIGN KEY(namespace,runtime_id,generation) REFERENCES runtimes(namespace,runtime_id,generation));
CREATE TABLE IF NOT EXISTS counters(id INTEGER PRIMARY KEY CHECK(id=1),spool_bytes INTEGER NOT NULL DEFAULT 0,control_bytes INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS journal_identity(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS takeovers(namespace TEXT NOT NULL,action_id TEXT NOT NULL,command_id TEXT NOT NULL,runtime_id TEXT NOT NULL,generation TEXT NOT NULL,PRIMARY KEY(namespace,action_id));
CREATE INDEX IF NOT EXISTS commands_runtime ON commands(namespace,runtime_id,generation);
CREATE INDEX IF NOT EXISTS command_outbox ON commands(namespace,created_at) WHERE receipt_revision>ack_revision;
CREATE INDEX IF NOT EXISTS pending_payloads ON commands(namespace,runtime_id) WHERE payload IS NOT NULL;
CREATE INDEX IF NOT EXISTS runtimes_logical ON runtimes(namespace,logical_session);
CREATE INDEX IF NOT EXISTS runtimes_ended ON runtimes(ended_at) WHERE ended_at IS NOT NULL;
INSERT OR IGNORE INTO counters(id) VALUES(1);
PRAGMA user_version=1;").map_err(internal)?;
        let identity: Option<String> = conn
            .query_row("SELECT id FROM journal_identity", [], |r| r.get(0))
            .optional()
            .map_err(internal)?;
        match (marker, identity) {
            (Some(marker), Some(id)) if marker == id.as_bytes() => {}
            (None, None) => {
                let id = random_id("journal_")?;
                conn.execute("INSERT INTO journal_identity(id) VALUES(?1)", [&id])
                    .map_err(internal)?;
                tower_pal::write_private(&marker_path, id.as_bytes()).map_err(internal)?;
            }
            _ => {
                return Err(error(
                    "journal_lost",
                    "Runtime journal identity changed; restore its matching identity file",
                ));
            }
        }
        Ok(Self {
            conn: Mutex::new(conn),
            digest_key: key,
            limits,
        })
    }
    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(internal)
    }
    fn fingerprint(&self, command: &ExecutionCommand) -> Result<Vec<u8>> {
        let value = serde_json::to_value(command).map_err(internal)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.digest_key).map_err(internal)?;
        mac.update(&serde_json::to_vec(&value).map_err(internal)?);
        Ok(mac.finalize().into_bytes().to_vec())
    }
    pub fn admit(&self, binding: &Binding, command: &ExecutionCommand) -> Result<ExecutionReceipt> {
        validate(binding, command)?;
        let fingerprint = self.fingerprint(command)?;
        let namespace = binding.namespace();
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let existing: Option<(Vec<u8>, String)> = tx
            .query_row(
                "SELECT fingerprint,receipt FROM commands WHERE namespace=?1 AND command_id=?2",
                params![namespace, command.command_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(internal)?;
        if let Some((old, receipt)) = existing {
            if old != fingerprint {
                return Err(error(
                    "command_conflict",
                    "Command ID was already used for different input",
                ));
            }
            return serde_json::from_str(&receipt).map_err(internal);
        }
        if parse_time(&command.expires_at)? <= millis() {
            return Err(error(
                "command_expired",
                "Command admission deadline expired",
            ));
        }
        let count: i64 = tx
            .query_row("SELECT count(*) FROM commands", [], |r| r.get(0))
            .map_err(internal)?;
        if count as u64 >= self.limits.receipt_count {
            return Err(error(
                "runtime_capacity",
                "Command receipt capacity exhausted",
            ));
        }
        let runtime:Option<(String,String,bool)>=tx.query_row("SELECT generation,binding,fenced FROM runtimes WHERE namespace=?1 AND runtime_id=?2",params![namespace,binding.runtime_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(internal)?;
        if let Some((generation, stored, fenced)) = runtime {
            let current: Binding = serde_json::from_str(&stored).map_err(internal)?;
            if generation != binding.generation
                || serde_json::to_value(&current).map_err(internal)?
                    != serde_json::to_value(binding).map_err(internal)?
            {
                return Err(error("runtime_conflict", "Runtime identity cannot change"));
            }
            if fenced {
                return Err(error(
                    "runtime_fenced",
                    "Runtime no longer accepts commands",
                ));
            }
        } else {
            if !matches!(command.kind.as_str(), "launch" | "resume") {
                return Err(error("not_found", "Runtime has not been created"));
            }
            verify_takeover(&tx, binding, command)?;
            tx.execute("INSERT INTO runtimes(namespace,runtime_id,generation,logical_session,tower_id,owner_id,binding,state) VALUES(?1,?2,?3,?4,?5,?6,?7,'starting')",params![namespace,binding.runtime_id,binding.generation,binding.session_id,binding.tower_session_id,binding.owner_id,serde_json::to_string(binding).map_err(internal)?]).map_err(internal)?;
        }
        if binding.protocol == "terminal" {
            tx.execute(
                "UPDATE runtimes SET coverage='partial' WHERE namespace=?1 AND runtime_id=?2",
                params![namespace, binding.runtime_id],
            )
            .map_err(internal)?;
        }
        let receipt = ExecutionReceipt {
            command_id: command.command_id.clone(),
            runtime_id: command.runtime_id.clone(),
            generation: command.generation.clone(),
            receipt_revision: 1,
            state: "accepted".into(),
            acceptance: Some(ExecutionAcceptance {
                level: "adapter".into(),
                at: timestamp(),
            }),
            dispatch: ExecutionDispatch {
                state: "not_started".into(),
                transport_written: Some(false),
            },
            result: None,
            error: None,
        };
        let payload = if command.kind == "reply" {
            None
        } else {
            Some(serde_json::to_string(&command.payload).map_err(internal)?)
        };
        if let Some(body) = &payload {
            let host:i64=tx.query_row("SELECT coalesce(sum(length(CAST(payload AS BLOB))),0)+(SELECT spool_bytes FROM counters WHERE id=1) FROM commands WHERE payload IS NOT NULL",[],|r|r.get(0)).map_err(internal)?;
            let runtime:i64=tx.query_row("SELECT coalesce(sum(length(CAST(payload AS BLOB))),0)+(SELECT spool_bytes FROM runtimes WHERE namespace=?1 AND runtime_id=?2) FROM commands WHERE namespace=?1 AND runtime_id=?2 AND payload IS NOT NULL",params![namespace,binding.runtime_id],|r|r.get(0)).map_err(internal)?;
            if (host as u64).saturating_add(body.len() as u64) > self.limits.host_spool_bytes
                || (runtime as u64).saturating_add(body.len() as u64)
                    > self.limits.runtime_spool_bytes
            {
                return Err(error(
                    "runtime_capacity",
                    "Pending input and transcript storage have reached their limit",
                ));
            }
        }
        tx.execute("INSERT INTO commands(namespace,command_id,runtime_id,generation,fingerprint,kind,payload,expires_at,receipt,receipt_revision,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10)",params![namespace,command.command_id,command.runtime_id,command.generation,fingerprint,command.kind,payload,parse_time(&command.expires_at)? as i64,serde_json::to_string(&receipt).map_err(internal)?,millis() as i64]).map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(receipt)
    }
    pub fn status(&self, namespace: &str, status: &ExecutionStatus) -> Result<ExecutionReceipt> {
        let row: Option<String> = self
            .lock()?
            .query_row(
                "SELECT receipt FROM commands WHERE namespace=?1 AND command_id=?2",
                params![namespace, status.command_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(internal)?;
        let Some(row) = row else {
            return Ok(ExecutionReceipt {
                command_id: status.command_id.clone(),
                runtime_id: status.runtime_id.clone(),
                generation: status.generation.clone(),
                receipt_revision: 0,
                state: "missing".into(),
                acceptance: None,
                dispatch: ExecutionDispatch {
                    state: "unknown".into(),
                    transport_written: None,
                },
                result: None,
                error: None,
            });
        };
        let receipt: ExecutionReceipt = serde_json::from_str(&row).map_err(internal)?;
        if receipt.runtime_id != status.runtime_id || receipt.generation != status.generation {
            return Err(error(
                "runtime_conflict",
                "Receipt belongs to a different execution",
            ));
        }
        Ok(receipt)
    }
}
fn verify_takeover(
    tx: &Transaction<'_>,
    binding: &Binding,
    command: &ExecutionCommand,
) -> Result<()> {
    let mut q=tx.prepare("SELECT runtime_id,generation,fenced,loss FROM runtimes WHERE namespace=?1 AND logical_session=?2 AND (fenced=0 OR termination!='confirmed')").map_err(internal)?;
    let rows = q
        .query_map(params![binding.namespace(), binding.session_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(internal)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(internal)?;
    let payload = serde_json::to_value(&command.payload).map_err(internal)?;
    let proof = payload.get("takeover");
    if rows.is_empty() && proof.is_none() {
        return Ok(());
    }
    if rows.len() != 1
        || !rows[0].2
        || command.kind != "resume"
        || binding.resume_session_id.is_none()
    {
        return Err(error(
            "runtime_overlap",
            "Previous execution has not been confirmed stopped",
        ));
    }
    let proof = proof.ok_or_else(|| {
        error(
            "runtime_overlap",
            "An explicit owner decision is required for uncertain overlap",
        )
    })?;
    let (runtime, generation, _, loss) = &rows[0];
    let loss: RuntimeLoss = serde_json::from_str(loss.as_deref().ok_or_else(|| {
        error(
            "runtime_overlap",
            "Previous owner has no durable fence report",
        )
    })?)
    .map_err(internal)?;
    let action = proof["owner_action_id"]
        .as_str()
        .filter(|s| identifier(s))
        .ok_or_else(|| error("takeover_conflict", "Owner action identity is required"))?;
    if proof["previous_runtime_id"] != *runtime
        || proof["previous_generation"] != *generation
        || proof["loss_id"] != loss.loss_id
        || proof["expected_loss_revision"].as_u64() != Some(loss.report_revision)
    {
        return Err(error(
            "takeover_conflict",
            "Owner decision does not match the current loss report",
        ));
    }
    let used: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM takeovers WHERE namespace=?1 AND action_id=?2)",
            params![binding.namespace(), action],
            |r| r.get(0),
        )
        .map_err(internal)?;
    if used {
        return Err(error(
            "takeover_conflict",
            "Owner action was already bound to a replacement",
        ));
    }
    tx.execute("INSERT INTO takeovers(namespace,action_id,command_id,runtime_id,generation) VALUES(?1,?2,?3,?4,?5)",params![binding.namespace(),action,command.command_id,binding.runtime_id,binding.generation]).map_err(internal)?;
    Ok(())
}
fn validate(binding: &Binding, command: &ExecutionCommand) -> Result<()> {
    for id in [
        &binding.machine_id,
        &binding.session_id,
        &binding.runtime_id,
        &binding.generation,
        &binding.tower_session_id,
        &binding.owner_id,
        &command.command_id,
    ] {
        if !identifier(id) {
            return Err(error("invalid_params", "Invalid execution identity"));
        }
    }
    if command.session_id != binding.session_id
        || command.runtime_id != binding.runtime_id
        || command.generation != binding.generation
    {
        return Err(error(
            "runtime_conflict",
            "Command does not match its runtime binding",
        ));
    }
    if !matches!(
        command.kind.as_str(),
        "launch" | "resume" | "prompt" | "cancel" | "reply" | "configure"
    ) {
        return Err(error("unsupported", "Unknown managed command"));
    }
    if serde_json::to_vec(command).map_err(internal)?.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(error(
            "too_large",
            "Managed command exceeds the frame limit",
        ));
    }
    let Value::Object(payload) = &command.payload else {
        return Err(error(
            "invalid_params",
            "Managed command payload must be an object",
        ));
    };
    if let Some(generation) = payload.get("generation")
        && generation != &Value::String(command.generation.clone())
    {
        return Err(error(
            "runtime_conflict",
            "Payload generation does not match command",
        ));
    }
    Ok(())
}

impl Journal {
    pub fn binding(&self, namespace: &str, runtime_id: &str, generation: &str) -> Result<Binding> {
        let row:Option<String>=self.lock()?.query_row("SELECT binding FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![namespace,runtime_id,generation],|r|r.get(0)).optional().map_err(internal)?;
        serde_json::from_str(&row.ok_or_else(|| error("not_found", "Runtime not found"))?)
            .map_err(internal)
    }
    pub fn bindings(&self, namespace: Option<&str>) -> Result<Vec<Binding>> {
        let conn = self.lock()?;
        let mut query = conn
            .prepare("SELECT binding FROM runtimes WHERE ?1 IS NULL OR namespace=?1 ORDER BY rowid")
            .map_err(internal)?;
        let rows = query
            .query_map([namespace], |r| r.get::<_, String>(0))
            .map_err(internal)?;
        rows.map(|r| serde_json::from_str(&r.map_err(internal)?).map_err(internal))
            .collect()
    }
    pub fn views(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<tower_sessions::execution::RuntimeView>> {
        let conn = self.lock()?;
        let mut q=conn.prepare("SELECT binding,state,fenced,termination,metadata,next_sequence>ack_sequence+1 FROM runtimes WHERE ?1 IS NULL OR namespace=?1 ORDER BY rowid").map_err(internal)?;
        q.query_map(params![namespace], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, bool>(5)?,
            ))
        })
        .map_err(internal)?
        .map(|r| {
            let (binding, state, fenced, termination, metadata, pending_events) =
                r.map_err(internal)?;
            Ok(tower_sessions::execution::RuntimeView {
                binding: serde_json::from_str(&binding).map_err(internal)?,
                metadata: serde_json::from_str(&metadata).map_err(internal)?,
                state,
                fenced,
                termination,
                pending_events: Some(pending_events),
            })
        })
        .collect()
    }
    pub fn terminal_observation(
        &self,
        binding: &Binding,
        observation: &serde_json::Value,
    ) -> Result<()> {
        self.lock()?.execute("UPDATE runtimes SET metadata=json_set(metadata,'$.observation',json(?1)) WHERE namespace=?2 AND runtime_id=?3 AND generation=?4", params![serde_json::to_string(observation).map_err(internal)?,binding.namespace(),binding.runtime_id,binding.generation]).map_err(internal)?;
        Ok(())
    }
    pub fn unsettled(&self, binding: &Binding) -> Result<Vec<(String, ExecutionReceipt)>> {
        let conn = self.lock()?;
        let mut q = conn.prepare("SELECT kind,receipt FROM commands WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND json_extract(receipt,'$.dispatch.state')!='finished'").map_err(internal)?;
        q.query_map(
            params![binding.namespace(), binding.runtime_id, binding.generation],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .map_err(internal)?
        .map(|r| {
            let (kind, body) = r.map_err(internal)?;
            Ok((kind, serde_json::from_str(&body).map_err(internal)?))
        })
        .collect()
    }
    /// Admitted commands that never reached the native session, oldest first.
    pub fn pending_commands(&self, binding: &Binding) -> Result<Vec<ExecutionCommand>> {
        let conn = self.lock()?;
        let mut q = conn.prepare("SELECT command_id,kind,payload,expires_at FROM commands WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND json_extract(receipt,'$.dispatch.state')='not_started' ORDER BY created_at,rowid").map_err(internal)?;
        q.query_map(
            params![binding.namespace(), binding.runtime_id, binding.generation],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        )
        .map_err(internal)?
        .map(|r| {
            let (command_id, kind, payload, expires) = r.map_err(internal)?;
            Ok(ExecutionCommand {
                command_id,
                session_id: binding.session_id.clone(),
                runtime_id: binding.runtime_id.clone(),
                generation: binding.generation.clone(),
                kind,
                payload: match payload {
                    Some(body) => serde_json::from_str(&body).map_err(internal)?,
                    None => serde_json::from_value(serde_json::Value::Null).map_err(internal)?,
                },
                expires_at: crate::format_time(expires as u64),
            })
        })
        .collect()
    }
    pub fn metadata(&self, binding: &Binding) -> Result<serde_json::Value> {
        let json:String=self.lock()?.query_row("SELECT metadata FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation],|r|r.get(0)).map_err(internal)?;
        serde_json::from_str(&json).map_err(internal)
    }
    pub fn state(&self, binding: &Binding) -> Result<(String, bool, String)> {
        self.lock()?.query_row("SELECT state,fenced,termination FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3", params![binding.namespace(),binding.runtime_id,binding.generation], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(internal)
    }
    pub fn losses(&self, namespace: &str) -> Result<Vec<RuntimeLoss>> {
        let conn = self.lock()?;
        let mut q=conn.prepare("SELECT loss FROM runtimes WHERE namespace=?1 AND loss IS NOT NULL AND json_extract(loss,'$.report_revision')>loss_ack LIMIT 32").map_err(internal)?;
        q.query_map([namespace], |r| r.get::<_, String>(0))
            .map_err(internal)?
            .map(|r| serde_json::from_str(&r.map_err(internal)?).map_err(internal))
            .collect()
    }
    pub fn acknowledge_loss(&self, binding: &Binding, id: &str, revision: u64) -> Result<()> {
        let revision = i64::try_from(revision).map_err(internal)?;
        let changed=self.lock()?.execute("UPDATE runtimes SET loss_ack=max(loss_ack,?1) WHERE namespace=?2 AND runtime_id=?3 AND generation=?4 AND json_extract(loss,'$.loss_id')=?5 AND json_extract(loss,'$.report_revision')>=?1",params![revision,binding.namespace(),binding.runtime_id,binding.generation,id]).map_err(internal)?;
        if changed != 1 {
            return Err(error(
                "invalid_ack",
                "Loss acknowledgement does not match durable evidence",
            ));
        }
        Ok(())
    }
    pub fn compact(&self) -> Result<()> {
        let cutoff = millis().saturating_sub(
            self.limits
                .receipt_retention_days
                .saturating_mul(86_400_000),
        );
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let mut q=tx.prepare("SELECT c.namespace,c.receipt FROM commands c JOIN runtimes r ON r.namespace=c.namespace AND r.runtime_id=c.runtime_id WHERE r.ended_at<?1 AND c.kind!='' AND c.ack_revision>=c.receipt_revision LIMIT 1000").map_err(internal)?;
        let rows = q
            .query_map([cutoff as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(internal)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(internal)?;
        drop(q);
        for (namespace, json) in rows {
            let mut receipt: ExecutionReceipt = serde_json::from_str(&json).map_err(internal)?;
            receipt.receipt_revision += 1;
            receipt.result = receipt.result.and_then(|r| {
                r.outcome.map(|outcome| ExecutionResult {
                    outcome: Some(outcome),
                    ..Default::default()
                })
            });
            if let Some(e) = &mut receipt.error {
                e.message = "Execution did not complete successfully".into();
            }
            save_receipt(&tx, &namespace, &receipt)?;
            tx.execute("UPDATE commands SET kind='',payload=NULL,expires_at=0,created_at=0 WHERE namespace=?1 AND command_id=?2",params![namespace,receipt.command_id]).map_err(internal)?;
        }
        tx.commit().map_err(internal)?;
        Ok(())
    }
    pub fn update_metadata(
        &self,
        binding: &Binding,
        state: &str,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        let changed=self.lock()?.execute("UPDATE runtimes SET state=?1,metadata=?2 WHERE namespace=?3 AND runtime_id=?4 AND generation=?5 AND fenced=0",params![state,serde_json::to_string(metadata).map_err(internal)?,binding.namespace(),binding.runtime_id,binding.generation]).map_err(internal)?;
        if changed != 1 {
            return Err(error("runtime_fenced", "Runtime is no longer writable"));
        }
        Ok(())
    }
    pub fn begin_dispatch(&self, binding: &Binding, command_id: &str) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        ensure_live(&tx, binding)?;
        let (receipt,expires):(String,i64)=tx.query_row("SELECT receipt,expires_at FROM commands WHERE namespace=?1 AND command_id=?2 AND runtime_id=?3 AND generation=?4",params![binding.namespace(),command_id,binding.runtime_id,binding.generation],|r|Ok((r.get(0)?,r.get(1)?))).map_err(internal)?;
        let mut receipt: ExecutionReceipt = serde_json::from_str(&receipt).map_err(internal)?;
        if receipt.dispatch.state != "not_started" {
            return Ok(false);
        }
        if expires <= millis() as i64 {
            return Err(error("command_expired", "Command expired before dispatch"));
        }
        receipt.receipt_revision += 1;
        receipt.dispatch.state = "in_flight".into();
        save_receipt(&tx, &binding.namespace(), &receipt)?;
        tx.execute(
            "UPDATE commands SET payload=NULL WHERE namespace=?1 AND command_id=?2",
            params![binding.namespace(), command_id],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(true)
    }
    pub fn finish_dispatch(
        &self,
        binding: &Binding,
        command_id: &str,
        written: bool,
        result: Option<ExecutionResult>,
        failure: Option<ExecutionError>,
    ) -> Result<ExecutionReceipt> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let json:String=tx.query_row("SELECT receipt FROM commands WHERE namespace=?1 AND command_id=?2 AND runtime_id=?3 AND generation=?4",params![binding.namespace(),command_id,binding.runtime_id,binding.generation],|r|r.get(0)).map_err(internal)?;
        let mut receipt: ExecutionReceipt = serde_json::from_str(&json).map_err(internal)?;
        if receipt.dispatch.state == "finished" {
            return Ok(receipt);
        }
        receipt.receipt_revision += 1;
        receipt.dispatch.state = if failure
            .as_ref()
            .is_some_and(|e| e.code == "dispatch_unknown")
        {
            "unknown"
        } else {
            "finished"
        }
        .into();
        receipt.dispatch.transport_written = Some(written);
        receipt.result = result;
        receipt.error = failure;
        save_receipt(&tx, &binding.namespace(), &receipt)?;
        tx.execute(
            "UPDATE commands SET payload=NULL WHERE namespace=?1 AND command_id=?2",
            params![binding.namespace(), command_id],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(receipt)
    }
    pub fn receipt_updates(&self, namespace: &str) -> Result<Vec<ExecutionReceipt>> {
        let conn = self.lock()?;
        let mut query=conn.prepare("SELECT receipt FROM commands WHERE namespace=?1 AND receipt_revision>ack_revision ORDER BY created_at LIMIT 32").map_err(internal)?;
        query
            .query_map([namespace], |r| r.get::<_, String>(0))
            .map_err(internal)?
            .map(|s| serde_json::from_str(&s.map_err(internal)?).map_err(internal))
            .collect()
    }
    pub fn acknowledge_receipt(
        &self,
        namespace: &str,
        command_id: &str,
        revision: u64,
    ) -> Result<()> {
        let revision = i64::try_from(revision).map_err(internal)?;
        let changed=self.lock()?.execute("UPDATE commands SET ack_revision=max(ack_revision,?3) WHERE namespace=?1 AND command_id=?2 AND receipt_revision>=?3",params![namespace,command_id,revision]).map_err(internal)?;
        if changed != 1 {
            return Err(error("invalid_ack", "Receipt ACK exceeds durable evidence"));
        }
        Ok(())
    }
    pub fn append(
        &self,
        binding: &Binding,
        kind: &str,
        payload: Value,
        command_id: Option<String>,
        control: bool,
    ) -> Result<RuntimeEvent> {
        if kind.is_empty() || kind.len() > 300 || kind.chars().any(char::is_control) {
            return Err(error("invalid_event", "Invalid event kind"));
        }
        if matches!(kind, "permission_request" | "elicitation_request") {
            let Value::Object(fields) = &payload else {
                return Err(error(
                    "private_payload",
                    "Private requests need redacted transcript markers",
                ));
            };
            if fields.get("redacted") != Some(&Value::Bool(true))
                || fields.keys().any(|k| {
                    !matches!(
                        k.as_str(),
                        "redacted" | "kind" | "mode" | "source_request_id" | "request_revision"
                    )
                })
            {
                return Err(error(
                    "private_payload",
                    "Private control payload cannot enter the transcript spool",
                ));
            }
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let (sequence,used,controls):(i64,i64,i64)=tx.query_row("SELECT next_sequence,spool_bytes,control_bytes FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(internal)?;
        if sequence <= 0 || sequence == i64::MAX {
            return Err(error("runtime_capacity", "Event sequence exhausted"));
        }
        let event = RuntimeEvent {
            sequence: sequence as u64,
            key: random_id("event_")?,
            kind: kind.into(),
            payload,
            at: Some(timestamp()),
            command_id,
        };
        let body = serde_json::to_string(&event).map_err(internal)?;
        let bytes = body.len() as i64;
        if bytes as usize > tower_protocol::MAX_FRAME_BYTES - 4096 {
            return Err(error(
                "event_too_large",
                "Event must be chunked or staged as an asset",
            ));
        }
        let (host, host_control): (i64, i64) = tx
            .query_row(
                "SELECT spool_bytes,control_bytes FROM counters WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(internal)?;
        if if control {
            host_control as u64 + bytes as u64 > self.limits.control_reserve_bytes
        } else {
            used as u64 + bytes as u64 > self.limits.runtime_spool_bytes
                || host as u64 + bytes as u64 > self.limits.host_spool_bytes
        } {
            return Err(error(
                if control {
                    "control_capacity"
                } else {
                    "spool_capacity"
                },
                "Durable event capacity exhausted before sequence allocation",
            ));
        }
        tx.execute("INSERT INTO events(namespace,runtime_id,generation,sequence,event_key,body,bytes,control) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![binding.namespace(),binding.runtime_id,binding.generation,sequence,event.key,body,bytes,control]).map_err(internal)?;
        tx.execute("UPDATE runtimes SET next_sequence=next_sequence+1,spool_bytes=?1,control_bytes=?2 WHERE namespace=?3 AND runtime_id=?4",params![used+if control{0}else{bytes},controls+if control{bytes}else{0},binding.namespace(),binding.runtime_id]).map_err(internal)?;
        tx.execute("UPDATE counters SET spool_bytes=spool_bytes+?1,control_bytes=control_bytes+?2 WHERE id=1",params![if control{0}else{bytes},if control{bytes}else{0}]).map_err(internal)?;
        if kind == "gap" {
            tx.execute(
                "UPDATE runtimes SET coverage='partial' WHERE namespace=?1 AND runtime_id=?2",
                params![binding.namespace(), binding.runtime_id],
            )
            .map_err(internal)?;
        }
        tx.commit().map_err(internal)?;
        Ok(event)
    }
    pub fn batch(&self, binding: &Binding) -> Result<Option<RuntimeEvents>> {
        let conn = self.lock()?;
        let mut query=conn.prepare("SELECT body FROM events WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 ORDER BY sequence LIMIT 200").map_err(internal)?;
        let mut events = Vec::new();
        let mut bytes = 0usize;
        for row in query
            .query_map(
                params![binding.namespace(), binding.runtime_id, binding.generation],
                |r| r.get::<_, String>(0),
            )
            .map_err(internal)?
        {
            let body = row.map_err(internal)?;
            if bytes + body.len() > tower_protocol::MAX_FRAME_BYTES - 4096 {
                break;
            }
            bytes += body.len();
            events.push(serde_json::from_str(&body).map_err(internal)?);
        }
        if events.is_empty() {
            return Ok(None);
        }
        Ok(Some(RuntimeEvents {
            runtime_id: binding.runtime_id.clone(),
            generation: binding.generation.clone(),
            source_key: binding.generation.clone(),
            events,
        }))
    }
    pub fn cursor(&self, binding: &Binding) -> Result<RuntimeCursor> {
        let (cursor,key,coverage):(i64,Option<String>,String)=self.lock()?.query_row("SELECT ack_sequence,ack_key,coverage FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(internal)?;
        Ok(RuntimeCursor {
            runtime_id: binding.runtime_id.clone(),
            generation: binding.generation.clone(),
            source_key: binding.generation.clone(),
            cursor: cursor as u64,
            last_event_key: key,
            coverage,
        })
    }
    pub fn latest_event_sequence(&self, binding: &Binding) -> Result<u64> {
        let sequence: i64 = self.lock()?.query_row(
            "SELECT next_sequence-1 FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",
            params![binding.namespace(), binding.runtime_id, binding.generation],
            |r| r.get(0),
        ).map_err(internal)?;
        u64::try_from(sequence).map_err(internal)
    }
    pub fn acknowledge_events(&self, binding: &Binding, cursor: &RuntimeCursor) -> Result<()> {
        if cursor.runtime_id != binding.runtime_id
            || cursor.generation != binding.generation
            || cursor.source_key != binding.generation
        {
            return Err(error(
                "invalid_ack",
                "Cursor belongs to a different runtime",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let (ack,next,ack_key):(i64,i64,Option<String>)=tx.query_row("SELECT ack_sequence,next_sequence,ack_key FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(internal)?;
        let cursor_seq = i64::try_from(cursor.cursor).map_err(internal)?;
        if cursor_seq < ack {
            return Err(error(
                "receiver_rollback",
                "Receiver cursor is behind released journal data",
            ));
        }
        if cursor_seq == ack {
            if cursor.last_event_key != ack_key {
                return Err(error(
                    "history_conflict",
                    "Receiver history identity changed",
                ));
            }
            return Ok(());
        }
        if cursor_seq >= next {
            return Err(error(
                "journal_lost",
                "Receiver cursor exceeds the durable journal",
            ));
        }
        let expected:String=tx.query_row("SELECT event_key FROM events WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND sequence=?4",params![binding.namespace(),binding.runtime_id,binding.generation,cursor_seq],|r|r.get(0)).map_err(internal)?;
        if cursor.last_event_key.as_deref() != Some(&expected) {
            return Err(error(
                "history_conflict",
                "Receiver event identity does not match the journal",
            ));
        }
        let (bytes,control):(i64,i64)=tx.query_row("SELECT coalesce(sum(CASE WHEN control=0 THEN bytes ELSE 0 END),0),coalesce(sum(CASE WHEN control=1 THEN bytes ELSE 0 END),0) FROM events WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND sequence<=?4",params![binding.namespace(),binding.runtime_id,binding.generation,cursor_seq],|r|Ok((r.get(0)?,r.get(1)?))).map_err(internal)?;
        tx.execute("DELETE FROM events WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND sequence<=?4",params![binding.namespace(),binding.runtime_id,binding.generation,cursor_seq]).map_err(internal)?;
        tx.execute("UPDATE runtimes SET ack_sequence=?1,ack_key=?2,spool_bytes=spool_bytes-?3,control_bytes=control_bytes-?4 WHERE namespace=?5 AND runtime_id=?6",params![cursor_seq,expected,bytes,control,binding.namespace(),binding.runtime_id]).map_err(internal)?;
        tx.execute("UPDATE counters SET spool_bytes=spool_bytes-?1,control_bytes=control_bytes-?2 WHERE id=1",params![bytes,control]).map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(())
    }
    pub fn fence(&self, binding: &Binding, reason: &str, termination: &str) -> Result<RuntimeLoss> {
        if !matches!(termination, "confirmed" | "unknown") {
            return Err(error("invalid_params", "Invalid termination evidence"));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(internal)?;
        let old: Option<String> = tx
            .query_row(
                "SELECT loss FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",
                params![binding.namespace(), binding.runtime_id, binding.generation],
                |r| r.get(0),
            )
            .map_err(internal)?;
        let first = old.is_none();
        let mut loss = if let Some(old) = old {
            serde_json::from_str::<RuntimeLoss>(&old).map_err(internal)?
        } else {
            RuntimeLoss {
                runtime_id: binding.runtime_id.clone(),
                generation: binding.generation.clone(),
                loss_id: random_id("loss_")?,
                report_revision: 1,
                owner_id: binding.owner_id.clone(),
                termination: termination.into(),
                reason: reason.into(),
                observed_at: timestamp(),
            }
        };
        if !first && (loss.termination == "confirmed" || termination == "unknown") {
            return Ok(loss);
        }
        if loss.termination == "unknown" && termination == "confirmed" {
            loss.report_revision += 1;
            loss.termination = "confirmed".into();
            loss.observed_at = timestamp();
        }
        let mut query=tx.prepare("SELECT receipt FROM commands WHERE namespace=?1 AND runtime_id=?2 AND generation=?3 AND json_extract(receipt,'$.dispatch.state')!='finished'").map_err(internal)?;
        let receipts = query
            .query_map(
                params![binding.namespace(), binding.runtime_id, binding.generation],
                |r| r.get::<_, String>(0),
            )
            .map_err(internal)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(internal)?;
        drop(query);
        for json in receipts.into_iter().filter(|_| first) {
            let mut receipt: ExecutionReceipt = serde_json::from_str(&json).map_err(internal)?;
            receipt.receipt_revision += 1;
            let may_have_effect = receipt.dispatch.state != "not_started";
            receipt.dispatch.state = if may_have_effect {
                "unknown"
            } else {
                "finished"
            }
            .into();
            receipt.error = Some(ExecutionError {
                code: if may_have_effect {
                    "dispatch_unknown"
                } else {
                    "runtime_lost"
                }
                .into(),
                message: "Runtime ownership was lost".into(),
                stage: "recovery".into(),
                may_have_effect,
            });
            save_receipt(&tx, &binding.namespace(), &receipt)?;
        }
        tx.execute("UPDATE commands SET payload=NULL WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",params![binding.namespace(),binding.runtime_id,binding.generation]).map_err(internal)?;
        tx.execute("UPDATE runtimes SET fenced=1,state='ended',termination=?1,loss=?2,ended_at=coalesce(ended_at,?3),metadata=json_set(metadata,'$.error',?7) WHERE namespace=?4 AND runtime_id=?5 AND generation=?6",params![loss.termination,serde_json::to_string(&loss).map_err(internal)?,millis() as i64,binding.namespace(),binding.runtime_id,binding.generation,if matches!(loss.reason.as_str(),"local_stop"|"server_restart"|"stop_runtime"){None}else{Some(&loss.reason)}]).map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(loss)
    }
}
fn ensure_live(tx: &Transaction<'_>, binding: &Binding) -> Result<()> {
    let fenced: Option<bool> = tx
        .query_row(
            "SELECT fenced FROM runtimes WHERE namespace=?1 AND runtime_id=?2 AND generation=?3",
            params![binding.namespace(), binding.runtime_id, binding.generation],
            |r| r.get(0),
        )
        .optional()
        .map_err(internal)?;
    if fenced != Some(false) {
        return Err(error("runtime_fenced", "Runtime generation is not live"));
    }
    Ok(())
}
fn save_receipt(tx: &Transaction<'_>, namespace: &str, receipt: &ExecutionReceipt) -> Result<()> {
    tx.execute(
        "UPDATE commands SET receipt=?1,receipt_revision=?2 WHERE namespace=?3 AND command_id=?4",
        params![
            serde_json::to_string(receipt).map_err(internal)?,
            receipt.receipt_revision as i64,
            namespace,
            receipt.command_id
        ],
    )
    .map_err(internal)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn binding() -> Binding {
        Binding {
            origin: "https://allgather.example".into(),
            machine_id: "machine".into(),
            session_id: "session".into(),
            runtime_id: "runtime".into(),
            generation: "generation".into(),
            tower_session_id: "tower-session".into(),
            owner_id: "owner".into(),
            protocol: "codex-app-server".into(),
            harness: "codex".into(),
            cwd: "/work/project".into(),
            resume_session_id: None,
        }
    }
    fn command(kind: &str, id: &str, payload: serde_json::Value) -> ExecutionCommand {
        ExecutionCommand {
            command_id: id.into(),
            session_id: "session".into(),
            runtime_id: "runtime".into(),
            generation: "generation".into(),
            kind: kind.into(),
            payload: serde_json::from_value(payload).unwrap(),
            expires_at: (time::OffsetDateTime::now_utc() + time::Duration::minutes(5))
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        }
    }
    fn status(c: &ExecutionCommand) -> ExecutionStatus {
        ExecutionStatus {
            command_id: c.command_id.clone(),
            runtime_id: c.runtime_id.clone(),
            generation: c.generation.clone(),
        }
    }
    #[test]
    fn receipt_identity_survives_reopen_and_dispatch_is_never_repeated() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        let b = binding();
        let c = command(
            "launch",
            "command",
            json!({"harness":"codex","generation":"generation"}),
        );
        let receipt = j.admit(&b, &c).unwrap();
        assert_eq!(receipt.state, "accepted");
        assert_eq!(receipt.acceptance.unwrap().level, "adapter");
        assert!(j.begin_dispatch(&b, &c.command_id).unwrap());
        assert!(!j.begin_dispatch(&b, &c.command_id).unwrap());
        let mut changed = c.clone();
        changed.payload = serde_json::from_value(json!({"harness":"other"})).unwrap();
        assert_eq!(
            j.admit(&b, &changed).unwrap_err().data.code,
            "command_conflict"
        );
        drop(j);
        let j = Journal::open(&path, Limits::default()).unwrap();
        assert_eq!(j.admit(&b, &c).unwrap().receipt_revision, 2);
        assert!(!j.begin_dispatch(&b, &c.command_id).unwrap());
        let loss = j.fence(&b, "owner_unreachable", "unknown").unwrap();
        let receipt = j.status(&b.namespace(), &status(&c)).unwrap();
        assert_eq!(receipt.state, "accepted");
        assert_eq!(receipt.dispatch.state, "unknown");
        assert!(receipt.error.unwrap().may_have_effect);
        assert_eq!(
            j.fence(&b, "owner_unreachable", "unknown").unwrap().loss_id,
            loss.loss_id
        );
        assert_eq!(
            j.status(&b.namespace(), &status(&c))
                .unwrap()
                .receipt_revision,
            3
        );
        let confirmed = j.fence(&b, "owner_unreachable", "confirmed").unwrap();
        assert_eq!(confirmed.report_revision, 2);
        assert_eq!(
            j.status(&b.namespace(), &status(&c))
                .unwrap()
                .dispatch
                .state,
            "unknown"
        );
    }
    #[test]
    fn private_replies_never_enter_durable_payloads_and_missing_keys_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        let b = binding();
        j.admit(&b, &command("launch", "launch", json!({})))
            .unwrap();
        let secret = "PRIVATE-ELICITATION-URL-AND-REPLY-38294";
        let reply = command("reply", "reply", json!({"response":{"secret":secret}}));
        j.admit(&b, &reply).unwrap();
        assert!(
            j.append(
                &b,
                "elicitation_request",
                serde_json::from_value(json!({"url":secret})).unwrap(),
                None,
                true
            )
            .is_err()
        );
        for entry in std::fs::read_dir(root.path()).unwrap().flatten() {
            if entry.file_type().unwrap().is_file() {
                let bytes = std::fs::read(entry.path()).unwrap();
                assert!(!bytes.windows(secret.len()).any(|w| w == secret.as_bytes()));
            }
        }
        drop(j);
        std::fs::remove_file(root.path().join("fingerprint.key")).unwrap();
        assert!(Journal::open(&path, Limits::default()).is_err());
    }
    #[test]
    fn event_ack_replay_and_receiver_rollback_preserve_sequence_evidence() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(&root.path().join("runtime.db"), Limits::default()).unwrap();
        let b = binding();
        j.admit(&b, &command("launch", "launch", json!({})))
            .unwrap();
        let first = j
            .append(
                &b,
                "agent_message_chunk",
                serde_json::from_value(json!({"content":{"type":"text","text":"hello"}})).unwrap(),
                None,
                false,
            )
            .unwrap();
        let second = j
            .append(
                &b,
                "turn_end",
                serde_json::from_value(json!({"stop_reason":"end_turn"})).unwrap(),
                None,
                true,
            )
            .unwrap();
        let before = serde_json::to_string(&j.batch(&b).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_string(&j.batch(&b).unwrap()).unwrap(),
            before
        );
        let mut cursor = j.cursor(&b).unwrap();
        cursor.cursor = first.sequence;
        cursor.last_event_key = Some(first.key.clone());
        j.acknowledge_events(&b, &cursor).unwrap();
        j.acknowledge_events(&b, &cursor).unwrap();
        assert_eq!(
            j.batch(&b).unwrap().unwrap().events[0].sequence,
            second.sequence
        );
        let mut rollback = cursor.clone();
        rollback.cursor = 0;
        rollback.last_event_key = None;
        assert_eq!(
            j.acknowledge_events(&b, &rollback).unwrap_err().data.code,
            "receiver_rollback"
        );
        let mut mismatch = cursor.clone();
        mismatch.last_event_key = Some("other".into());
        assert_eq!(
            j.acknowledge_events(&b, &mismatch).unwrap_err().data.code,
            "history_conflict"
        );
        cursor.cursor = second.sequence;
        cursor.last_event_key = Some(second.key);
        j.acknowledge_events(&b, &cursor).unwrap();
        assert!(j.batch(&b).unwrap().is_none());
    }
    #[test]
    fn bulk_capacity_does_not_consume_control_capacity_or_allocate_a_sequence() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(
            &root.path().join("runtime.db"),
            Limits {
                runtime_spool_bytes: 3,
                host_spool_bytes: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let b = binding();
        j.admit(&b, &command("launch", "launch", json!({})))
            .unwrap();
        assert_eq!(
            j.append(&b, "text", Value::String("bulk".into()), None, false)
                .err()
                .unwrap()
                .data
                .code,
            "spool_capacity"
        );
        let stopped = j
            .append(
                &b,
                "turn_end",
                serde_json::from_value(json!({"stop_reason":"cancelled"})).unwrap(),
                None,
                true,
            )
            .unwrap();
        assert_eq!(stopped.sequence, 1);
    }
    #[test]
    fn receipt_capacity_and_uncertain_overlap_fail_before_acceptance() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(
            &root.path().join("runtime.db"),
            Limits {
                receipt_count: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let b = binding();
        let c = command("launch", "one", json!({}));
        j.admit(&b, &c).unwrap();
        assert!(j.admit(&b, &c).is_ok());
        assert_eq!(
            j.admit(&b, &command("prompt", "two", json!({})))
                .unwrap_err()
                .data
                .code,
            "runtime_capacity"
        );
    }
    #[test]
    fn takeover_evidence_is_bound_to_one_replacement_and_the_exact_loss_report() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(&root.path().join("runtime.db"), Limits::default()).unwrap();
        let b = binding();
        j.admit(&b, &command("launch", "first", json!({}))).unwrap();
        let loss = j.fence(&b, "owner_unreachable", "unknown").unwrap();
        let mut next = b.clone();
        next.runtime_id = "replacement".into();
        next.generation = "replacement-generation".into();
        next.owner_id = "new-owner".into();
        next.resume_session_id = Some("native".into());
        let proof = json!({"previous_runtime_id":b.runtime_id,"previous_generation":b.generation,"loss_id":loss.loss_id,"expected_loss_revision":loss.report_revision,"owner_action_id":"owner-action"});
        let mut c = command("resume", "second", json!({"takeover":proof}));
        c.runtime_id = next.runtime_id.clone();
        c.generation = next.generation.clone();
        let accepted = j.admit(&next, &c).unwrap();
        assert_eq!(j.admit(&next, &c).unwrap(), accepted);
        j.fence(&next, "stopped", "confirmed").unwrap();
        let mut third = next.clone();
        third.runtime_id = "third".into();
        third.generation = "third-generation".into();
        let mut retry = c.clone();
        retry.command_id = "third-command".into();
        retry.runtime_id = third.runtime_id.clone();
        retry.generation = third.generation.clone();
        assert_eq!(
            j.admit(&third, &retry).unwrap_err().data.code,
            "takeover_conflict"
        );
        j.fence(&b, "terminated", "confirmed").unwrap();
        let mut stale = c.clone();
        stale.command_id = "stale-proof".into();
        stale.runtime_id = "stale-runtime".into();
        stale.generation = "stale-generation".into();
        third.runtime_id = stale.runtime_id.clone();
        third.generation = stale.generation.clone();
        assert_eq!(
            j.admit(&third, &stale).unwrap_err().data.code,
            "runtime_overlap"
        );
    }
    #[test]
    fn expired_full_receipts_compact_without_losing_deduplication_or_outcome() {
        let root = tempfile::tempdir().unwrap();
        let j = Journal::open(&root.path().join("runtime.db"), Limits::default()).unwrap();
        let b = binding();
        let c = command("launch", "first", json!({}));
        j.admit(&b, &c).unwrap();
        j.begin_dispatch(&b, &c.command_id).unwrap();
        let receipt = j
            .finish_dispatch(
                &b,
                &c.command_id,
                true,
                Some(ExecutionResult {
                    owner_id: Some("owner".into()),
                    outcome: Some("completed".into()),
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
        j.fence(&b, "stopped", "confirmed").unwrap();
        j.acknowledge_receipt(&b.namespace(), &c.command_id, receipt.receipt_revision)
            .unwrap();
        j.lock()
            .unwrap()
            .execute("UPDATE runtimes SET ended_at=0", [])
            .unwrap();
        j.compact().unwrap();
        let next = j.admit(&b, &c).unwrap();
        assert_eq!(next.state, "accepted");
        assert!(next.receipt_revision > receipt.receipt_revision);
        assert_eq!(
            next.result.as_ref().unwrap().outcome.as_deref(),
            Some("completed")
        );
        assert!(next.result.unwrap().owner_id.is_none());
        let mut changed = c.clone();
        changed.payload = Value::Null;
        assert!(j.admit(&b, &changed).is_err());
    }
    #[test]
    fn a_missing_journal_cannot_reset_existing_command_identity() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("runtime.db");
        let j = Journal::open(&path, Limits::default()).unwrap();
        j.admit(&binding(), &command("launch", "identity", json!({})))
            .unwrap();
        drop(j);
        std::fs::remove_file(&path).unwrap();
        assert!(Journal::open(&path, Limits::default()).is_err());
    }
}
