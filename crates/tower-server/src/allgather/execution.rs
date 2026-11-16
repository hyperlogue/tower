use crate::{Error, Result, worker::ServiceWorker};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use tokio::time::{Duration, Instant};
use tower_protocol::{ExecutionReceipt, RuntimeCursor, RuntimeEvents, RuntimeLoss};
use tower_sessions::execution::RuntimeBinding;

enum Item {
    Cursor(RuntimeBinding),
    Events(RuntimeBinding, RuntimeEvents),
    Receipt(ExecutionReceipt),
    Loss(RuntimeBinding, RuntimeLoss),
    Acp(RuntimeBinding, u64),
}
struct Pending {
    key: String,
    item: Item,
    sent: Instant,
}
#[derive(Default)]
pub(super) struct Publisher {
    counter: u64,
    pending: BTreeMap<String, Pending>,
    reconciled: HashSet<String>,
    fenced: HashSet<String>,
}
fn key(b: &RuntimeBinding) -> String {
    serde_json::to_string(&(&b.runtime_id, &b.generation)).expect("string tuple")
}
fn invalid(message: &str) -> Error {
    Error::Rpc(tower_protocol::RpcError::application(
        "invalid_runtime_ack",
        message,
    ))
}
fn public(v: Value) -> Result<tower_protocol::Value> {
    Ok(serde_json::from_value(v)?)
}
impl Publisher {
    fn insert(
        &mut self,
        method: &str,
        params: Value,
        key: String,
        item: Item,
        frames: &mut Vec<Value>,
    ) -> Result<()> {
        if self.pending.len() >= 32 || self.pending.values().any(|p| p.key == key) {
            return Ok(());
        }
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| invalid("Runtime RPC identity exhausted"))?;
        let id = format!("runtime/{}", self.counter);
        let frame = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        if serde_json::to_vec(&frame)?.len() > tower_protocol::frame_limit(method) {
            return Err(invalid("Runtime publication exceeds frame limit"));
        }
        self.pending.insert(
            id,
            Pending {
                key,
                item,
                sent: Instant::now(),
            },
        );
        frames.push(frame);
        Ok(())
    }
    pub async fn prepare(
        &mut self,
        worker: &ServiceWorker,
        epoch: u64,
        protocols: [bool; 2],
    ) -> Result<Vec<Value>> {
        if self
            .pending
            .values()
            .any(|p| p.sent.elapsed() > Duration::from_secs(30))
        {
            return Err(invalid("Runtime publication acknowledgement timed out"));
        }
        let updates = worker
            .execute(move |s| s.execution_updates(epoch))
            .await??;
        let views: Vec<_> = worker
            .execute(move |s| s.execution_views(epoch))
            .await??
            .into_iter()
            .filter(|v| protocols[usize::from(v.binding.protocol == "terminal")])
            .collect();
        let mut frames = Vec::new();
        for receipt in updates.receipts {
            if !views.iter().any(|v| {
                v.binding.protocol == "terminal"
                    && v.binding.runtime_id == receipt.runtime_id
                    && v.binding.generation == receipt.generation
            }) {
                continue;
            }
            self.insert(
                "command.receipt",
                serde_json::to_value(&receipt)?,
                format!("receipt/{}", receipt.command_id),
                Item::Receipt(receipt),
                &mut frames,
            )?;
        }
        for loss in updates.losses {
            if let Some(v) = views.iter().find(|v| {
                v.binding.runtime_id == loss.runtime_id && v.binding.generation == loss.generation
            }) {
                self.insert(
                    "runtime.lost",
                    serde_json::to_value(&loss)?,
                    format!("loss/{}", loss.loss_id),
                    Item::Loss(v.binding.clone(), loss),
                    &mut frames,
                )?;
            }
        }
        for frame in updates.acp_frames {
            if let Some(view) = views.iter().find(|v| {
                v.binding.protocol == "acp"
                    && v.binding.runtime_id == frame.runtime_id
                    && v.binding.generation == frame.generation
            }) {
                self.insert(
                    "acp.frame",
                    serde_json::to_value(&frame)?,
                    format!("acp/{}", frame.runtime_id),
                    Item::Acp(view.binding.clone(), frame.sequence),
                    &mut frames,
                )?;
            }
        }
        for v in views {
            if v.binding.protocol != "terminal" {
                continue;
            }
            let b = v.binding;
            let k = key(&b);
            if self.fenced.contains(&k) {
                continue;
            }
            if !self.reconciled.contains(&k) {
                self.insert("session.cursor",json!({"runtime_id":b.runtime_id,"generation":b.generation,"source_key":b.generation}),format!("source/{k}"),Item::Cursor(b),&mut frames)?;
                continue;
            }
            if !v.pending_events {
                continue;
            }
            if self.pending.len() >= 32
                || self
                    .pending
                    .values()
                    .any(|p| p.key == format!("source/{k}"))
            {
                continue;
            }
            let binding = b.clone();
            if let Some(batch) = worker
                .execute(move |s| s.execution_batch(binding, epoch))
                .await??
            {
                self.insert(
                    "session.events",
                    serde_json::to_value(&batch)?,
                    format!("source/{k}"),
                    Item::Events(b, batch),
                    &mut frames,
                )?;
            }
        }
        Ok(frames)
    }
    pub async fn response(&mut self, v: Value, worker: &ServiceWorker, epoch: u64) -> Result<bool> {
        let Some(id) = v["id"].as_str() else {
            return Ok(false);
        };
        let Some(p) = self.pending.remove(id) else {
            return Ok(false);
        };
        if v.get("error").is_some() {
            let code = v
                .pointer("/error/data/code")
                .and_then(Value::as_str)
                .unwrap_or("runtime_sync_failed");
            if matches!(
                code,
                "sequence_conflict"
                    | "sequence_gap"
                    | "event_conflict"
                    | "runtime_identity_conflict"
                    | "runtime_fenced"
                    | "runtime_conflict"
            ) && let Item::Cursor(b) | Item::Events(b, _) = p.item
            {
                self.fenced.insert(key(&b));
                let reason = code.to_owned();
                worker
                    .execute(move |s| s.execution_fence(b, reason, epoch))
                    .await??;
                return Ok(true);
            }
            return Err(invalid("Allgather rejected a runtime publication"));
        }
        let r = &v["result"];
        match p.item {
            Item::Cursor(b) => {
                let cursor: RuntimeCursor = serde_json::from_value(r.clone())?;
                if cursor.runtime_id != b.runtime_id
                    || cursor.generation != b.generation
                    || cursor.source_key != b.generation
                {
                    return Err(invalid("Cursor belongs to another execution"));
                }
                let binding = b.clone();
                let local = worker
                    .execute(move |s| s.execution_cursor(binding, epoch))
                    .await??;
                let ack = public(r.clone())?;
                let accepted = worker
                    .execute(move |s| s.execution_ack("events".into(), ack, epoch))
                    .await?;
                if let Err(e) = accepted {
                    self.fenced.insert(key(&b));
                    let reason = e.data.code;
                    worker
                        .execute(move |s| s.execution_fence(b, reason, epoch))
                        .await??;
                } else {
                    if cursor.cursor < local.cursor {
                        return Err(invalid("Receiver cursor moved backwards"));
                    }
                    self.reconciled.insert(key(&b));
                    worker
                        .execute(move |s| s.execution_reannounce(b, epoch))
                        .await??;
                }
            }
            Item::Events(b, batch) => {
                if r["runtime_id"] != b.runtime_id
                    || r["generation"] != b.generation
                    || r["source_key"] != b.generation
                {
                    return Err(invalid("Event ACK belongs to another execution"));
                }
                let cursor = r["cursor"]
                    .as_u64()
                    .ok_or_else(|| invalid("Event ACK lacks a cursor"))?;
                let event = batch
                    .events
                    .last()
                    .filter(|e| e.sequence == cursor)
                    .ok_or_else(|| invalid("Event ACK did not commit the complete batch"))?;
                let ack = RuntimeCursor {
                    runtime_id: b.runtime_id,
                    generation: b.generation.clone(),
                    source_key: b.generation,
                    cursor,
                    last_event_key: Some(event.key.clone()),
                    coverage: "partial".into(),
                };
                let value = public(serde_json::to_value(ack)?)?;
                worker
                    .execute(move |s| s.execution_ack("events".into(), value, epoch))
                    .await??;
            }
            Item::Receipt(receipt) => {
                if r["command_id"] != receipt.command_id
                    || r["receipt_revision"]
                        .as_u64()
                        .is_none_or(|n| n < receipt.receipt_revision)
                {
                    return Err(invalid("Receipt acknowledgement changed identity"));
                }
                let value = public(json!({
                    "command_id":receipt.command_id,
                    "runtime_id":receipt.runtime_id,
                    "generation":receipt.generation,
                    "receipt_revision":receipt.receipt_revision,
                    "reported_revision":r["receipt_revision"]
                }))?;
                worker
                    .execute(move |s| s.execution_ack("receipt".into(), value, epoch))
                    .await??;
            }
            Item::Loss(b, loss) => {
                if r["runtime_id"] != b.runtime_id
                    || r["generation"] != b.generation
                    || r["loss_id"] != loss.loss_id
                    || r["report_revision"] != loss.report_revision
                    || r["applied_revision"]
                        .as_u64()
                        .is_some_and(|revision| revision < loss.report_revision)
                    || r["fenced"] != true
                {
                    return Err(invalid("Runtime loss acknowledgement changed identity"));
                }
                let value = public(r.clone())?;
                worker
                    .execute(move |s| s.execution_ack("loss".into(), value, epoch))
                    .await??;
            }
            Item::Acp(b, sequence) => {
                if r["runtime_id"] != b.runtime_id
                    || r["generation"] != b.generation
                    || r["sequence"] != sequence
                {
                    return Err(invalid("ACP acknowledgement changed identity"));
                }
                let ack = public(r.clone())?;
                worker
                    .execute(move |s| s.execution_ack("acp".into(), ack, epoch))
                    .await??;
            }
        }
        Ok(true)
    }
}
