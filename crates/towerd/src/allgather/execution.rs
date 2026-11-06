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
    Request(RuntimeBinding, String, u64, bool),
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
    ready: HashSet<String>,
    connection: String,
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
        if serde_json::to_vec(&frame)?.len() > tower_protocol::MAX_FRAME_BYTES {
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
        if self.connection.is_empty() {
            let mut bytes = [0u8; 16];
            getrandom::fill(&mut bytes).map_err(|_| invalid("Random source unavailable"))?;
            self.connection = bytes.iter().map(|b| format!("{b:02x}")).collect();
        }
        let connection = self.connection.clone();
        let updates = worker
            .execute(move |s| s.execution_updates(epoch, connection))
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
                v.binding.runtime_id == receipt.runtime_id
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
        for request in updates.requests {
            let id = request.source_request_id.clone();
            let revision = request.request_revision;
            let Some(view) = views.iter().find(|v| {
                v.binding.runtime_id == request.runtime_id
                    && v.binding.generation == request.generation
            }) else {
                continue;
            };
            let live = request.state == "pending";
            if live && (!self.ready.contains(&key(&view.binding)) || view.fenced) {
                continue;
            }
            self.insert(
                "session.request",
                serde_json::to_value(request)?,
                format!("request/{id}"),
                Item::Request(view.binding.clone(), id, revision, live),
                &mut frames,
            )?;
        }
        for v in views {
            let b = v.binding;
            let k = key(&b);
            if self.fenced.contains(&k) {
                continue;
            }
            if !self.reconciled.contains(&k) {
                self.insert("session.cursor",json!({"runtime_id":b.runtime_id,"generation":b.generation,"source_key":b.generation}),format!("source/{k}"),Item::Cursor(b),&mut frames)?;
                continue;
            }
            if v.pending_events == Some(false) {
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
            if let Item::Request(binding, id, revision, live) = &p.item
                && matches!(
                    code,
                    "stale_request"
                        | "unsupported_control"
                        | "control_scope_required"
                        | "request_conflict"
                        | "invalid_control"
                        | "unsupported_schema"
                        | "request_too_large"
                        | "runtime_fenced"
                        | "runtime_conflict"
                )
            {
                let ack = public(
                    json!({"source_request_id":id,"request_revision":revision,"connection":self.connection}),
                )?;
                worker
                    .execute(move |s| s.execution_ack("request".into(), ack, epoch))
                    .await??;
                if *live {
                    let b = binding.clone();
                    let reason = code.to_owned();
                    self.fenced.insert(key(&b));
                    worker
                        .execute(move |s| s.execution_fence(b, reason, epoch))
                        .await??;
                }
                return Ok(true);
            }
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
                let ready = batch.events.iter().any(|event| {
                    if event.kind != "runtime_state" {
                        return false;
                    }
                    let Ok(p) = serde_json::to_value(&event.payload) else {
                        return false;
                    };
                    p["owner_id"] == b.owner_id
                        && p["tower_session_id"] == b.tower_session_id
                        && matches!(p["state"].as_str(), Some("idle" | "running" | "waiting"))
                        && (b.protocol == "terminal"
                            || p[if b.protocol == "acp" {
                                "acp_session_id"
                            } else {
                                "native_session_id"
                            }]
                            .as_str()
                            .is_some_and(|id| !id.is_empty()))
                });
                let runtime_key = key(&b);
                let ack = RuntimeCursor {
                    runtime_id: b.runtime_id,
                    generation: b.generation.clone(),
                    source_key: b.generation,
                    cursor,
                    last_event_key: Some(event.key.clone()),
                    coverage: if b.protocol == "terminal"
                        || batch.events.iter().any(|e| e.kind == "gap")
                    {
                        "partial"
                    } else {
                        "complete"
                    }
                    .into(),
                };
                let value = public(serde_json::to_value(ack)?)?;
                worker
                    .execute(move |s| s.execution_ack("events".into(), value, epoch))
                    .await??;
                if ready {
                    self.ready.insert(runtime_key);
                }
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
            Item::Request(_, id, revision, _) => {
                if r["source_request_id"] != id
                    || (r["request_revision"] != revision
                        && !matches!(
                            r["state"].as_str(),
                            Some("answered" | "cancelled" | "stale")
                        ))
                {
                    return Err(invalid("Private request acknowledgement changed identity"));
                }
                let ack = public(
                    json!({"source_request_id":id,"request_revision":revision,"connection":self.connection,"peer_state":r["state"]}),
                )?;
                worker
                    .execute(move |s| s.execution_ack("request".into(), ack, epoch))
                    .await??;
            }
        }
        Ok(true)
    }
}
