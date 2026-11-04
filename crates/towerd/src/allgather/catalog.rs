use super::{Error, Result, encode};
use crate::worker::ServiceWorker;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;
use tower_protocol::Checkout;
use tower_protocol::{CatalogDelta, RepositoryCatalog, RpcError};

pub(super) struct Sync {
    enabled: bool,
    deltas: bool,
    checkouts: bool,
    ready: bool,
    publisher: Publisher,
    cursor: Option<u64>,
    pending_since: Option<tokio::time::Instant>,
}
impl Sync {
    pub fn new(local: &[String], remote: &[String]) -> Self {
        let supports =
            |name: &str| local.iter().any(|c| c == name) && remote.iter().any(|c| c == name);
        let enabled = supports("repository-catalog.v1");
        Self {
            enabled,
            deltas: enabled && supports("repository-catalog-delta.v1"),
            checkouts: enabled && supports("checkout-sync.v1"),
            ready: false,
            publisher: Publisher::default(),
            cursor: None,
            pending_since: None,
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn checkouts_enabled(&self) -> bool {
        self.checkouts
    }
    pub async fn apply(
        &mut self,
        method: &str,
        params: Value,
        worker: &ServiceWorker,
        epoch: u64,
    ) -> Result<std::result::Result<u64, RpcError>> {
        worker
            .execute(move |s| s.sync_catalog_pending(epoch))
            .await??;
        let result = if !self.enabled || method == "catalog.delta" && (!self.deltas || !self.ready)
        {
            Err(RpcError::application(
                "unsupported",
                "Repository catalog was not negotiated",
            ))
        } else if method == "catalog.delta" {
            match serde_json::from_value::<CatalogDelta>(params) {
                Ok(catalog) => {
                    worker
                        .execute(move |s| s.sync_catalog_delta(catalog, epoch))
                        .await?
                }
                Err(_) => Err(RpcError::application(
                    "invalid_params",
                    "Invalid catalog delta",
                )),
            }
        } else {
            match serde_json::from_value::<RepositoryCatalog>(params) {
                Ok(catalog) => {
                    worker
                        .execute(move |s| s.sync_catalog(catalog, epoch))
                        .await?
                }
                Err(_) => Err(RpcError::application(
                    "invalid_params",
                    "Invalid catalog snapshot",
                )),
            }
        };
        self.ready = result.is_ok();
        Ok(result)
    }
    pub async fn prepare(&mut self, worker: &ServiceWorker, epoch: u64) -> Result<Option<Value>> {
        if !self.checkouts || !self.ready {
            return Ok(None);
        }
        if self
            .pending_since
            .is_some_and(|at| at.elapsed() >= Duration::from_secs(30))
        {
            return Err(Error::Other("Checkout acknowledgement timed out".into()));
        }
        if self.publisher.waiting() {
            return Ok(None);
        }
        let since = self.cursor;
        let Some((cursor, records)) = worker
            .execute(move |s| s.sync_checkouts(epoch, since))
            .await??
        else {
            return Ok(None);
        };
        let frame = self.publisher.prepare(records)?;
        self.cursor = Some(cursor);
        if frame.is_some() {
            self.pending_since = Some(tokio::time::Instant::now());
        }
        Ok(frame)
    }
    pub fn acknowledge(&mut self, cursor: u64) {
        if self.publisher.acknowledge(cursor) {
            self.pending_since = None;
        }
    }
    pub fn resync(&mut self) {
        self.publisher.resync();
        self.pending_since = None;
        self.cursor = None;
    }
}

#[derive(Default)]
struct Publisher {
    sequence: u64,
    acknowledged_sequence: u64,
    acknowledged: Option<BTreeMap<String, Checkout>>,
    pending: Option<(u64, BTreeMap<String, Checkout>)>,
}
impl Publisher {
    pub fn waiting(&self) -> bool {
        self.pending.is_some()
    }
    pub fn resync(&mut self) {
        self.acknowledged = None;
        self.pending = None;
    }
    pub fn prepare(&mut self, records: Vec<Checkout>) -> Result<Option<Value>> {
        if self.waiting() {
            return Ok(None);
        }
        if records.len() > 5000 {
            return Err(Error::Other(
                "Checkout snapshot exceeds its record limit".into(),
            ));
        }
        let next: BTreeMap<_, _> = records.into_iter().map(|c| (c.id.clone(), c)).collect();
        let seq = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other("Checkout sequence exhausted".into()))?;
        let full = json!({"jsonrpc":"2.0","method":"checkout.snapshot","params":{"seq":seq,"full":true,"records":next.values().collect::<Vec<_>>()}});
        encode(&full)?;
        let frame = if let Some(old) = &self.acknowledged {
            let upserts = next
                .iter()
                .filter(|(id, row)| old.get(*id) != Some(row))
                .map(|(_, row)| row)
                .collect::<Vec<_>>();
            let removed = old
                .keys()
                .filter(|id| !next.contains_key(*id))
                .collect::<Vec<_>>();
            if upserts.is_empty() && removed.is_empty() {
                return Ok(None);
            }
            let delta = json!({"jsonrpc":"2.0","method":"checkout.delta","params":{"seq":seq,"base_seq":self.acknowledged_sequence,"upserts":upserts,"removed":removed}});
            if serde_json::to_vec(&delta)?.len() < serde_json::to_vec(&full)?.len() {
                delta
            } else {
                full
            }
        } else {
            full
        };
        encode(&frame)?;
        self.sequence = seq;
        self.pending = Some((seq, next));
        Ok(Some(frame))
    }
    pub fn acknowledge(&mut self, cursor: u64) -> bool {
        if self.pending.as_ref().is_none_or(|(seq, _)| *seq != cursor) {
            return false;
        }
        let (seq, records) = self.pending.take().expect("checked");
        self.acknowledged_sequence = seq;
        self.acknowledged = Some(records);
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn row(id: &str) -> Checkout {
        Checkout {
            id: id.into(),
            repository_id: None,
            path: format!("~/work/{id}"),
            system: "git".into(),
            branch: Some("main".into()),
            head: Some("a".repeat(40)),
            dirty: false,
            conflicts: false,
            locked: false,
            prunable: false,
            available: true,
            tracking_ref: None,
            ahead: None,
            behind: None,
            observed_at: 1,
            revision: 1,
        }
    }
    #[test]
    fn checkout_delta_is_committed_only_after_ack_and_full_recovers() {
        let mut p = Publisher::default();
        let mut rows = vec![row("a"), row("b")];
        let first = p.prepare(rows.clone()).unwrap().unwrap();
        assert_eq!(first["method"], "checkout.snapshot");
        rows[0].dirty = true;
        assert!(p.prepare(rows.clone()).unwrap().is_none());
        assert!(!p.acknowledge(99));
        assert!(p.acknowledge(1));
        let delta = p.prepare(rows.clone()).unwrap().unwrap();
        assert_eq!(delta["method"], "checkout.delta");
        assert_eq!(delta["params"]["base_seq"], 1);
        assert_eq!(delta["params"]["upserts"].as_array().unwrap().len(), 1);
        p.resync();
        assert!(!p.acknowledge(2));
        let full = p.prepare(rows).unwrap().unwrap();
        assert_eq!(full["method"], "checkout.snapshot");
    }
}
