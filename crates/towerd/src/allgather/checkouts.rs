use super::{Error, Result, encode};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tower_protocol::Checkout;

#[derive(Default)]
pub(super) struct Publisher {
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
