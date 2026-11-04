use super::{Error, Result, encode};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tower_protocol::{AgentRecord, MAX_SYNC_RECORDS, SyncAck};

type Projection = BTreeMap<String, Map<String, Value>>;

const FIELDS: &[&str] = &[
    "id",
    "display_name",
    "node",
    "harness",
    "mode",
    "ownership",
    "protocol",
    "trust",
    "native_session_id",
    "pid",
    "cwd",
    "status",
    "status_detail",
    "caps",
    "labels",
    "task_refs",
    "last_seen",
    "revision",
];

fn projection(records: &[AgentRecord], checkouts: bool) -> Result<Projection> {
    if records.len() > MAX_SYNC_RECORDS {
        return Err(tower_protocol::RpcError::application(
            "sync_record_limit",
            "Full status projection exceeds 5,000 records",
        )
        .into());
    }
    records
        .iter()
        .map(|record| {
            let Value::Object(mut fields) = serde_json::to_value(record)? else {
                unreachable!()
            };
            fields.retain(|key, _| {
                FIELDS.contains(&key.as_str()) || checkouts && key == "checkout_id"
            });
            Ok((record.id.clone(), fields))
        })
        .collect()
}

struct Pending {
    seq: u64,
    records: Projection,
    ids: BTreeSet<String>,
    full: bool,
}

#[derive(Default)]
pub(super) struct Publisher {
    checkouts: bool,
    sequence: u64,
    acknowledged_sequence: u64,
    acknowledged: Option<Projection>,
    pending: Option<Pending>,
    deltas_safe: bool,
}

impl Publisher {
    pub fn include_checkouts(&mut self, enabled: bool) {
        self.checkouts = enabled;
        self.resync();
    }
    pub fn waiting(&self) -> bool {
        self.pending.is_some()
    }
    pub fn resync(&mut self) {
        self.acknowledged = None;
        self.pending = None;
        self.deltas_safe = false;
    }
    pub fn prepare(&mut self, records: &[AgentRecord], delta: bool) -> Result<Option<Value>> {
        if self.waiting() {
            return Ok(None);
        }
        let next = projection(records, self.checkouts)?;
        let seq = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other("Status sequence exhausted".into()))?;
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        if let Some(previous) = &self.acknowledged {
            removed = previous
                .keys()
                .filter(|id| !next.contains_key(*id))
                .cloned()
                .collect();
            for (id, fields) in &next {
                let old = previous.get(id);
                let mut set = Map::new();
                let mut unset = Vec::new();
                for (key, value) in fields {
                    if key != "id"
                        && key != "revision"
                        && old.and_then(|f| f.get(key)) != Some(value)
                    {
                        set.insert(key.clone(), value.clone());
                    }
                }
                if let Some(old) = old {
                    unset = old
                        .keys()
                        .filter(|key| key.as_str() != "id" && !fields.contains_key(*key))
                        .cloned()
                        .collect();
                }
                if old.is_none() || !set.is_empty() || !unset.is_empty() {
                    if let Some(revision) = fields.get("revision") {
                        set.insert("revision".into(), revision.clone());
                    }
                    changed.push(json!({"id":id,"set":set,"unset":unset}));
                }
            }
            if changed.is_empty() && removed.is_empty() {
                return Ok(None);
            }
        }
        let full = json!({"jsonrpc":"2.0","method":"state.push","params":{"seq":seq,"full":true,"records":next.values().collect::<Vec<_>>()}});
        let frame = if delta && self.deltas_safe && self.acknowledged.is_some() {
            let patch = json!({"jsonrpc":"2.0","method":"state.delta","params":{"seq":seq,"base_seq":self.acknowledged_sequence,"upserts":changed,"removed":removed}});
            if serde_json::to_vec(&patch)?.len() < serde_json::to_vec(&full)?.len() {
                patch
            } else {
                full
            }
        } else {
            full
        };
        encode(&frame)?;
        self.sequence = seq;
        let full = frame["method"] == "state.push";
        let ids = if full {
            next.keys().cloned().collect()
        } else {
            frame["params"]["upserts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|p| p["id"].as_str().map(str::to_owned))
                .chain(
                    frame["params"]["removed"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|id| id.as_str().map(str::to_owned)),
                )
                .collect()
        };
        self.pending = Some(Pending {
            seq,
            ids,
            records: next,
            full,
        });
        Ok(Some(frame))
    }
    pub fn acknowledge(&mut self, ack: &SyncAck) -> Result<Option<Vec<String>>> {
        let Some(pending) = self.pending.as_ref().filter(|p| p.seq == ack.cursor) else {
            return Ok(None);
        };
        let mut rejected = BTreeSet::new();
        if ack.rejected.len() > MAX_SYNC_RECORDS
            || ack
                .rejected
                .iter()
                .any(|r| !pending.ids.contains(&r.id) || !rejected.insert(&r.id))
        {
            return Err(Error::Other("Invalid status acknowledgement".into()));
        }
        let pending = self.pending.take().expect("checked");
        self.deltas_safe = ack.rejected.is_empty() && (pending.full || self.deltas_safe);
        self.acknowledged_sequence = pending.seq;
        self.acknowledged = Some(pending.records);
        Ok(Some(pending.ids.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: &str) -> AgentRecord {
        serde_json::from_value(json!({"id":id,"node":"machine","harness":"fixture","ownership":"registered","trust":"weak","cwd":"/work/project","status":"idle","caps":tower_protocol::Capabilities::unavailable("fixture"),"attach_count":0,"sources":["cli"],"registered_at":1,"updated_at":1,"last_seen":1,"revision":1,"sync_state":"pending"})).unwrap()
    }
    fn ack(publisher: &mut Publisher, frame: &Value) {
        publisher
            .acknowledge(&SyncAck {
                cursor: frame["params"]["seq"].as_u64().unwrap(),
                rejected: vec![],
            })
            .unwrap();
    }
    #[test]
    fn field_deltas_coalesce_until_ack_and_resync_restores_a_full_snapshot() {
        let mut p = Publisher::default();
        let mut records = vec![record("a"), record("b")];
        let first = p.prepare(&records, true).unwrap().unwrap();
        assert_eq!(first["method"], "state.push");
        records[0].status = "working".into();
        records[0].revision = 2;
        assert!(p.prepare(&records, true).unwrap().is_none());
        assert!(
            p.acknowledge(&SyncAck {
                cursor: 99,
                rejected: vec![]
            })
            .unwrap()
            .is_none()
        );
        ack(&mut p, &first);
        let delta = p.prepare(&records, true).unwrap().unwrap();
        assert_eq!(delta["method"], "state.delta");
        assert_eq!(delta["params"]["base_seq"], first["params"]["seq"]);
        assert_eq!(
            delta["params"]["upserts"],
            json!([{"id":"a","set":{"status":"working","revision":2},"unset":[]}])
        );
        ack(&mut p, &delta);
        records[0].sync_state = "synced".into();
        records[0].revision = 3;
        assert!(
            p.prepare(&records, true).unwrap().is_none(),
            "ACK bookkeeping must not trigger traffic"
        );
        records.remove(1);
        let removed = p.prepare(&records, true).unwrap().unwrap();
        assert_eq!(removed["params"]["removed"], json!(["b"]));
        p.resync();
        assert!(
            p.acknowledge(&SyncAck {
                cursor: removed["params"]["seq"].as_u64().unwrap(),
                rejected: vec![]
            })
            .unwrap()
            .is_none()
        );
        assert_eq!(
            p.prepare(&records, true).unwrap().unwrap()["method"],
            "state.push"
        );
    }
    #[test]
    fn new_records_null_values_and_rejection_recovery_are_explicit() {
        let mut p = Publisher::default();
        let mut records = vec![record("a")];
        records[0].display_name = Some("before".into());
        let first = p.prepare(&records, true).unwrap().unwrap();
        ack(&mut p, &first);
        records[0].display_name = None;
        records.push(record("b"));
        let patch = p.prepare(&records, true).unwrap().unwrap();
        assert_eq!(patch["method"], "state.delta");
        assert!(
            patch["params"]["upserts"][0]["set"]
                .as_object()
                .unwrap()
                .contains_key("display_name")
        );
        assert_eq!(
            patch["params"]["upserts"][0]["set"]["display_name"],
            Value::Null
        );
        assert_eq!(patch["params"]["upserts"][1]["set"]["harness"], "fixture");
        let seq = patch["params"]["seq"].as_u64().unwrap();
        assert!(
            p.acknowledge(&SyncAck {
                cursor: seq,
                rejected: vec![tower_protocol::SyncRejection {
                    id: "unrelated".into(),
                    reason: "bad".into()
                }]
            })
            .is_err()
        );
        p.acknowledge(&SyncAck {
            cursor: seq,
            rejected: vec![tower_protocol::SyncRejection {
                id: "b".into(),
                reason: "bad".into(),
            }],
        })
        .unwrap();
        assert!(p.prepare(&records, true).unwrap().is_none());
        records[0].status = "working".into();
        assert_eq!(
            p.prepare(&records, true).unwrap().unwrap()["method"],
            "state.push"
        );
    }
    #[test]
    fn old_peers_receive_full_snapshots_only() {
        let mut p = Publisher::default();
        let mut records = vec![record("a")];
        let first = p.prepare(&records, false).unwrap().unwrap();
        ack(&mut p, &first);
        records[0].status = "working".into();
        assert_eq!(
            p.prepare(&records, false).unwrap().unwrap()["method"],
            "state.push"
        );
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[test]
    fn baseline_projection_never_sends_unnegotiated_session_metadata() {
        let record: AgentRecord=serde_json::from_value(json!({"id":"a","checkout_id":"checkout-one","node":"machine","harness":"fixture","ownership":"registered","trust":"weak","cwd":"/project","status":"idle","caps":tower_protocol::Capabilities::unavailable("fixture"),"attach_count":0,"sources":[],"registered_at":1,"updated_at":1,"last_seen":1,"revision":1,"sync_state":"pending","tags":["pinned"],"needs_attention":true})).unwrap();
        let mut publisher = Publisher::default();
        let full = publisher
            .prepare(std::slice::from_ref(&record), true)
            .unwrap()
            .unwrap();
        let public = &full["params"]["records"][0];
        assert!(public.get("tags").is_none());
        assert!(public.get("needs_attention").is_none());
        assert!(public.get("checkout_id").is_none());
        let mut negotiated = Publisher::default();
        negotiated.include_checkouts(true);
        let full = negotiated
            .prepare(std::slice::from_ref(&record), true)
            .unwrap()
            .unwrap();
        assert_eq!(full["params"]["records"][0]["checkout_id"], "checkout-one");

        publisher
            .acknowledge(&SyncAck {
                cursor: full["params"]["seq"].as_u64().unwrap(),
                rejected: vec![],
            })
            .unwrap();
        let mut changed = record;
        changed.tags.clear();
        changed.needs_attention = false;
        changed.revision += 1;
        assert!(publisher.prepare(&[changed], true).unwrap().is_none());
    }
}
