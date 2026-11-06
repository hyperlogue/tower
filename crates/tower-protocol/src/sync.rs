use crate::{AgentRecord, ProtocolVersion, Rule, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_SYNC_RECORDS: usize = 5000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatePush {
    pub seq: u64,
    pub full: bool,
    pub records: Vec<AgentRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordPatch {
    pub id: String,
    pub set: BTreeMap<String, Value>,
    #[serde(default)]
    pub unset: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateDelta {
    pub seq: u64,
    pub base_seq: u64,
    pub upserts: Vec<RecordPatch>,
    pub removed: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncRejection {
    pub id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncAck {
    pub cursor: u64,
    #[serde(default)]
    pub rejected: Vec<SyncRejection>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllgatherHello {
    pub protocol_version: ProtocolVersion,
    pub tower_version: String,
    pub host_id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub policy_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acp_harnesses: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub harness_controls: BTreeMap<String, crate::HarnessControls>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub terminal_controls: BTreeMap<String, crate::HarnessControls>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllgatherHelloAck {
    pub protocol_version: ProtocolVersion,
    pub node_name: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyPush {
    pub policy_id: String,
    pub revision: u64,
    pub rules: Vec<Rule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyDelta {
    pub policy_id: String,
    pub base_revision: u64,
    pub revision: u64,
    pub upserts: Vec<Rule>,
    pub removed: Vec<String>,
    pub order: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyAck {
    #[serde(default)]
    pub received_at: u64,
    pub policy_id: String,
    pub previous_revision: Option<u64>,
    pub applied_revision: Option<u64>,
    pub changed_rule_ids: Vec<String>,
    pub outcome: String,
    pub reason: Option<String>,
}
