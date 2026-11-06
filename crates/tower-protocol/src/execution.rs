use crate::{RequestId, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HarnessControls {
    pub protocol: String,
    pub capabilities: Vec<String>,
    pub cancel_mode: Option<String>,
}
impl HarnessControls {
    pub fn terminal() -> Self {
        Self {
            protocol: "terminal".into(),
            capabilities: Vec::new(),
            cancel_mode: Some("runtime".into()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExecutionCommand {
    pub command_id: String,
    pub session_id: String,
    pub runtime_id: String,
    pub generation: String,
    pub kind: String,
    pub payload: Value,
    pub expires_at: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionStatus {
    pub command_id: String,
    pub runtime_id: String,
    pub generation: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionAcceptance {
    pub level: String,
    pub at: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionDispatch {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_written: Option<bool>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tower_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionError {
    pub code: String,
    pub message: String,
    pub stage: String,
    pub may_have_effect: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub command_id: String,
    pub runtime_id: String,
    pub generation: String,
    pub receipt_revision: u64,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance: Option<ExecutionAcceptance>,
    pub dispatch: ExecutionDispatch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ExecutionResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ExecutionError>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub runtime_id: String,
    pub generation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeLoss {
    pub runtime_id: String,
    pub generation: String,
    pub loss_id: String,
    pub report_revision: u64,
    pub owner_id: String,
    pub termination: String,
    pub reason: String,
    pub observed_at: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub sequence: u64,
    pub key: String,
    pub kind: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct RuntimeEvents {
    pub runtime_id: String,
    pub generation: String,
    pub source_key: String,
    pub events: Vec<RuntimeEvent>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeCursor {
    pub runtime_id: String,
    pub generation: String,
    pub source_key: String,
    pub cursor: u64,
    pub last_event_key: Option<String>,
    pub coverage: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct RuntimeRequest {
    pub runtime_id: String,
    pub generation: String,
    pub owner_id: String,
    pub source_request_id: String,
    pub request_revision: u64,
    pub native_request_id: RequestId,
    pub kind: String,
    pub state: String,
    pub payload: Value,
}
