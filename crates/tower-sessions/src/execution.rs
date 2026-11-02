use crate::Result;
use serde::{Deserialize, Serialize};
use tower_protocol::{
    ExecutionCommand, ExecutionReceipt, ExecutionStatus, RuntimeCursor, RuntimeEvents, RuntimeLoss,
    RuntimeRequest, Value,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeBinding {
    pub origin: String,
    pub machine_id: String,
    pub session_id: String,
    pub runtime_id: String,
    pub generation: String,
    pub tower_session_id: String,
    pub owner_id: String,
    pub protocol: String,
    pub harness: String,
    pub cwd: String,
    pub resume_session_id: Option<String>,
}
impl RuntimeBinding {
    pub fn namespace(&self) -> String {
        serde_json::to_string(&(&self.origin, &self.machine_id)).expect("string tuple serializes")
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct RuntimeView {
    pub binding: RuntimeBinding,
    pub state: String,
    pub fenced: bool,
    pub termination: String,
    pub metadata: Value,
    #[serde(default)]
    pub pending_events: Option<bool>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RuntimeUpdates {
    pub receipts: Vec<ExecutionReceipt>,
    pub losses: Vec<RuntimeLoss>,
    pub requests: Vec<RuntimeRequest>,
}
pub use tower_protocol::HarnessControls;
pub trait ExecutionHost: Send + Sync {
    fn controls(&self) -> std::collections::BTreeMap<String, HarnessControls>;
    fn execute(
        &self,
        binding: &RuntimeBinding,
        command: &ExecutionCommand,
    ) -> Result<ExecutionReceipt>;
    fn status(&self, namespace: &str, command: &ExecutionStatus) -> Result<ExecutionReceipt>;
    fn list(&self, namespace: Option<&str>) -> Result<Vec<RuntimeView>>;
    fn updates(&self, namespace: &str, connection: &str) -> Result<RuntimeUpdates>;
    fn batch(&self, binding: &RuntimeBinding) -> Result<Option<RuntimeEvents>>;
    fn cursor(&self, binding: &RuntimeBinding) -> Result<RuntimeCursor>;
    fn reannounce(&self, binding: &RuntimeBinding) -> Result<()>;
    fn acknowledge(&self, namespace: &str, kind: &str, value: Value) -> Result<()>;
    fn fence(&self, binding: &RuntimeBinding, reason: &str) -> Result<()>;
    fn stop(&self, binding: &RuntimeBinding) -> Result<()>;
}
