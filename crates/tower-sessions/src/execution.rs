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
    pub fn scope(&self) -> &'static str {
        if self.protocol == "terminal" {
            "terminal:control"
        } else {
            "acp:control"
        }
    }
    pub fn from_session(session: &crate::ManagedSession) -> Result<Option<Self>> {
        let value = serde_json::to_value(&session.binding).map_err(|_| {
            tower_protocol::RpcError::application("internal_error", "Invalid session binding")
        })?;
        let value = if session.protocol == "terminal" {
            let Some(value) = value.get("execution") else {
                return Ok(None);
            };
            value.clone()
        } else {
            value
        };
        serde_json::from_value(value).map(Some).map_err(|_| {
            tower_protocol::RpcError::application("internal_error", "Invalid execution binding")
        })
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
/// A command the runtime is about to dispatch to a native agent.
pub struct Authorization {
    pub binding: RuntimeBinding,
    pub command_id: String,
    pub kind: String,
    /// Cancelling or declining work, which needs no policy approval.
    pub cleanup: bool,
    pub target: Option<RuntimeBinding>,
}
pub type Future<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;
/// Checks current policy just before dispatch; supplied by the server.
pub type Authorize = std::sync::Arc<dyn Fn(Authorization) -> Future<Result<()>> + Send + Sync>;
pub trait ExecutionHost: Send + Sync {
    /// Called once at startup, before any command is executed.
    fn bind(&self, _authorize: Authorize) {}
    /// Stops every runtime before the server exits.
    fn shutdown(&self) -> Future<()> {
        Box::pin(async {})
    }
    fn controls(&self) -> std::collections::BTreeMap<String, HarnessControls>;
    fn validate_new(&self, _binding: &RuntimeBinding, _command: &ExecutionCommand) -> Result<()> {
        Ok(())
    }
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

pub fn force_resume(
    command: &ExecutionCommand,
) -> Result<Option<Vec<tower_protocol::RuntimeIdentity>>> {
    let p = serde_json::to_value(&command.payload).map_err(|_| {
        tower_protocol::RpcError::application("invalid_params", "Invalid command payload")
    })?;
    let invalid = || {
        tower_protocol::RpcError::application(
            "invalid_params",
            "Force resume requires a fixed list of 1 to 32 distinct cleanup targets",
        )
    };
    if p.get("takeover").is_some() {
        return Err(tower_protocol::RpcError::application(
            "unsupported_control",
            "Uncertain-overlap takeover is no longer supported; stop the old execution or use force resume",
        ));
    }
    match p.get("force") {
        None | Some(serde_json::Value::Bool(false)) if p.get("cleanup").is_none() => {
            return Ok(None);
        }
        Some(serde_json::Value::Bool(true))
            if command.kind == "resume" && p["protocol"] != "terminal" => {}
        _ => return Err(invalid()),
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Target {
        runtime_id: String,
        generation: String,
    }
    let targets: Vec<Target> =
        serde_json::from_value(p["cleanup"].clone()).map_err(|_| invalid())?;
    let mut ids = std::collections::BTreeSet::new();
    if targets.is_empty()
        || targets.len() > 32
        || targets.iter().any(|t| {
            t.runtime_id == command.runtime_id
                || !ids.insert(t.runtime_id.clone())
                || [&t.runtime_id, &t.generation]
                    .iter()
                    .any(|id| id.is_empty() || id.len() > 160 || id.chars().any(char::is_control))
        })
    {
        return Err(invalid());
    }
    Ok(Some(
        targets
            .into_iter()
            .map(|t| tower_protocol::RuntimeIdentity {
                runtime_id: t.runtime_id,
                generation: t.generation,
            })
            .collect(),
    ))
}
