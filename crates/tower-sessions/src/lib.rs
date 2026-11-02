use std::{collections::BTreeMap, sync::Arc};
use terminal::TerminalState;
use tower_protocol::{Capabilities, RpcError, Secret, SessionOpenParams, Value};
pub mod execution;
pub mod messaging;
pub mod terminal;

pub type Result<T> = std::result::Result<T, RpcError>;

#[derive(Clone, Debug)]
pub struct ManagedSession {
    pub id: String,
    pub protocol: String,
    pub binding: Value,
    pub launch: Option<Value>,
    pub auto_resume: bool,
}

pub struct PreparedSession {
    pub harness: String,
    pub cwd: String,
    pub native_session_id: Option<String>,
    pub binding: Value,
    pub launch: Value,
    pub mode: Option<String>,
    pub caps: Capabilities,
    pub terminal: Option<TerminalState>,
}

#[derive(Clone, Debug)]
pub struct Observation {
    pub id: String,
    // None retains the existing binding; Some persists a newly assigned handle.
    pub binding: Option<Value>,
    pub status: String,
    pub pid: Option<u32>,
    pub pid_start_time: Option<u64>,
    pub caps: Capabilities,
    pub terminal: Option<TerminalState>,
    pub recovery_pending: bool,
    pub metadata: SessionMetadata,
}

#[derive(Clone, Debug, Default)]
pub struct SessionMetadata {
    pub native_session_id: Option<String>,
    pub display_name: Option<String>,
    pub cwd: Option<String>,
    pub detail: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct ClipboardTarget {
    pub generation: String,
}

pub trait ManagedBackend: Send + Sync {
    fn clipboard(&self, _session: &ManagedSession) -> Result<Option<ClipboardTarget>> {
        Ok(None)
    }

    fn send_message(
        &self,
        _session: &ManagedSession,
        _text: &str,
    ) -> Result<tower_protocol::DeliveryReceipt> {
        Err(RpcError::application(
            "unsupported",
            "This backend has no message channel",
        ))
    }
    fn conversations(
        &self,
        _request: &tower_protocol::ConversationListParams,
    ) -> Result<tower_protocol::ConversationList> {
        Ok(Default::default())
    }
    fn cleanup(&self, _session: &ManagedSession) -> Result<()> {
        Ok(())
    }
    fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        Vec::new()
    }
    fn protocol(&self) -> &str;
    fn features(&self) -> &'static [&'static str] {
        &[]
    }
    // Preparation may initialize shared infrastructure, but must not start the
    // session: tower persists the binding and identity before calling start.
    fn prepare(&self, id: &str, request: &SessionOpenParams) -> Result<PreparedSession>;
    fn start(&self, session: &ManagedSession, token: &Secret) -> Result<Observation>;
    fn observe(&self, sessions: &[ManagedSession]) -> Result<Vec<Observation>>;
    fn stop(&self, session: &ManagedSession) -> Result<Observation>;
    fn prepare_resume(
        &self,
        _session: &ManagedSession,
        _mode: Option<&str>,
        _args: Option<&[String]>,
    ) -> Result<PreparedSession> {
        Err(RpcError::application(
            "unsupported",
            "This backend cannot resume sessions",
        ))
    }
    fn recover(&self, _session: &ManagedSession) -> Result<Option<PreparedSession>> {
        Ok(None)
    }
    fn terminal(&self) -> Option<&dyn terminal::TerminalAccess> {
        None
    }
}

#[derive(Clone, Default)]
pub struct Backends {
    default: Option<String>,
    drivers: BTreeMap<String, Arc<dyn ManagedBackend>>,
    delivery: Option<Arc<dyn messaging::RegisteredDelivery>>,
    execution: Option<Arc<dyn execution::ExecutionHost>>,
}
impl Backends {
    pub fn with_execution(mut self, execution: Arc<dyn execution::ExecutionHost>) -> Self {
        self.execution = Some(execution);
        self
    }
    pub fn execution(&self) -> Option<&dyn execution::ExecutionHost> {
        self.execution.as_deref()
    }
    pub fn with_delivery(mut self, delivery: Arc<dyn messaging::RegisteredDelivery>) -> Self {
        self.delivery = Some(delivery);
        self
    }
    pub fn delivery(&self) -> Option<&dyn messaging::RegisteredDelivery> {
        self.delivery.as_deref()
    }
    pub fn new(default: &str, drivers: Vec<Arc<dyn ManagedBackend>>) -> Result<Self> {
        let mut result = Self::default();
        for driver in drivers {
            let protocol = driver.protocol().to_owned();
            if !valid_protocol(&protocol) || result.drivers.insert(protocol, driver).is_some() {
                return Err(RpcError::application(
                    "invalid_params",
                    "Invalid or duplicate session backend",
                ));
            }
        }
        if !result.drivers.contains_key(default) {
            return Err(RpcError::application(
                "unsupported",
                "Default session backend is unavailable",
            ));
        }
        result.default = Some(default.into());
        Ok(result)
    }
    pub fn select(&self, protocol: Option<&str>) -> Result<&dyn ManagedBackend> {
        protocol
            .or(self.default.as_deref())
            .and_then(|p| self.get(p))
            .ok_or_else(|| {
                RpcError::application("unsupported", "Session protocol has no installed backend")
            })
    }
    pub fn get(&self, protocol: &str) -> Option<&dyn ManagedBackend> {
        self.drivers.get(protocol).map(AsRef::as_ref)
    }
    pub fn iter(&self) -> impl Iterator<Item = &dyn ManagedBackend> {
        self.drivers.values().map(AsRef::as_ref)
    }
}
pub fn valid_protocol(protocol: &str) -> bool {
    !protocol.is_empty()
        && protocol.len() <= 64
        && protocol
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
