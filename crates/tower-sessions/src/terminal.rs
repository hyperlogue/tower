use crate::{ManagedSession, Result};
use tower_protocol::{
    PoolInfo,
    terminal::{AttachInfo, SnapshotResult},
};

pub trait TerminalAccess: Send + Sync {
    fn attach_info(&self, session: &ManagedSession) -> Result<AttachInfo>;
    fn snapshot(&self, session: &ManagedSession) -> Result<SnapshotResult>;
    fn pool_info(&self) -> PoolInfo;
}

#[derive(Clone, Debug)]
pub struct TerminalState {
    pub pool_session: String,
    pub attach_count: u32,
}
