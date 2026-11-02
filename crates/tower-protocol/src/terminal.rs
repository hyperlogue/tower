use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachInfo {
    pub pool_socket: String,
    pub pool_session: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResult {
    pub id: String,
    pub rows: u16,
    pub cols: u16,
    pub buffer: String,
    pub truncated: bool,
    pub limited: bool,
}
