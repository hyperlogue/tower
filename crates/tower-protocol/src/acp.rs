use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpFrame {
    pub runtime_id: String,
    pub generation: String,
    pub sequence: u64,
    pub frame: String,
}
