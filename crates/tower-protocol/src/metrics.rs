use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostMetrics {
    pub sampled_at: u64,
    pub cpu_percent: Option<f64>,
    pub memory: MemoryMetrics,
    pub disk: DiskMetrics,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub used_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskMetrics {
    pub available_bytes: Option<u64>,
}
