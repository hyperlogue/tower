use crate::Harness;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeSession {
    pub id: String,
    #[serde(default)]
    pub resume_ref: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSpec {
    pub harness: Harness,
    pub cwd: String,
    pub args: Vec<String>,
    pub mode: Option<String>,
    #[serde(default)]
    pub resume: Option<NativeSession>,
    #[serde(default)]
    pub fork: bool,
    #[serde(default)]
    pub native_home: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LauncherPlan {
    pub id: String,
    pub generation: String,
    pub state_dir: String,
    pub tower_socket: String,
    pub control_socket: String,
    pub spec: AgentSpec,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background_jobs: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeState {
    pub id: String,
    pub generation: String,
    pub harness: Harness,
    pub status: String,
    pub cwd: String,
    pub native: Option<NativeSession>,
    pub display_name: Option<String>,
    pub detail: AgentDetail,
    pub launcher_pid: u32,
    pub launcher_start_time: u64,
    pub boot_id: String,
    pub agent_pid: Option<u32>,
    pub agent_start_time: Option<u64>,
    pub seq: u64,
    pub updated_at: u64,
    pub stop_requested: bool,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub private: BTreeMap<String, serde_json::Value>,
}
