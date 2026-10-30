//! Extensible public wire types. This crate deliberately depends only on serde.

mod value;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
pub use value::{Number, Value};

pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(pub String);
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(Number),
    String(String),
    Null,
}
fn present_id<'de, D: Deserializer<'de>>(d: D) -> Result<Option<RequestId>, D::Error> {
    RequestId::deserialize(d).map(Some)
}

// Omission is allowed. A present identity field must have its declared type;
// null must not silently remove credentials or an agent identity marker.
fn present_field<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CallerContext {
    #[serde(
        default,
        deserialize_with = "present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub agent_token: Option<Secret>,
    #[serde(
        default,
        deserialize_with = "present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub bearer_token: Option<Secret>,
    #[serde(
        default,
        deserialize_with = "present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub agent_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub harness: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_session_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(
        default,
        deserialize_with = "present_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub id: Option<RequestId>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub context: CallerContext,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(flatten)]
    pub body: ResponseBody,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponseBody {
    Success { result: Value },
    Failure { error: RpcError },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    pub data: ErrorData,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorData {
    pub code: String,
    #[serde(flatten)]
    pub details: BTreeMap<String, Value>,
}
impl RpcError {
    pub fn application(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
            data: ErrorData {
                code: code.into(),
                details: BTreeMap::new(),
            },
        }
    }
    pub fn protocol(code: i32, name: &str, message: &str) -> Self {
        Self {
            code,
            ..Self::application(name, message)
        }
    }
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.data.code, self.message)
    }
}
impl std::error::Error for RpcError {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HelloParams {
    pub protocol_version: ProtocolVersion,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HelloResult {
    pub protocol_version: ProtocolVersion,
    pub capabilities: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub version: String,
    pub protocol_version: ProtocolVersion,
    pub pid: u32,
    pub host_id: String,
    pub node_name: String,
    pub mode: String,
    pub pool: PoolInfo,
    pub allgather: AllgatherInfo,
    pub startup: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolInfo {
    pub running: bool,
    pub proto_version: Option<ProtocolVersion>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllgatherInfo {
    pub connected: bool,
    pub policy_revision: Option<u64>,
}

/// Strings remain open for future harnesses, statuses, capabilities and sources.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: String,
    pub display_name: Option<String>,
    pub node: String,
    pub harness: String,
    pub mode: Option<String>,
    pub ownership: String,
    pub trust: String,
    pub native_session_id: Option<String>,
    pub pid: Option<u32>,
    pub pid_start_time: Option<u64>,
    pub cwd: String,
    pub status: String,
    pub status_detail: Option<Value>,
    pub caps: Capabilities,
    pub pool_session: Option<String>,
    pub attach_count: u32,
    pub sources: Vec<String>,
    pub registered_at: u64,
    pub updated_at: u64,
    pub last_seen: u64,
    pub revision: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub task_refs: Vec<String>,
    pub sync_state: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub attach: bool,
    pub kill: bool,
    pub message: String,
    pub message_reason: Option<String>,
    pub wake_when_idle: bool,
    pub reply_path: bool,
    pub steer: bool,
}
impl Capabilities {
    pub fn unavailable(reason: &str) -> Self {
        Self {
            attach: false,
            kill: false,
            message: "none".into(),
            message_reason: Some(reason.into()),
            wake_when_idle: false,
            reply_path: false,
            steer: false,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterParams {
    pub harness: String,
    pub cwd: String,
    pub native_session_id: Option<String>,
    pub pid: Option<u32>,
    pub display_name: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterResult {
    pub id: String,
    pub token: Secret,
    pub trust: String,
}

/// Preserve missing versus explicit null when applying partial updates.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Patch<T> {
    #[default]
    Missing,
    Clear,
    Set(T),
}
impl<T> Patch<T> {
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}
impl<T: Serialize> Serialize for Patch<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Set(v) => v.serialize(s),
            _ => s.serialize_none(),
        }
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Patch<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match Option::<T>::deserialize(d)? {
            Some(v) => Self::Set(v),
            None => Self::Clear,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateParams {
    pub id: String,
    pub token: Secret,
    pub seq: u64,
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub display_name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub detail: Patch<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateResult {
    pub applied: bool,
    pub revision: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentCredential {
    pub id: String,
    pub token: Secret,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentFilter {
    pub harness: Option<String>,
    pub status: Option<String>,
    pub cwd_prefix: Option<String>,
    pub ownership: Option<String>,
    #[serde(default)]
    pub include_exited: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub filter: AgentFilter,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdParams {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub from: String,
    pub to: String,
    pub action: String,
    pub created_by: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulePutParams {
    pub rule: Rule,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulesReplaceParams {
    pub rules: Vec<Rule>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulesView {
    pub local_rules: Vec<Rule>,
    pub allgather_rules: Vec<Rule>,
    pub policy_revision: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenIssueParams {
    pub name: String,
    pub scope: Vec<String>,
    pub expires: Option<String>,
    pub expires_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub name: String,
    pub scope: Vec<String>,
    pub created_at: u64,
    pub expires_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenResult {
    pub token: Secret,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NameParams {
    pub name: String,
}
