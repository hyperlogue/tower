use crate::{Patch, Secret};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeliveryEndpoint {
    pub messaging_socket: Option<String>,
    pub messaging_token: Option<Secret>,
    pub thread_id: Option<String>,
    pub app_server: Option<String>,
    pub codex_home: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeliveryUpdate {
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub messaging_socket: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub messaging_token: Patch<Secret>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub thread_id: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub codex_home: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_missing")]
    pub app_server: Patch<String>,
}
impl DeliveryUpdate {
    pub fn apply(self, endpoint: &mut DeliveryEndpoint) {
        fn field<T>(patch: Patch<T>, target: &mut Option<T>) {
            match patch {
                Patch::Missing => {}
                Patch::Clear => *target = None,
                Patch::Set(value) => *target = Some(value),
            }
        }
        field(self.messaging_socket, &mut endpoint.messaging_socket);
        field(self.messaging_token, &mut endpoint.messaging_token);
        field(self.thread_id, &mut endpoint.thread_id);
        field(self.codex_home, &mut endpoint.codex_home);
        field(self.app_server, &mut endpoint.app_server);
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryParams {
    pub id: String,
    pub token: Secret,
    pub delivery: DeliveryEndpoint,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct MessageSendParams {
    pub to: String,
    pub body: String,
    #[serde(
        default,
        deserialize_with = "crate::present_field",
        skip_serializing_if = "Option::is_none"
    )]
    pub from_token: Option<Secret>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub method: String,
    pub acknowledgement: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageResult {
    pub message_id: String,
    pub state: String,
    #[serde(flatten)]
    pub receipt: DeliveryReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Approval {
    pub id: String,
    pub from: String,
    pub to: String,
    pub state: String,
    pub required: Vec<String>,
    pub created_at: u64,
    pub expires_at: u64,
    pub resolved_at: Option<u64>,
    pub resolved_by: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalResolveParams {
    pub id: String,
    pub allow: bool,
    #[serde(default = "remember")]
    pub remember: bool,
}
fn remember() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalWatchParams {
    pub id: String,
    #[serde(default = "watch_timeout")]
    pub timeout_s: u64,
}
fn watch_timeout() -> u64 {
    600
}
