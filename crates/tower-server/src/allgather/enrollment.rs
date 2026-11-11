use super::{Config, Error, Result, http_client, json_response, safe_url};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tower_protocol::Secret;

pub(crate) const ATTEMPT_LIFETIME_MS: u64 = 600_000;
pub(crate) const MAX_ATTEMPTS: usize = 8;
const MAX_STATE_BYTES: usize = 64 * 1024;
pub(crate) type Attempts = BTreeMap<String, Pending>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub actor: String,
    pub origin: String,
    pub host_id: String,
    pub scopes: Vec<String>,
    pub label: Option<String>,
    pub pairing_code: Secret,
    pub request_id: String,
    pub tower_version: String,
}
impl Plan {
    pub(crate) fn body(&self) -> serde_json::Value {
        let mut body = json!({"host_id":self.host_id,"tower_version":self.tower_version,"requested_scopes":self.scopes,"pairing_code":self.pairing_code,"request_id":self.request_id});
        if let Some(label) = &self.label {
            body["node_label"] = label.clone().into();
        }
        body
    }
}
/// Allgather's reply to a pairing request: an issued, not yet confirmed credential.
#[derive(Deserialize)]
pub(crate) struct Enrollment {
    pub enrollment_id: String,
    pub authorization_url: String,
    pub confirmation_token: Secret,
    pub expires_at: String,
    pub machine_id: String,
    pub credential_id: String,
    pub access_key: Secret,
    pub node_label: Option<String>,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Pending {
    pub plan: Plan,
    pub code_hash: String,
    pub deadline_ms: u64,
    pub result: Option<serde_json::Value>,
    pub confirmation: Option<Confirmation>,
}
impl Pending {
    pub(crate) fn enrollment_id(&self) -> Option<&str> {
        self.result.as_ref()?.get("id")?.as_str()
    }
    pub(crate) fn clear_secrets(&mut self) {
        self.plan.pairing_code.0.clear();
        self.confirmation = None;
    }
}
pub(crate) fn valid_pairing_secret(value: &str) -> bool {
    value.len() == 43
        && URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|bytes| bytes.len() == 32 && URL_SAFE_NO_PAD.encode(bytes) == value)
}
pub(crate) fn request_id() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| Error::Other("Random source unavailable".into()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
fn state_path(paths: &tower_pal::Paths) -> std::path::PathBuf {
    paths.state.join("allgather-pairing.json")
}
pub(crate) fn load(paths: &tower_pal::Paths) -> Result<Attempts> {
    let bytes = match tower_pal::read_private_bounded(&state_path(paths), MAX_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Attempts::new()),
        Err(_) => return Err(Error::Other("Cannot read pairing state".into())),
    };
    let attempts: Attempts =
        serde_json::from_slice(&bytes).map_err(|_| Error::Other("Invalid pairing state".into()))?;
    if attempts.len() > MAX_ATTEMPTS
        || attempts
            .iter()
            .any(|(id, pending)| id != &pending.plan.request_id || !valid_pairing_secret(id))
    {
        return Err(Error::Other("Invalid pairing state".into()));
    }
    Ok(attempts)
}
fn encode(attempts: &Attempts) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(attempts)
        .map_err(|_| Error::Other("Cannot encode pairing state".into()))?;
    if attempts.len() > MAX_ATTEMPTS || bytes.len() > MAX_STATE_BYTES {
        return Err(Error::Other("Pairing state capacity exceeded".into()));
    }
    Ok(bytes)
}
pub(crate) fn check_capacity(attempts: &Attempts) -> Result<()> {
    encode(attempts).map(|_| ())
}
pub(crate) fn persist(paths: &tower_pal::Paths, attempts: &Attempts) -> Result<()> {
    tower_pal::write_private(&state_path(paths), &encode(attempts)?)
        .map_err(|_| Error::Other("Cannot save pairing state".into()))
}
pub(crate) fn expires_at(value: &str) -> Result<u64> {
    let time = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| Error::Other("Invalid enrollment expiry".into()))?;
    u64::try_from(time.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| Error::Other("Invalid enrollment expiry".into()))
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Consent {
    pub origin: String,
    pub machine_id: String,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Confirmation {
    pub request_id: String,
    pub origin: String,
    pub enrollment_id: String,
    #[serde(alias = "polling_token")]
    pub confirmation_token: Secret,
    pub credential_id: String,
    pub machine_id: String,
    pub access_key_hash: String,
    pub expires_at_ms: u64,
}
pub fn origin(input: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(input)
        .map_err(|_| Error::Other("Use an absolute Allgather HTTP(S) URL".into()))?;
    match url.scheme() {
        "wss" => {
            let _ = url.set_scheme("https");
        }
        "ws" => {
            let _ = url.set_scheme("http");
        }
        _ => {}
    }
    if !matches!(url.path(), "/" | "/tower") || url.query().is_some() || url.fragment().is_some() {
        return Err(Error::Other(
            "Use the Allgather origin or its /tower endpoint without a query or fragment".into(),
        ));
    }
    safe_url(url.as_str(), false)?;
    Ok(url.origin().ascii_serialization())
}
pub(crate) fn socket_url(origin: &str) -> String {
    format!(
        "{}/tower",
        origin
            .replacen("https:", "wss:", 1)
            .replacen("http:", "ws:", 1)
    )
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 160
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
pub(crate) async fn begin(plan: &Plan) -> Result<Enrollment> {
    if !valid_pairing_secret(&plan.pairing_code.0) || !valid_pairing_secret(&plan.request_id) {
        return Err(enrollment_error(
            "invalid_params",
            "Invalid pairing credentials",
        ));
    }
    let response = http_client()?
        .post(format!("{}/api/tower/enrollments", plan.origin))
        .json(&plan.body())
        .send()
        .await
        .map_err(|_| enrollment_error("enrollment_unavailable", "Cannot reach the private Allgather origin; the saved pairing request can be retried"))?;
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        || response.status().is_server_error()
    {
        return Err(enrollment_error(
            "enrollment_unavailable",
            "Allgather is temporarily unable to pair this machine",
        ));
    }
    if !response.status().is_success() {
        return Err(enrollment_error(
            "pairing_rejected",
            "Allgather refused the pairing code or its bound request; obtain a valid invitation from the owner",
        ));
    }
    let started: Enrollment = serde_json::from_value(
        json_response(response)
            .await
            .map_err(|_| enrollment_error("enrollment_failed", "Invalid pairing response"))?,
    )
    .map_err(|_| enrollment_error("enrollment_failed", "Invalid pairing response"))?;
    let url = reqwest::Url::parse(&started.authorization_url)
        .map_err(|_| enrollment_error("enrollment_failed", "Invalid owner status URL"))?;
    if !identifier(&started.enrollment_id)
        || started.confirmation_token.0.len() < 16
        || started.confirmation_token.0.len() > 4096
        || started.confirmation_token.0.chars().any(char::is_control)
        || url.origin().ascii_serialization() != plan.origin
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.path() != format!("/tower/enroll/{}", started.enrollment_id)
        || url.query().is_some()
        || started.authorization_url.chars().any(char::is_control)
        || started
            .authorization_url
            .contains(&started.confirmation_token.0)
        || started.authorization_url.contains(&plan.pairing_code.0)
        || [&started.enrollment_id, &started.expires_at]
            .iter()
            .any(|text| {
                text.contains(&started.confirmation_token.0) || text.contains(&plan.pairing_code.0)
            })
        || expires_at(&started.expires_at).is_err()
    {
        return Err(enrollment_error(
            "enrollment_failed",
            "Unsafe or invalid pairing response",
        ));
    }
    Ok(started)
}
pub(crate) async fn confirm(c: &Confirmation) -> Result<()> {
    let response = http_client()?
        .post(format!(
            "{}/api/tower/enrollments/{}/confirm",
            c.origin, c.enrollment_id
        ))
        .bearer_auth(&c.confirmation_token.0)
        .json(&json!({"credential_id":c.credential_id,"access_key_hash":c.access_key_hash}))
        .send()
        .await
        .map_err(|_| Error::Other("Enrollment confirmation failed".into()))?;
    if !response.status().is_success() {
        return Err(Error::Other(
            "Allgather refused enrollment confirmation".into(),
        ));
    }
    let response = json_response(response).await?;
    if response["state"] != "confirmed"
        || response["credential_id"] != c.credential_id
        || response["machine_id"] != c.machine_id
    {
        return Err(Error::Other(
            "Enrollment confirmation does not match the saved credential".into(),
        ));
    }
    Ok(())
}
pub(crate) fn key_hash(key: &Secret) -> String {
    Sha256::digest(key.0.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn enrollment_error(code: &str, message: &str) -> Error {
    tower_protocol::RpcError::application(code, message).into()
}
pub(crate) fn configured_origin(
    paths: &tower_pal::Paths,
    supplied: Option<&str>,
) -> Result<String> {
    if let Some(url) = supplied {
        return origin(url);
    }
    if let Ok(bytes) = tower_pal::read_private(&paths.state.join("allgather-server")) {
        return origin(
            std::str::from_utf8(&bytes)
                .map_err(|_| Error::Other("Invalid enrollment binding".into()))?,
        );
    }
    origin(&Config::load(paths)?.url)
}
