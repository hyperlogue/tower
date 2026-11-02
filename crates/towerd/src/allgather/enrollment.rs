use super::{Config, Error, Result, http_client, json_response, safe_url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tower_protocol::Secret;

#[derive(Clone)]
pub(crate) struct Plan {
    pub actor: String,
    pub origin: String,
    pub host_id: String,
    pub scopes: Vec<String>,
    pub label: Option<String>,
}
#[derive(Clone, Deserialize)]
pub(crate) struct Started {
    pub enrollment_id: String,
    pub authorization_url: String,
    pub polling_token: Secret,
    pub expires_at: String,
    pub interval: u64,
}
#[derive(Clone)]
pub(crate) struct Pending {
    pub plan: Plan,
    pub started: Started,
    pub deadline: std::time::Instant,
    pub next_poll: std::time::Instant,
    pub result: Option<serde_json::Value>,
}
#[derive(Deserialize)]
pub(crate) struct Poll {
    pub state: String,
    pub interval: Option<u64>,
    pub machine_id: Option<String>,
    pub credential_id: Option<String>,
    pub access_key: Option<Secret>,
    pub node_label: Option<String>,
    pub scopes: Option<Vec<String>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Consent {
    pub origin: String,
    pub machine_id: String,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Confirmation {
    pub origin: String,
    pub enrollment_id: String,
    pub polling_token: Secret,
    pub credential_id: String,
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
pub(crate) async fn begin(plan: &Plan) -> Result<Started> {
    let mut body = json!({"host_id":plan.host_id,"tower_version":env!("CARGO_PKG_VERSION"),"requested_scopes":plan.scopes});
    if let Some(label) = &plan.label {
        body["node_label"] = label.clone().into();
    }
    let response = http_client()?
        .post(format!("{}/api/tower/enrollments", plan.origin))
        .json(&body)
        .send()
        .await
        .map_err(|_| Error::Other("Cannot start browser enrollment".into()))?;
    if !response.status().is_success() {
        return Err(Error::Other("Allgather refused machine enrollment".into()));
    }
    let started: Started = serde_json::from_value(json_response(response).await?)
        .map_err(|_| Error::Other("Invalid enrollment response".into()))?;
    let url = reqwest::Url::parse(&started.authorization_url)
        .map_err(|_| Error::Other("Invalid authorization URL".into()))?;
    if !identifier(&started.enrollment_id)
        || started.polling_token.0.len() < 16
        || started.polling_token.0.len() > 4096
        || started.polling_token.0.chars().any(char::is_control)
        || url.origin().ascii_serialization() != plan.origin
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.path() != format!("/tower/enroll/{}", started.enrollment_id)
        || url.query().is_some()
        || started.authorization_url.chars().any(char::is_control)
        || started.authorization_url.contains(&started.polling_token.0)
        || !(1..=30).contains(&started.interval)
    {
        return Err(Error::Other("Unsafe or invalid enrollment response".into()));
    }
    Ok(started)
}
pub(crate) async fn poll(pending: &Pending) -> Result<Poll> {
    let response = http_client()?
        .post(format!(
            "{}/api/tower/enrollments/{}/poll",
            pending.plan.origin, pending.started.enrollment_id
        ))
        .bearer_auth(&pending.started.polling_token.0)
        .json(&json!({}))
        .send()
        .await
        .map_err(|_| Error::Other("Enrollment polling failed".into()))?;
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Ok(Poll {
            state: "pending".into(),
            interval: Some(10),
            machine_id: None,
            credential_id: None,
            access_key: None,
            node_label: None,
            scopes: None,
        });
    }
    if !response.status().is_success() {
        return Err(Error::Other("Allgather refused enrollment polling".into()));
    }
    serde_json::from_value(json_response(response).await?)
        .map_err(|_| Error::Other("Invalid enrollment polling response".into()))
}
pub(crate) async fn confirm(c: &Confirmation) -> Result<()> {
    let response = http_client()?
        .post(format!(
            "{}/api/tower/enrollments/{}/confirm",
            c.origin, c.enrollment_id
        ))
        .bearer_auth(&c.polling_token.0)
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
    if response["state"] != "confirmed" || response["credential_id"] != c.credential_id {
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
pub(crate) async fn recover_confirmation(paths: &tower_pal::Paths) -> Result<()> {
    let file = paths.state.join("allgather-enrollment.json");
    let bytes = match tower_pal::read_private(&file) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let c: Confirmation = serde_json::from_slice(&bytes)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    if now > c.expires_at_ms {
        tower_pal::remove_private(&file)?;
        return Ok(());
    }
    let key = tower_pal::read_private(&paths.state.join("allgather.key"))?;
    let binding = tower_pal::read_private(&paths.state.join("allgather-server"))?;
    if key_hash(&Secret(
        String::from_utf8(key).map_err(|_| Error::Other("Invalid saved credential".into()))?,
    )) != c.access_key_hash
        || binding != socket_url(&c.origin).as_bytes()
    {
        return Err(Error::Other(
            "Enrollment confirmation no longer matches local state".into(),
        ));
    }
    confirm(&c).await?;
    tower_pal::remove_private(&file)?;
    Ok(())
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
