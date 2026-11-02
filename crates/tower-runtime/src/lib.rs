mod backend;
mod client;
mod controls;
mod engine;
mod host;
mod journal;
mod requests;
mod wire;
pub use backend::Backend;
pub mod bridge;
pub use client::Client;
pub use host::run;
pub(crate) use journal::{Binding, Journal, Limits};
use tower_protocol::RpcError;

pub type Result<T> = std::result::Result<T, RpcError>;
fn error(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}
fn internal(_: impl std::fmt::Display) -> RpcError {
    error("runtime_storage", "Runtime journal is unavailable")
}
fn timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}
fn millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn parse_time(value: &str) -> Result<u64> {
    let parsed = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| error("invalid_params", "Expected an ISO timestamp with timezone"))?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| error("invalid_params", "Invalid timestamp"))
}
fn random_id(prefix: &str) -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| error("internal_error", "Random source unavailable"))?;
    Ok(format!(
        "{prefix}{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ))
}
fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
}
pub(crate) fn command(
    binding: &Binding,
    kind: &str,
    payload: serde_json::Value,
) -> Result<tower_protocol::ExecutionCommand> {
    Ok(tower_protocol::ExecutionCommand {
        command_id: random_id("command_")?,
        session_id: binding.session_id.clone(),
        runtime_id: binding.runtime_id.clone(),
        generation: binding.generation.clone(),
        kind: kind.into(),
        payload: serde_json::from_value(payload).map_err(internal)?,
        expires_at: (time::OffsetDateTime::now_utc() + time::Duration::minutes(10))
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(internal)?,
    })
}
