mod broker;
pub mod client;
pub mod shim;
pub use broker::{Broker, LeaseGuard, ProviderGuard, ProviderRequest, Session};
pub type Result<T> = std::result::Result<T, tower_protocol::RpcError>;
pub(crate) fn error(code: &str, message: &str) -> tower_protocol::RpcError {
    tower_protocol::RpcError::application(code, message)
}
pub(crate) fn unavailable() -> tower_protocol::RpcError {
    error(
        "clipboard_unavailable",
        "No authorized clipboard image is available",
    )
}
pub(crate) fn internal(_: impl std::fmt::Display) -> tower_protocol::RpcError {
    error("clipboard_io", "Clipboard transfer failed")
}
fn random(prefix: &str) -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(internal)?;
    Ok(format!(
        "{prefix}{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ))
}
