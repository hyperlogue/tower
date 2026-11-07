use crate::Result;
use std::{path::PathBuf, time::Duration};
use tower_protocol::{AttachInfo, ClipboardClient, ClipboardTicket, IdParams};
use tower_server::client::Client;

pub async fn run(client: &mut Client, id: &str) -> Result<()> {
    with_clipboard(client, id, None, false).await
}
pub async fn with_clipboard(
    client: &mut Client,
    id: &str,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<()> {
    let terminal = tower_pal::LocalTerminal::open()?;
    let bytes = if let Some(file) = file {
        let bytes = tower_pal::read_private(&file)?;
        if bytes.len() > 4096 {
            return Err("Clipboard credential is too large".into());
        }
        Some(bytes)
    } else if stdin {
        if tower_pal::is_terminal() {
            // The frontend waits for this marker before sending a secret over
            // an SSH PTY. LocalTerminal has already disabled terminal echo.
            eprintln!("tower: ready for clipboard credential");
            Some(
                tokio::time::timeout(Duration::from_secs(30), async {
                    let mut bytes = Vec::new();
                    let mut byte = [0];
                    loop {
                        if terminal.read(&mut byte).await? == 0 {
                            return Err(std::io::ErrorKind::UnexpectedEof.into());
                        }
                        if byte[0] == b'\n' {
                            return Ok::<_, std::io::Error>(bytes);
                        }
                        if bytes.len() == 4096 {
                            return Err(std::io::Error::other("Clipboard credential is too large"));
                        }
                        bytes.push(byte[0]);
                    }
                })
                .await??,
            )
        } else {
            Some(
                tokio::task::spawn_blocking(|| {
                    tower_pal::read_stdin_bounded(4096, Duration::from_secs(30))
                })
                .await??,
            )
        }
    } else {
        None
    };
    let mut params = serde_json::to_value(IdParams { id: id.into() })?;
    let mut ticket = None;
    if let Some(bytes) = bytes {
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if value.get("ticket").is_some() {
            ticket = Some(serde_json::from_value::<ClipboardTicket>(value)?);
        } else {
            let identity: ClipboardClient = serde_json::from_value(value)?;
            params["client_id"] = identity.client_id.into();
            params["client_token"] = identity.client_token.0.into();
        }
    }
    let result = client.call("session.attach_info", &params).await?;
    let mut info: AttachInfo = serde_json::from_value(serde_json::to_value(result)?)?;
    if ticket.is_some() {
        info.clipboard = ticket;
    }
    tower_terminal::attach_with_terminal(info, terminal).await
}
