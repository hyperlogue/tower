use crate::Result;
use clap::Subcommand;
use std::path::PathBuf;
use tower_protocol::{ClipboardOffer, ImageFormat};

#[derive(Subcommand)]
pub enum Command {
    /// Keep a provider connection open. The reader writes image bytes to stdout.
    Provide {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "image/png", value_parser = ["image/png", "image/bmp"], value_delimiter = ',')]
        format: Vec<String>,
        #[arg(long, help = "Write connection credentials to a private JSON file")]
        credentials: PathBuf,
        #[arg(last = true, required = true)]
        reader: Vec<String>,
    },
    Status {
        id: String,
    },
    /// Read the current image from within a managed agent environment.
    Read {
        #[arg(long, default_value = "image/png", value_parser = ["image/png", "image/bmp"])]
        format: String,
    },
}
pub async fn run(
    command: Command,
    paths: &tower_pal::Paths,
    client: &mut towerd::client::Client,
) -> Result<()> {
    match command {
        Command::Provide {
            name,
            format,
            credentials,
            reader,
        } => {
            let credentials = tower_pal::cwd()?.join(credentials);
            if credentials.exists() {
                return Err("Credential file already exists; use a new path".into());
            }
            let offer = ClipboardOffer {
                name,
                formats: format
                    .into_iter()
                    .map(|f| {
                        if f == "image/png" {
                            ImageFormat::Png
                        } else {
                            ImageFormat::Bmp
                        }
                    })
                    .collect(),
            };
            let mut published = false;
            let result = tower_clipboard::client::provide(
                &paths.socket,
                offer,
                reader,
                crate::context()?,
                |client| {
                    let data = serde_json::to_vec(client).map_err(|_| {
                        tower_protocol::RpcError::application(
                            "clipboard_io",
                            "Cannot encode credentials",
                        )
                    })?;
                    tower_pal::write_new_private(&credentials, &data).map_err(|_| {
                        tower_protocol::RpcError::application(
                            "clipboard_io",
                            "Cannot write private credentials",
                        )
                    })?;
                    published = true;
                    eprintln!("Clipboard provider ready: {}", client.client_id);
                    Ok(())
                },
            )
            .await;
            if published {
                let _ = tower_pal::remove_private(&credentials);
            }
            result?;
        }
        Command::Status { id } => crate::print_json(
            &client
                .call("clipboard.status", &serde_json::json!({"id":id}))
                .await?,
        )?,
        Command::Read { format } => {
            tower_clipboard::shim::run("wl-paste", vec!["--type".into(), format]).await?;
        }
    }
    Ok(())
}
