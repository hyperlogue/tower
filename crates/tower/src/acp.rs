use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tower_pal::Paths;
use tower_protocol::CallerContext;
use tower_server::{Error, client::Client};

pub(crate) async fn run(
    paths: &Paths,
    context: CallerContext,
    autostart: bool,
    harness: &str,
    cwd: &str,
) -> super::Result<()> {
    let mut writer = Client::connect(paths, context.clone(), autostart).await?;
    let mut reader = Client::connect(paths, context.clone(), false).await?;
    let mut closer = Client::connect(paths, context, false).await?;
    let identity = serde_json::to_value(
        writer
            .call(
                "acp.connect",
                &json!({"harness":harness,"cwd":std::fs::canonicalize(cwd)?}),
            )
            .await?,
    )?;
    let result = tokio::select! {
        result = send_input(&mut writer, &identity) => result,
        result = receive_output(&mut reader, &identity) => result,
    };
    let _ = closer.call("acp.close", &identity).await;
    result
}

async fn send_input(writer: &mut Client, identity: &Value) -> super::Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut sequence = 0u64;
    loop {
        let mut line = String::new();
        let n = (&mut stdin)
            .take(tower_protocol::MAX_FRAME_BYTES as u64 + 2)
            .read_line(&mut line)
            .await?;
        if n == 0 {
            return Ok(());
        }
        if n > tower_protocol::MAX_FRAME_BYTES + 1 || !line.ends_with('\n') {
            return Err("ACP input must be a bounded newline-delimited frame".into());
        }
        line.pop();
        sequence += 1;
        let mut params = identity.clone();
        params["sequence"] = json!(sequence);
        params["frame"] = json!(line);
        loop {
            match writer.call("acp.send", &params).await {
                Ok(_) => break,
                Err(Error::Rpc(error)) if error.data.code == "acp_backpressure" => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

async fn receive_output(reader: &mut Client, identity: &Value) -> super::Result<()> {
    let mut stdout = tokio::io::stdout();
    loop {
        let polled = serde_json::to_value(reader.call("acp.poll", identity).await?)?;
        let frames = polled["frames"]
            .as_array()
            .ok_or("ACP poll returned no frames")?;
        for frame in frames {
            stdout
                .write_all(
                    frame["frame"]
                        .as_str()
                        .ok_or("ACP output is not text")?
                        .as_bytes(),
                )
                .await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
            let mut ack = identity.clone();
            ack["sequence"] = frame["sequence"].clone();
            reader.call("acp.ack", &ack).await?;
        }
        if frames.is_empty() {
            if polled["closed"] == true {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
