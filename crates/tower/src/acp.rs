use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tower_pal::Paths;
use tower_protocol::CallerContext;
use tower_server::client::Client;

pub(crate) async fn run(
    paths: &Paths,
    context: CallerContext,
    autostart: bool,
    harness: &str,
    cwd: &str,
) -> super::Result<()> {
    let mut writer = Client::connect(paths, context.clone(), autostart).await?;
    let opened: Value = serde_json::to_value(
        writer
            .call(
                "acp.open",
                &json!({"harness":harness,"cwd":std::fs::canonicalize(cwd)?}),
            )
            .await?,
    )?;
    let identity = json!({"runtime_id":opened["runtime_id"],"generation":opened["generation"]});
    let mut reader = Client::connect(paths, context.clone(), false).await?;
    let mut closer = Client::connect(paths, context, false).await?;
    let result: super::Result<()> = tokio::select! {
        result=async {
            let mut stdin=BufReader::new(tokio::io::stdin());
            let mut sequence=0u64;
            loop {
                let mut line=String::new();
                let n=(&mut stdin).take(tower_protocol::MAX_FRAME_BYTES as u64+2).read_line(&mut line).await?;
                if n==0 {return Ok(());}
                if n>tower_protocol::MAX_FRAME_BYTES+1 || !line.ends_with('\n') {return Err("ACP input must be a bounded newline-delimited frame".into());}
                line.pop();
                sequence+=1;
                let mut p=identity.clone();p["sequence"]=json!(sequence);p["frame"]=json!(line);
                writer.call("acp.send",&p).await?;
            }
        }=>result,
        result=async {
            let mut stdout=tokio::io::stdout();
            loop {
                let polled:Value=serde_json::to_value(reader.call("acp.poll",&identity).await?)?;
                let frames=polled["frames"].as_array().ok_or("ACP poll returned no frames")?;
                for frame in frames {
                    stdout.write_all(frame["frame"].as_str().ok_or("ACP output is not text")?.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;stdout.flush().await?;
                    let mut ack=identity.clone();ack["sequence"]=frame["sequence"].clone();
                    reader.call("acp.ack",&ack).await?;
                }
                if frames.is_empty() {
                    if polled["closed"]==true {return Ok(());}
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
            }
        }=>result,
    };
    let _ = closer.call("acp.close", &identity).await;
    result
}
