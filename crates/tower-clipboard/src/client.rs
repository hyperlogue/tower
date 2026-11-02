use crate::{Result, error, internal};
use base64::Engine;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
};
use tower_protocol::{
    CLIPBOARD_CHUNK_BYTES, CallerContext, ClipboardAttach, ClipboardChunk, ClipboardClient,
    ClipboardLease, ClipboardOffer, ImageFormat, MAX_CLIPBOARD_BYTES,
};

async fn line<R: tokio::io::AsyncBufRead + Unpin>(r: &mut R) -> Result<Option<Value>> {
    let mut bytes = Vec::new();
    loop {
        let part = r.fill_buf().await.map_err(internal)?;
        if part.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(internal("incomplete frame"))
            };
        }
        let end = part.iter().position(|b| *b == b'\n');
        let n = end.map_or(part.len(), |i| i + 1);
        if bytes.len() + n > tower_protocol::MAX_FRAME_BYTES {
            return Err(error(
                "too_large",
                "Clipboard control frame exceeds its limit",
            ));
        }
        bytes.extend_from_slice(&part[..n]);
        r.consume(n);
        if end.is_some() {
            return serde_json::from_slice(&bytes).map(Some).map_err(internal);
        }
    }
}
async fn write<W: tokio::io::AsyncWrite + Unpin>(w: &mut W, v: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(v).map_err(internal)?;
    bytes.push(b'\n');
    if bytes.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(error(
            "too_large",
            "Clipboard control frame exceeds its limit",
        ));
    }
    tokio::time::timeout(Duration::from_secs(5), w.write_all(&bytes))
        .await
        .map_err(|_| internal("write timeout"))?
        .map_err(internal)
}
fn response<T: DeserializeOwned>(v: Value) -> Result<T> {
    if let Some(e) = v.get("error") {
        return Err(serde_json::from_value(e.clone()).map_err(internal)?);
    }
    serde_json::from_value(
        v.get("result")
            .cloned()
            .ok_or_else(|| internal("missing result"))?,
    )
    .map_err(internal)
}
pub async fn call<T: DeserializeOwned>(
    socket: &Path,
    method: &str,
    params: impl Serialize,
    context: &CallerContext,
) -> Result<T> {
    let mut stream = tower_pal::connect(socket).await.map_err(internal)?;
    write(&mut stream,&json!({"jsonrpc":"2.0","id":"clipboard","method":method,"params":params,"context":context})).await?;
    let v = tokio::time::timeout(Duration::from_secs(35), line(&mut BufReader::new(stream)))
        .await
        .map_err(|_| error("clipboard_timeout", "Clipboard operation timed out"))??
        .ok_or_else(|| internal("closed"))?;
    if v["id"] != "clipboard" {
        return Err(internal("wrong reply"));
    }
    response(v)
}
pub async fn attach(
    socket: &Path,
    params: &ClipboardAttach,
) -> Result<(ClipboardLease, tower_pal::IpcStream)> {
    let mut stream = tower_pal::connect(socket).await.map_err(internal)?;
    write(&mut stream,&json!({"jsonrpc":"2.0","id":"clipboard-attach","method":"clipboard.attach","params":params})).await?;
    let mut reader = BufReader::new(stream);
    let v = tokio::time::timeout(Duration::from_secs(5), line(&mut reader))
        .await
        .map_err(|_| internal("attach timeout"))??
        .ok_or_else(|| internal("closed"))?;
    if v["id"] != "clipboard-attach" {
        return Err(internal("wrong reply"));
    }
    Ok((response(v)?, reader.into_inner()))
}
struct Tasks(Vec<tokio::task::AbortHandle>);
impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

pub async fn provide(
    socket: &Path,
    offer: ClipboardOffer,
    reader_command: Vec<String>,
    context: CallerContext,
    on_registered: impl FnOnce(&ClipboardClient) -> Result<()>,
) -> Result<()> {
    if reader_command.is_empty() {
        return Err(error(
            "invalid_params",
            "A clipboard reader command is required",
        ));
    }
    let mut stream = tower_pal::connect(socket).await.map_err(internal)?;
    write(&mut stream,&json!({"jsonrpc":"2.0","id":"offer","method":"clipboard.offer","params":offer,"context":context})).await?;
    let mut stream = BufReader::new(stream);
    let registered: ClipboardClient =
        response(line(&mut stream).await?.ok_or_else(|| internal("closed"))?)?;
    on_registered(&registered)?;
    let (read, mut write_half) = tokio::io::split(stream);
    let mut input = BufReader::new(read);
    let (writer, mut outgoing) = mpsc::channel::<ClipboardChunk>(4);
    // Only this task writes frames. Cancelling a reader cannot leave a partial
    // NDJSON frame on a connection that another transfer will reuse.
    let mut output = tokio::spawn(async move {
        while let Some(chunk) = outgoing.recv().await {
            write(
                &mut write_half,
                &json!({"jsonrpc":"2.0","method":"clipboard.chunk","params":chunk}),
            )
            .await?;
        }
        Ok::<_, tower_protocol::RpcError>(())
    });
    let limit = Arc::new(Semaphore::new(1));
    let mut tasks = HashMap::<String, tokio::task::JoinHandle<()>>::new();
    let mut guard = Tasks(vec![output.abort_handle()]);
    loop {
        let v = tokio::select! {v=line(&mut input)=>v?,result=&mut output=>{return result.map_err(internal)?;},r=tower_pal::shutdown_signal()=>{r.map_err(internal)?;return Ok(());}};
        let Some(v) = v else {
            return Ok(());
        };
        tasks.retain(|_, h| !h.is_finished());
        guard.0.retain(|h| !h.is_finished());
        match v["method"].as_str() {
            Some("clipboard.cancel") => {
                if let Some(id) = v["params"]["transfer_id"].as_str()
                    && let Some(task) = tasks.remove(id)
                {
                    task.abort();
                }
            }
            Some("clipboard.read") => {
                let id = v["params"]["transfer_id"]
                    .as_str()
                    .filter(|id| id.len() <= 160 && !id.chars().any(char::is_control))
                    .ok_or_else(|| internal("invalid transfer identity"))?
                    .to_owned();
                let format: ImageFormat =
                    serde_json::from_value(v["params"]["format"].clone()).map_err(internal)?;
                let writer = writer.clone();
                let permit = limit.clone().try_acquire_owned();
                let command = reader_command.clone();
                let task_id = id.clone();
                let task = tokio::spawn(async move {
                    let image = if let Ok(_permit) = permit {
                        let mut env = tower_pal::environment();
                        env.insert("TOWER_CLIPBOARD_FORMAT".into(), format.mime().into());
                        match tower_pal::cwd() {
                            Ok(cwd) => tower_pal::capture(
                                &command,
                                &env,
                                &cwd,
                                Duration::from_secs(20),
                                MAX_CLIPBOARD_BYTES,
                            )
                            .await
                            .ok()
                            .filter(|(code, bytes)| *code == 0 && !bytes.is_empty())
                            .map(|(_, b)| b),
                            Err(_) => None,
                        }
                    } else {
                        None
                    };
                    if let Some(bytes) = image {
                        for (sequence, chunk) in bytes.chunks(CLIPBOARD_CHUNK_BYTES).enumerate() {
                            let p = ClipboardChunk {
                                transfer_id: task_id.clone(),
                                sequence: sequence as u64,
                                data: base64::engine::general_purpose::STANDARD.encode(chunk),
                                done: false,
                                unavailable: false,
                            };
                            if writer.send(p).await.is_err() {
                                return;
                            }
                        }
                        let p = ClipboardChunk {
                            transfer_id: task_id,
                            sequence: bytes.len().div_ceil(CLIPBOARD_CHUNK_BYTES) as u64,
                            data: String::new(),
                            done: true,
                            unavailable: false,
                        };
                        let _ = writer.send(p).await;
                    } else {
                        let p = ClipboardChunk {
                            transfer_id: task_id,
                            sequence: 0,
                            data: String::new(),
                            done: true,
                            unavailable: true,
                        };
                        let _ = writer.send(p).await;
                    }
                });
                guard.0.push(task.abort_handle());
                if let Some(old) = tasks.insert(id, task) {
                    old.abort();
                }
            }
            _ => {}
        }
    }
}
