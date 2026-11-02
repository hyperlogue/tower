use crate::{Result, error};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

pub async fn read<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> Result<T> {
    let n = r
        .read_u32()
        .await
        .map_err(|_| error("runtime_unavailable", "Runtime connection closed"))?
        as usize;
    if n > tower_protocol::MAX_FRAME_BYTES {
        return Err(error("too_large", "Runtime frame exceeds limit"));
    }
    let mut bytes = vec![0; n];
    r.read_exact(&mut bytes)
        .await
        .map_err(|_| error("runtime_unavailable", "Incomplete runtime frame"))?;
    serde_json::from_slice(&bytes).map_err(|_| error("invalid_params", "Invalid runtime frame"))
}
pub async fn write<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, v: &T) -> Result<()> {
    let bytes = serde_json::to_vec(v).map_err(crate::internal)?;
    if bytes.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(error("too_large", "Runtime frame exceeds limit"));
    }
    w.write_u32(bytes.len() as u32)
        .await
        .map_err(crate::internal)?;
    w.write_all(&bytes).await.map_err(crate::internal)?;
    w.flush().await.map_err(crate::internal)
}
pub fn call<T: Serialize, U: DeserializeOwned>(
    stream: &mut tower_pal::BlockingIpcStream,
    v: &T,
) -> Result<U> {
    let bytes = serde_json::to_vec(v).map_err(crate::internal)?;
    if bytes.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(error("too_large", "Runtime frame exceeds limit"));
    }
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| stream.write_all(&bytes))
        .map_err(|_| error("runtime_unavailable", "Runtime write failed"))?;
    let mut n = [0; 4];
    stream
        .read_exact(&mut n)
        .map_err(|_| error("runtime_unavailable", "Runtime read failed"))?;
    let n = u32::from_be_bytes(n) as usize;
    if n > tower_protocol::MAX_FRAME_BYTES {
        return Err(error("too_large", "Runtime reply exceeds limit"));
    }
    let mut bytes = vec![0; n];
    stream
        .read_exact(&mut bytes)
        .map_err(|_| error("runtime_unavailable", "Runtime reply incomplete"))?;
    serde_json::from_slice(&bytes).map_err(crate::internal)
}
pub async fn line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<Option<serde_json::Value>> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(crate::internal)?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(error("native_protocol", "Truncated native message"))
            };
        }
        let n = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|n| n + 1)
            .unwrap_or(available.len());
        if bytes.len() + n > tower_protocol::MAX_FRAME_BYTES {
            return Err(error("native_protocol", "Native message exceeds limit"));
        }
        let done = available[n - 1] == b'\n';
        bytes.extend_from_slice(&available[..n]);
        reader.consume(n);
        if done {
            return serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| error("native_protocol", "Invalid native JSON"));
        }
    }
}
pub async fn json_line<W: AsyncWrite + Unpin>(w: &mut W, v: &serde_json::Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(v).map_err(crate::internal)?;
    if bytes.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(error("too_large", "Native request exceeds limit"));
    }
    bytes.push(b'\n');
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        w.write_all(&bytes).await?;
        w.flush().await
    })
    .await
    .map_err(|_| error("dispatch_unknown", "Native write timed out"))?
    .map_err(|_| error("dispatch_unknown", "Native write failed"))
}
