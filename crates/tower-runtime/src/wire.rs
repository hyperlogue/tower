use crate::{Result, error};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

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
