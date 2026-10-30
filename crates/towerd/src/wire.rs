use serde::Serialize;
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tower_protocol::MAX_FRAME_BYTES;

/// Bound allocation before reading an entire newline-free request into memory.
pub(crate) async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    loop {
        let bytes = reader.fill_buf().await?;
        if bytes.is_empty() {
            return if out.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete RPC frame",
                ))
            };
        }
        let end = bytes.iter().position(|b| *b == b'\n');
        let take = end.map_or(bytes.len(), |i| i + 1);
        if out.len() + take > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RPC frame exceeds limit",
            ));
        }
        out.extend_from_slice(&bytes[..take]);
        reader.consume(take);
        if end.is_some() {
            return Ok(Some(out));
        }
    }
}
pub(crate) async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> io::Result<()> {
    let mut out = serde_json::to_vec(value).map_err(io::Error::other)?;
    out.push(b'\n');
    if out.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RPC result exceeds limit",
        ));
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), writer.write_all(&out))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RPC write timed out"))??;
    Ok(())
}
