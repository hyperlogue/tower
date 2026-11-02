use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u16 = 1;
pub const MAX_FRAME: usize = 256 * 1024;
pub const DATA_CHUNK: usize = 32 * 1024;
pub const MAX_SNAPSHOT: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}
impl Default for Size {
    fn default() -> Self {
        Self { rows: 24, cols: 80 }
    }
}
impl Size {
    pub fn bounded(self) -> io::Result<Self> {
        if self.rows > 256 || self.cols > 512 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "terminal size exceeds 256 rows or 512 columns",
            ));
        }
        Ok(Self {
            rows: self.rows.max(1),
            cols: self.cols.max(1),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Launch {
    pub name: String,
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub size: Option<Size>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Hello {
        version: u16,
    },
    Create {
        launch: Launch,
    },
    List,
    Inject {
        name: String,
        proof: InputProof,
        text: String,
    },
    Attach {
        name: String,
        size: Size,
    },
    Resize {
        size: Size,
    },
    Kill {
        name: String,
    },
    Remove {
        name: String,
    },
    Snapshot {
        name: String,
    },
    Detach,
    Shutdown,
    #[serde(other)]
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
// Timestamps use the host monotonic clock, independent of wall-clock changes.
pub struct InputProof {
    pub pid: u32,
    pub pid_start_time: u64,
    pub idle_since: u64,
    pub checked_at: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub name: String,
    pub state: String,
    pub pid: Option<u32>,
    pub pid_start_time: Option<u64>,
    pub attach_count: u32,
    pub size: Size,
    pub exit_code: Option<i32>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Hello {
        version: u16,
        capabilities: Vec<String>,
    },
    Created {
        session: Session,
    },
    Sessions {
        sessions: Vec<Session>,
    },
    Attached {
        client_id: u64,
        size: Size,
    },
    Snapshot {
        size: Size,
        truncated: bool,
        limited: bool,
        bytes: usize,
    },
    Exited {
        name: String,
        code: i32,
    },
    Ok,
    End,
    Error {
        code: String,
        message: String,
    },
    #[serde(other)]
    Unknown,
}
impl Reply {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self::Error {
            code: code.into(),
            message: message.into(),
        }
    }
    pub fn checked(self) -> io::Result<Self> {
        if let Self::Error { code, message } = self {
            Err(io::Error::other(format!("{code}: {message}")))
        } else {
            Ok(self)
        }
    }
}
#[derive(Debug)]
pub enum Frame {
    Json(Vec<u8>),
    Data(Vec<u8>),
    Unknown,
}
impl Frame {
    pub fn json<T: serde::de::DeserializeOwned>(self) -> io::Result<T> {
        match self {
            Self::Json(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected control frame",
            )),
        }
    }
}
fn length(header: [u8; 4]) -> io::Result<usize> {
    let n = u32::from_be_bytes(header) as usize;
    if n == 0 || n > MAX_FRAME {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid pool frame length",
        ))
    } else {
        Ok(n)
    }
}
fn unpack(mut bytes: Vec<u8>) -> Frame {
    let kind = bytes.remove(0);
    match kind {
        0 => Frame::Json(bytes),
        1 => Frame::Data(bytes),
        _ => Frame::Unknown,
    }
}
fn packed(kind: u8, bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() + 1 > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "pool frame too large",
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() + 5);
    out.extend_from_slice(&((bytes.len() + 1) as u32).to_be_bytes());
    out.push(kind);
    out.extend_from_slice(bytes);
    Ok(out)
}
pub fn read_sync(reader: &mut impl Read) -> io::Result<Frame> {
    let mut head = [0; 4];
    reader.read_exact(&mut head)?;
    let mut bytes = vec![0; length(head)?];
    reader.read_exact(&mut bytes)?;
    Ok(unpack(bytes))
}
pub fn write_sync(writer: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    writer.write_all(&packed(
        0,
        &serde_json::to_vec(value).map_err(io::Error::other)?,
    )?)
}
pub async fn read(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Frame> {
    let mut head = [0; 4];
    reader.read_exact(&mut head).await?;
    let mut bytes = vec![0; length(head)?];
    reader.read_exact(&mut bytes).await?;
    Ok(unpack(bytes))
}
pub async fn write(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> io::Result<()> {
    writer
        .write_all(&packed(
            0,
            &serde_json::to_vec(value).map_err(io::Error::other)?,
        )?)
        .await
}
pub async fn data(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> io::Result<()> {
    for part in bytes.chunks(DATA_CHUNK) {
        writer.write_all(&packed(1, part)?).await?;
    }
    Ok(())
}
pub async fn hello(stream: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> io::Result<()> {
    write(stream, &Request::Hello { version: VERSION }).await?;
    match read(stream).await?.json::<Reply>()?.checked()? {
        Reply::Hello { version, .. } if version == VERSION => Ok(()),
        _ => Err(io::Error::other("incompatible pool protocol")),
    }
}
pub fn hello_sync(stream: &mut (impl Read + Write)) -> io::Result<()> {
    write_sync(stream, &Request::Hello { version: VERSION })?;
    match read_sync(stream)?.json::<Reply>()?.checked()? {
        Reply::Hello { version, .. } if version == VERSION => Ok(()),
        _ => Err(io::Error::other("incompatible pool protocol")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_lengths_before_allocation_and_ignores_unknown_controls() {
        assert!(read_sync(&mut [255, 255, 255, 255].as_slice()).is_err());
        assert!(read_sync(&mut [0, 0, 0, 0].as_slice()).is_err());
        assert!(matches!(
            serde_json::from_str::<Request>(r#"{"type":"future","new":true}"#).unwrap(),
            Request::Unknown
        ));
        assert_eq!(
            Size { rows: 0, cols: 0 }.bounded().unwrap(),
            Size { rows: 1, cols: 1 }
        );
    }
}
