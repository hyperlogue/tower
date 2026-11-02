use crate::Secret;
use serde::{Deserialize, Serialize};

pub const MAX_CLIPBOARD_BYTES: usize = 16 * 1024 * 1024;
pub const CLIPBOARD_CHUNK_BYTES: usize = 48 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageFormat {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/bmp")]
    Bmp,
}
impl ImageFormat {
    pub fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Bmp => "image/bmp",
        }
    }
    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Bmp => "bmp",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardOffer {
    pub name: String,
    pub formats: Vec<ImageFormat>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardClient {
    pub client_id: String,
    pub client_token: Secret,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardTicket {
    pub client_id: String,
    pub ticket: Secret,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardAttach {
    pub id: String,
    pub generation: String,
    pub client_id: String,
    pub ticket: Secret,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardLease {
    pub client_id: String,
    pub lease: Secret,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardChunk {
    pub transfer_id: String,
    pub sequence: u64,
    #[serde(default)]
    pub data: String,
    #[serde(default)]
    pub done: bool,
    #[serde(default)]
    pub unavailable: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClipboardImage {
    pub paste_id: String,
    pub path: String,
    pub format: ImageFormat,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum ClipboardProviderRequest {
    #[serde(rename = "clipboard.read")]
    Read {
        transfer_id: String,
        format: ImageFormat,
        max_bytes: usize,
    },
    #[serde(rename = "clipboard.cancel")]
    Cancel { transfer_id: String },
}
