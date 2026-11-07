type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use std::{
    io,
    path::Path,
    time::{Duration, Instant},
};
use tower_pal::LocalTerminal;
use tower_pool_proto::{self as wire, Frame, Reply, Request, Size};
use tower_protocol::AttachInfo;

pub async fn attach(info: AttachInfo) -> Result<()> {
    attach_with_terminal(info, LocalTerminal::open()?).await
}
pub async fn attach_with_terminal(info: AttachInfo, terminal: LocalTerminal) -> Result<()> {
    let mut socket = tower_pal::connect(Path::new(&info.pool_socket)).await?;
    let capabilities = tokio::time::timeout(
        Duration::from_secs(5),
        wire::hello_capabilities(&mut socket),
    )
    .await??;
    if info.clipboard.is_some() && !capabilities.iter().any(|c| c == "clipboard.images.v1") {
        return Err("This pool does not support clipboard attachments".into());
    }
    let size = dimensions(&terminal)?;
    wire::write(
        &mut socket,
        &Request::Attach {
            term: tower_pal::environment()
                .get("TERM")
                .cloned()
                .unwrap_or_else(|| "xterm-256color".into()),
            name: info.pool_session,
            size,
            clipboard: info.clipboard,
        },
    )
    .await?;
    let mut resize = tower_pal::terminal_resize_signal()?;
    let (mut read, mut write) = tokio::io::split(socket);
    let output = async {
        loop {
            match wire::read(&mut read).await? {
                Frame::Data(bytes) => terminal.write(&bytes).await?,
                Frame::Json(bytes) => {
                    match serde_json::from_slice::<Reply>(&bytes)
                        .map_err(io::Error::other)?
                        .checked()?
                    {
                        Reply::Exited { .. } => return Ok::<_, io::Error>(()),
                        Reply::Clipboard {
                            code: Some(code), ..
                        } => {
                            let code: String = code
                                .chars()
                                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                                .take(80)
                                .collect();
                            terminal.write(format!("\r\n[tower: image paste failed: {code}; pending input discarded]\r\n").as_bytes()).await?;
                        }
                        _ => {}
                    }
                }
                Frame::Unknown => {}
            }
        }
    };
    let input = async {
        let mut buf = [0; 8192];
        let mut detach = Detach::default();
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            let mut bytes = Vec::new();
            let end = tokio::select! {
                result = terminal.read(&mut buf) => {
                    let count = result?;
                    count == 0 || detach.push(&buf[..count], &mut bytes)
                },
                _ = resize.recv() => {
                    wire::write(&mut write, &Request::Resize { size: dimensions(&terminal)? }).await?;
                    false
                },
                _ = tick.tick() => { detach.expire(&mut bytes); false },
            };
            if !bytes.is_empty() {
                wire::data(&mut write, &bytes).await?;
            }
            if end {
                wire::write(&mut write, &Request::Detach).await?;
                return Ok::<_, io::Error>(());
            }
        }
    };
    // Keep the frame reader alive across input and resize events: cancelling a
    // partial length-prefixed read would lose stream alignment.
    tokio::select! {
        result = output => result?,
        result = input => result?,
        result = tower_pal::shutdown_signal() => result?,
    }
    Ok(())
}
fn dimensions(terminal: &LocalTerminal) -> io::Result<Size> {
    let (rows, cols) = terminal.size()?;
    Size { rows, cols }.bounded()
}

#[derive(Default)]
struct Detach {
    pending: Vec<u8>,
    paste: bool,
    prefix: Vec<u8>,
    deadline: Option<Instant>,
}
impl Detach {
    fn push(&mut self, input: &[u8], out: &mut Vec<u8>) -> bool {
        const START: &[&[u8]] = &[b"\0", b"\x1b[32;5u", b"\x1b[32;5:1u"];
        const RELEASE: &[&[u8]] = &[b"\x1b[32;5:3u", b"\x1b[32;1:3u"];
        const END: &[&[u8]] = &[b"\x11", b"\x1b[113;5u", b"\x1b[113;5:1u"];
        for &byte in input {
            self.pending.push(byte);
            self.deadline = Some(Instant::now() + Duration::from_millis(50));
            loop {
                let paste_marker = if self.paste {
                    b"\x1b[201~"
                } else {
                    b"\x1b[200~"
                };
                if self.pending == paste_marker {
                    out.append(&mut self.prefix);
                    out.append(&mut self.pending);
                    self.paste = !self.paste;
                    self.prefix.clear();
                    break;
                }
                if !self.paste {
                    if !self.prefix.is_empty() && RELEASE.contains(&self.pending.as_slice()) {
                        self.prefix.append(&mut self.pending);
                        self.deadline = Some(Instant::now() + Duration::from_secs(1));
                        break;
                    }
                    if !self.prefix.is_empty() && END.contains(&self.pending.as_slice()) {
                        return true;
                    }
                    if START.contains(&self.pending.as_slice()) {
                        if !self.prefix.is_empty() {
                            out.append(&mut self.prefix);
                        }
                        self.prefix = std::mem::take(&mut self.pending);
                        self.deadline = Some(Instant::now() + Duration::from_secs(1));
                        break;
                    }
                }
                let possible = paste_marker.starts_with(&self.pending)
                    || (!self.paste
                        && (START.iter().any(|s| s.starts_with(&self.pending))
                            || !self.prefix.is_empty()
                                && END
                                    .iter()
                                    .chain(RELEASE)
                                    .any(|s| s.starts_with(&self.pending))));
                if possible || self.pending.is_empty() {
                    break;
                }
                if !self.prefix.is_empty() {
                    out.append(&mut self.prefix);
                    self.prefix.clear();
                }
                out.push(self.pending.remove(0));
            }
        }
        false
    }
    fn expire(&mut self, out: &mut Vec<u8>) {
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            if !self.prefix.is_empty() {
                out.append(&mut self.prefix);
                self.prefix.clear();
            }
            out.append(&mut self.pending);
            self.deadline = None;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detach_handles_fragmented_kitty_keys_but_not_pasted_keys() {
        for keys in [
            b"\0\x11".as_slice(),
            b"\x1b[32;5u\x1b[113;5u",
            b"\x1b[32;5u\x1b[32;5:3u\x1b[113;5u",
        ] {
            let mut d = Detach::default();
            let mut out = Vec::new();
            for &b in &keys[..keys.len() - 1] {
                assert!(!d.push(&[b], &mut out));
            }
            assert!(d.push(&keys[keys.len() - 1..], &mut out));
            assert!(out.is_empty());
        }
        let mut d = Detach::default();
        let mut out = Vec::new();
        let paste = b"\x1b[200~\0\x11\x1b[201~";
        assert!(!d.push(paste, &mut out));
        assert_eq!(out, paste);
        let mut d = Detach::default();
        let mut out = Vec::new();
        let keys = b"\x1b[32;5uX";
        assert!(!d.push(keys, &mut out));
        assert_eq!(out, keys);
    }
}
