use crate::input::Unit;
use serde_json::json;
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
};
use tokio::sync::mpsc;
use tower_clipboard::client::call;
use tower_pool_proto::{ClipboardConfig, ClipboardMode};
use tower_protocol::{CallerContext, ClipboardImage, ClipboardLease};

type Result<T> = std::result::Result<T, tower_protocol::RpcError>;
pub enum Event {
    Ready {
        id: u64,
        result: Result<ClipboardImage>,
    },
    Done {
        id: u64,
        result: Result<()>,
    },
}
pub enum Action {
    Input {
        id: u64,
        bytes: Vec<u8>,
    },
    Finished {
        id: u64,
        code: Option<String>,
        input: Vec<Vec<u8>>,
    },
    Ignore,
}
struct Active {
    id: u64,
    key: Vec<u8>,
    image: Option<ClipboardImage>,
    task: tokio::task::AbortHandle,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct BoundClient {
    lease: ClipboardLease,
    _connection: tower_pal::IpcStream,
}
pub struct Clipboard {
    config: Option<ClipboardConfig>,
    name: String,
    clients: BTreeMap<u64, BoundClient>,
    waiting: BTreeMap<u64, VecDeque<Unit>>,
    queue: VecDeque<(u64, Vec<u8>)>,
    bytes: usize,
    active: Option<Active>,
    send: mpsc::Sender<Event>,
    pub receive: mpsc::Receiver<Event>,
}
impl Clipboard {
    pub fn new(name: String, config: Option<ClipboardConfig>) -> Self {
        let (send, receive) = mpsc::channel(32);
        Self {
            config,
            name,
            clients: BTreeMap::new(),
            waiting: BTreeMap::new(),
            queue: VecDeque::new(),
            bytes: 0,
            active: None,
            send,
            receive,
        }
    }
    pub fn bind(&mut self, id: u64, lease: ClipboardLease, connection: tower_pal::IpcStream) {
        self.clients.insert(
            id,
            BoundClient {
                lease,
                _connection: connection,
            },
        );
    }
    pub fn detached(&mut self, id: u64) {
        self.clients.remove(&id);
        self.queue.retain(|(client, _)| *client != id);
        if let Some(q) = self.waiting.remove(&id) {
            self.bytes -= q.iter().map(|unit| unit.bytes.len()).sum::<usize>();
        }
        if self.active.as_ref().is_some_and(|a| a.id == id) {
            self.active = None;
        }
        self.start();
    }
    pub fn clear(&mut self) {
        self.active = None;
        self.waiting.clear();
        self.queue.clear();
        self.clients.clear();
        self.bytes = 0;
    }
    pub fn push(
        &mut self,
        id: u64,
        units: Vec<Unit>,
        budget: usize,
    ) -> std::result::Result<Vec<Vec<u8>>, String> {
        let mut output = Vec::new();
        let mut output_bytes = 0;
        for unit in units {
            if let Some(wait) = self.waiting.get_mut(&id) {
                if self.bytes + unit.bytes.len() + output_bytes > budget || wait.len() >= 4096 {
                    return Err("input_overflow".into());
                }
                self.bytes += unit.bytes.len();
                wait.push_back(unit);
            } else if self.config.is_some() && !unit.paste && paste_key(&unit.bytes) {
                self.waiting.insert(id, VecDeque::new());
                self.queue.push_back((id, unit.bytes));
            } else {
                output_bytes += unit.bytes.len();
                output.push(unit.bytes);
            }
        }
        self.start();
        Ok(output)
    }
    fn start(&mut self) {
        if self.active.is_some() {
            return;
        }
        if let Some((id, key)) = self.queue.pop_front() {
            let lease = self.clients.get(&id).map(|c| c.lease.clone());
            let Some(config) = self.config.clone() else {
                return;
            };
            let tx = self.send.clone();
            let name = self.name.clone();
            let task = tokio::spawn(async move {
                let result=async{
                    let lease = lease.ok_or_else(|| tower_protocol::RpcError::application("clipboard_unavailable", "This attachment has no clipboard provider"))?;
                    let image:ClipboardImage=call(Path::new(&config.socket),"clipboard.fetch",json!({"lease":lease.lease}),&CallerContext::default()).await?;
                    let image:ClipboardImage=call(Path::new(&config.socket),"clipboard.claim",json!({"lease":lease.lease,"paste_id":image.paste_id}),&CallerContext::default()).await?;
                    if config.mode==ClipboardMode::NativeKey {let _:serde_json::Value=call(Path::new(&config.socket),"clipboard.activate",json!({"id":name,"generation":config.generation,"lease":lease.lease,"paste_id":image.paste_id}),&CallerContext::default()).await?;}
                    Ok(image)
                }.await;
                let _ = tx.send(Event::Ready { id, result }).await;
            });
            self.active = Some(Active {
                id,
                key,
                image: None,
                task: task.abort_handle(),
            });
        }
    }
    pub fn event(&mut self, event: Event, budget: usize) -> Action {
        match event {
            Event::Ready { id, result } => {
                if !self.active.as_ref().is_some_and(|a| a.id == id) {
                    return Action::Ignore;
                }
                match result {
                    Ok(image) => {
                        let active = self.active.as_mut().unwrap();
                        let bytes = if self.config.as_ref().unwrap().mode == ClipboardMode::FileUrl
                        {
                            file_input(&image.path)
                        } else {
                            active.key.clone()
                        };
                        active.image = Some(image);
                        Action::Input { id, bytes }
                    }
                    Err(e) => self.finish(id, Some(e.data.code), budget),
                }
            }
            Event::Done { id, result } => {
                if self.active.as_ref().is_some_and(|a| a.id == id) {
                    self.finish(id, result.err().map(|e| e.data.code), budget)
                } else {
                    Action::Ignore
                }
            }
        }
    }
    fn finish(&mut self, id: u64, mut code: Option<String>, budget: usize) -> Action {
        self.active = None;
        let units = self.waiting.remove(&id).unwrap_or_default();
        self.bytes -= units.iter().map(|unit| unit.bytes.len()).sum::<usize>();
        let input = if code.is_none() {
            match self.push(id, units.into(), budget) {
                Ok(v) => v,
                Err(e) => {
                    code = Some(e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        self.start();
        Action::Finished { id, code, input }
    }
    pub fn submitted(&mut self, id: u64) {
        let Some(active) = self.active.as_mut().filter(|a| a.id == id) else {
            return;
        };
        let Some(image) = active.image.clone() else {
            return;
        };
        let Some(lease) = self.clients.get(&id).map(|c| c.lease.clone()) else {
            return;
        };
        let config = self.config.clone().unwrap();
        let tx = self.send.clone();
        let task = tokio::spawn(async move {
            let params = json!({"lease":lease.lease,"paste_id":image.paste_id});
            let result = if config.mode == ClipboardMode::NativeKey {
                call::<serde_json::Value>(
                    Path::new(&config.socket),
                    "clipboard.wait",
                    &params,
                    &CallerContext::default(),
                )
                .await
                .map(|_| ())
            } else {
                Ok(())
            };
            let _ = call::<serde_json::Value>(
                Path::new(&config.socket),
                "clipboard.finish",
                json!({"lease":lease.lease,"paste_id":image.paste_id,"success":result.is_ok()}),
                &CallerContext::default(),
            )
            .await;
            let _ = tx.send(Event::Done { id, result }).await;
        });
        active.task = task.abort_handle();
    }
}
pub fn paste_key(unit: &[u8]) -> bool {
    if unit == b"\x16" || unit == b"\x1b[27;5;118~" {
        return true;
    }
    let Some(body) = unit
        .strip_prefix(b"\x1b[")
        .and_then(|s| s.strip_suffix(b"u"))
        .and_then(|s| std::str::from_utf8(s).ok())
    else {
        return false;
    };
    let Some((key, mods)) = body.split_once(';') else {
        return false;
    };
    let fields = key.split(':').collect::<Vec<_>>();
    if fields.first() != Some(&"118")
        || fields.len() > 3
        || fields[1..]
            .iter()
            .any(|f| !f.is_empty() && f.parse::<u32>().is_err())
    {
        return false;
    }
    let mut mods = mods.split(':');
    let Some(value) = mods.next().and_then(|v| v.parse::<u16>().ok()) else {
        return false;
    };
    value.checked_sub(1).is_some_and(|v| v & !192 == 4)
        && matches!(mods.next(), None | Some("1" | "2"))
        && mods.next().is_none()
}
fn file_input(path: &str) -> Vec<u8> {
    use std::fmt::Write;
    let mut out = String::from("\x1b[200~file://");
    for b in path.bytes() {
        if b.is_ascii_alphanumeric() || b"/-._~".contains(&b) {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out.push_str("\x1b[201~");
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paste_key_recognition_ignores_releases_and_preserves_pasted_controls() {
        for key in [
            b"\x16".as_slice(),
            b"\x1b[118;5u",
            b"\x1b[118;5:1u",
            b"\x1b[118::86;5:2u",
            b"\x1b[118;69u",
            b"\x1b[27;5;118~",
        ] {
            assert!(paste_key(key), "{key:?}");
            let mut input = crate::input::Input::default();
            let mut units = Vec::new();
            for b in key {
                units.extend(input.push(&[*b], std::time::Instant::now()).unwrap());
            }
            assert_eq!(
                units.into_iter().map(|u| u.bytes).collect::<Vec<_>>(),
                vec![key]
            );
        }
        for key in [
            b"\x1b[118;5:3u".as_slice(),
            b"\x1b[118;7u",
            b"\x1b[200~\x16\x1b[201~",
            b"\x1b[118;5;118u",
        ] {
            assert!(!paste_key(key));
        }
        let mut input = crate::input::Input::default();
        let pasted = b"\x1b[200~\x16\x1b[201~";
        assert_eq!(
            input
                .push(pasted, std::time::Instant::now())
                .unwrap()
                .into_iter()
                .map(|u| u.bytes)
                .collect::<Vec<_>>(),
            vec![pasted]
        );
        assert_eq!(
            file_input("/tmp/image space #.png"),
            b"\x1b[200~file:///tmp/image%20space%20%23.png\x1b[201~"
        );
    }
}
