use crate::{Result, error, internal, random, unavailable};
use base64::Engine;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot, watch};
use tower_protocol::{
    CLIPBOARD_CHUNK_BYTES, ClipboardAttach, ClipboardChunk, ClipboardClient, ClipboardImage,
    ClipboardLease, ClipboardOffer, ClipboardTicket, ImageFormat, MAX_CLIPBOARD_BYTES, Secret,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Session {
    pub id: String,
    pub generation: String,
}
impl Session {
    fn directory(&self, root: &Path) -> PathBuf {
        root.join(digest(self.id.as_bytes()))
            .join(digest(self.generation.as_bytes()))
    }
}
fn digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub use tower_protocol::ClipboardProviderRequest as ProviderRequest;
struct Provider {
    name: String,
    hash: String,
    formats: Vec<ImageFormat>,
    send: mpsc::Sender<ProviderRequest>,
}
struct Ticket {
    client: String,
    session: Session,
    expires: Instant,
}
#[derive(Clone)]
struct Attachment {
    client: String,
    session: Session,
    order: u64,
}
struct Flight {
    client: String,
    lease: String,
    format: ImageFormat,
    next: u64,
    bytes: Vec<u8>,
    reply: oneshot::Sender<Result<Vec<u8>>>,
}
#[derive(Clone)]
struct Image {
    wire: ClipboardImage,
    lease: String,
    session: Session,
    expires: Instant,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Completion {
    Pending,
    Consumed,
    Cancelled,
}
struct Context {
    image: Image,
    done: watch::Sender<Completion>,
}
#[derive(Default)]
struct State {
    providers: HashMap<String, Provider>,
    tickets: HashMap<String, Ticket>,
    attachments: HashMap<String, Attachment>,
    flights: HashMap<String, Flight>,
    images: HashMap<String, Image>,
    contexts: HashMap<Session, Context>,
    order: u64,
}
pub struct Broker {
    state: Mutex<State>,
    files: Mutex<()>,
    root: PathBuf,
}
pub struct ProviderGuard {
    broker: Arc<Broker>,
    client: String,
}
impl Drop for ProviderGuard {
    fn drop(&mut self) {
        self.broker.revoke(&self.client);
    }
}
pub struct LeaseGuard {
    broker: Arc<Broker>,
    lease: String,
}
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.broker.detach(&self.lease);
    }
}
struct FlightGuard {
    broker: Arc<Broker>,
    id: String,
}
impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.broker.cancel_flight(&self.id);
    }
}

impl Broker {
    pub fn new(root: PathBuf) -> Self {
        Self {
            state: Mutex::new(State::default()),
            files: Mutex::new(()),
            root,
        }
    }
    pub fn offer(
        self: &Arc<Self>,
        offer: ClipboardOffer,
    ) -> Result<(
        ClipboardClient,
        ProviderGuard,
        mpsc::Receiver<ProviderRequest>,
    )> {
        if offer.name.is_empty()
            || offer.name.chars().count() > 80
            || offer.name.chars().any(char::is_control)
            || offer.formats.is_empty()
            || offer.formats.len() > 2
            || (offer.formats.len() == 2 && offer.formats[0] == offer.formats[1])
        {
            return Err(error(
                "invalid_params",
                "A provider needs a name and distinct PNG/BMP formats",
            ));
        }
        let mut s = self.state.lock().map_err(internal)?;
        if s.providers.len() >= 32 {
            return Err(error("clipboard_capacity", "Too many clipboard providers"));
        }
        let client = ClipboardClient {
            client_id: random("client_")?,
            client_token: Secret(random("")?),
        };
        let (send, receive) = mpsc::channel(16);
        s.providers.insert(
            client.client_id.clone(),
            Provider {
                name: offer.name,
                formats: offer.formats,
                hash: digest(client.client_token.0.as_bytes()),
                send,
            },
        );
        Ok((
            client.clone(),
            ProviderGuard {
                broker: self.clone(),
                client: client.client_id,
            },
            receive,
        ))
    }
    pub fn ticket(&self, session: Session, client: &ClipboardClient) -> Result<ClipboardTicket> {
        let mut s = self.state.lock().map_err(internal)?;
        let provider = s.providers.get(&client.client_id).ok_or_else(unavailable)?;
        if !bool::from(
            provider
                .hash
                .as_bytes()
                .ct_eq(digest(client.client_token.0.as_bytes()).as_bytes()),
        ) {
            return Err(unavailable());
        }
        s.tickets.retain(|_, t| t.expires > Instant::now());
        if s.tickets.len() >= 256 {
            return Err(error("clipboard_capacity", "Too many pending attachments"));
        }
        let ticket = Secret(random("")?);
        s.tickets.insert(
            digest(ticket.0.as_bytes()),
            Ticket {
                client: client.client_id.clone(),
                session,
                expires: Instant::now() + Duration::from_secs(60),
            },
        );
        Ok(ClipboardTicket {
            client_id: client.client_id.clone(),
            ticket,
        })
    }
    pub fn attach(
        self: &Arc<Self>,
        p: &ClipboardAttach,
        current: &Session,
    ) -> Result<(ClipboardLease, LeaseGuard)> {
        let mut s = self.state.lock().map_err(internal)?;
        if s.attachments.len() >= 256 {
            return Err(error(
                "clipboard_capacity",
                "Too many clipboard attachments",
            ));
        }
        let t = s
            .tickets
            .remove(&digest(p.ticket.0.as_bytes()))
            .ok_or_else(unavailable)?;
        if t.expires <= Instant::now()
            || t.client != p.client_id
            || t.session != *current
            || p.id != current.id
            || p.generation != current.generation
            || !s.providers.contains_key(&t.client)
        {
            return Err(unavailable());
        }
        s.order = s.order.checked_add(1).ok_or_else(unavailable)?;
        let order = s.order;
        let secret = Secret(random("")?);
        let key = digest(secret.0.as_bytes());
        s.attachments.insert(
            key.clone(),
            Attachment {
                client: t.client.clone(),
                session: t.session,
                order,
            },
        );
        Ok((
            ClipboardLease {
                client_id: t.client,
                lease: secret,
            },
            LeaseGuard {
                broker: self.clone(),
                lease: key,
            },
        ))
    }
    pub fn session_for_lease(&self, lease: &Secret) -> Result<Session> {
        self.state
            .lock()
            .map_err(internal)?
            .attachments
            .get(&digest(lease.0.as_bytes()))
            .map(|a| a.session.clone())
            .ok_or_else(unavailable)
    }
    pub fn status(&self, session: &Session) -> Result<serde_json::Value> {
        let s = self.state.lock().map_err(internal)?;
        let source = s
            .attachments
            .values()
            .filter(|a| a.session == *session)
            .max_by_key(|a| a.order)
            .and_then(|a| {
                s.providers
                    .get(&a.client)
                    .map(|p| serde_json::json!({"client_id":a.client,"name":p.name}))
            });
        Ok(
            serde_json::json!({"id":session.id,"generation":session.generation,"source":source,"attachments":s.attachments.values().filter(|a|a.session==*session).count()}),
        )
    }
    fn cancel_flight(&self, id: &str) {
        if let Ok(mut s) = self.state.lock()
            && let Some(f) = s.flights.remove(id)
        {
            if let Some(p) = s.providers.get(&f.client) {
                let _ = p.send.try_send(ProviderRequest::Cancel {
                    transfer_id: id.into(),
                });
            }
            let _ = f.reply.send(Err(unavailable()));
        }
    }
    fn invalidate(s: &mut State, leases: &[String]) {
        for lease in leases {
            s.attachments.remove(lease);
        }
        let flights: Vec<_> = s
            .flights
            .iter()
            .filter(|(_, f)| leases.contains(&f.lease))
            .map(|(id, _)| id.clone())
            .collect();
        for id in flights {
            if let Some(f) = s.flights.remove(&id) {
                if let Some(p) = s.providers.get(&f.client) {
                    let _ = p.send.try_send(ProviderRequest::Cancel { transfer_id: id });
                }
                let _ = f.reply.send(Err(unavailable()));
            }
        }
        for context in s.contexts.values() {
            if leases.contains(&context.image.lease)
                && *context.done.borrow() == Completion::Pending
            {
                context.done.send_replace(Completion::Cancelled);
            }
        }
        s.images.retain(|_, image| !leases.contains(&image.lease));
    }
    fn detach(&self, lease: &str) {
        if let Ok(mut s) = self.state.lock() {
            Self::invalidate(&mut s, &[lease.into()]);
        }
    }
    fn revoke(&self, client: &str) {
        if let Ok(mut s) = self.state.lock() {
            let leases = s
                .attachments
                .iter()
                .filter(|(_, a)| a.client == client)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            Self::invalidate(&mut s, &leases);
            s.providers.remove(client);
            s.tickets.retain(|_, t| t.client != client);
        }
    }
    pub fn chunk(&self, client: &str, p: ClipboardChunk) -> Result<()> {
        let mut s = self.state.lock().map_err(internal)?;
        let f = s.flights.get_mut(&p.transfer_id).ok_or_else(unavailable)?;
        if f.client != client {
            return Err(unavailable());
        }
        let result = (|| {
            if p.sequence != f.next
                || p.data.len() > CLIPBOARD_CHUNK_BYTES.div_ceil(3) * 4
                || (p.unavailable && (!p.done || !p.data.is_empty()))
            {
                return Err(error("invalid_params", "Invalid clipboard chunk"));
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&p.data)
                .map_err(|_| error("invalid_params", "Invalid clipboard bytes"))?;
            if bytes.len() > CLIPBOARD_CHUNK_BYTES
                || f.bytes.len() + bytes.len() > MAX_CLIPBOARD_BYTES
            {
                return Err(error("too_large", "Clipboard image exceeds its limit"));
            }
            f.next += 1;
            f.bytes.extend(bytes);
            if p.unavailable {
                return Err(unavailable());
            }
            if p.done {
                validate_image(f.format, &f.bytes)?;
            }
            Ok(())
        })();
        if result.is_err() || p.done {
            let f = s.flights.remove(&p.transfer_id).expect("present");
            let _ = f.reply.send(result.map(|()| f.bytes));
        }
        Ok(())
    }
    pub async fn fetch(
        self: &Arc<Self>,
        session: Session,
        lease: Option<&Secret>,
        format: Option<ImageFormat>,
    ) -> Result<ClipboardImage> {
        let (id, key, format, receive) = {
            let mut s = self.state.lock().map_err(internal)?;
            let (key, a) = if let Some(lease) = lease {
                let key = digest(lease.0.as_bytes());
                let a = s
                    .attachments
                    .get(&key)
                    .filter(|a| a.session == session)
                    .ok_or_else(unavailable)?
                    .clone();
                (key, a)
            } else {
                s.attachments
                    .iter()
                    .filter(|(_, a)| a.session == session)
                    .max_by_key(|(_, a)| a.order)
                    .map(|(k, a)| (k.clone(), a.clone()))
                    .ok_or_else(unavailable)?
            };
            let provider = s.providers.get(&a.client).ok_or_else(unavailable)?;
            let format = format.unwrap_or_else(|| {
                if provider.formats.contains(&ImageFormat::Png) {
                    ImageFormat::Png
                } else {
                    ImageFormat::Bmp
                }
            });
            if !provider.formats.contains(&format) {
                return Err(unavailable());
            }
            if s.flights.len() >= 8
                || s.flights.values().filter(|f| f.client == a.client).count() >= 2
            {
                return Err(error(
                    "clipboard_busy",
                    "Clipboard transfer capacity is full",
                ));
            }
            let id = random("paste_")?;
            let (send, receive) = oneshot::channel();
            provider
                .send
                .try_send(ProviderRequest::Read {
                    transfer_id: id.clone(),
                    format,
                    max_bytes: MAX_CLIPBOARD_BYTES,
                })
                .map_err(|_| unavailable())?;
            s.flights.insert(
                id.clone(),
                Flight {
                    client: a.client,
                    lease: key.clone(),
                    format,
                    next: 0,
                    bytes: Vec::new(),
                    reply: send,
                },
            );
            (id, key, format, receive)
        };
        let _guard = FlightGuard {
            broker: self.clone(),
            id: id.clone(),
        };
        let bytes = tokio::time::timeout(Duration::from_secs(30), receive)
            .await
            .map_err(|_| error("clipboard_timeout", "Clipboard provider timed out"))?
            .map_err(|_| unavailable())??;
        self.valid_lease(&key, &session)?;
        let broker = self.clone();
        let copy = session.clone();
        let file_id = id.clone();
        let image =
            tokio::task::spawn_blocking(move || broker.stage(&copy, &file_id, format, &bytes))
                .await
                .map_err(internal)??;
        let mut s = self.state.lock().map_err(internal)?;
        if !s
            .attachments
            .get(&key)
            .is_some_and(|a| a.session == session)
        {
            let _ = tower_pal::remove_private(Path::new(&image.path));
            return Err(unavailable());
        }
        s.images.retain(|_, i| i.expires > Instant::now());
        if s.images.len() >= 256 {
            let _ = tower_pal::remove_private(Path::new(&image.path));
            return Err(error(
                "clipboard_capacity",
                "Too many retained transfer handles",
            ));
        }
        s.images.insert(
            id,
            Image {
                wire: image.clone(),
                lease: key,
                session,
                expires: Instant::now() + Duration::from_secs(60),
            },
        );
        Ok(image)
    }
    fn valid_lease(&self, key: &str, session: &Session) -> Result<()> {
        if self
            .state
            .lock()
            .map_err(internal)?
            .attachments
            .get(key)
            .is_some_and(|a| a.session == *session)
        {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    pub fn claim(&self, session: &Session, paste: &str) -> Result<ClipboardImage> {
        let s = self.state.lock().map_err(internal)?;
        let image = s
            .images
            .get(paste)
            .filter(|i| i.session == *session && i.expires > Instant::now())
            .ok_or_else(unavailable)?;
        if !s.attachments.contains_key(&image.lease) {
            return Err(unavailable());
        }
        Ok(image.wire.clone())
    }
    pub fn activate(&self, session: &Session, paste: &str) -> Result<()> {
        let mut s = self.state.lock().map_err(internal)?;
        let image = s
            .images
            .get(paste)
            .filter(|i| i.session == *session && i.expires > Instant::now())
            .ok_or_else(unavailable)?
            .clone();
        if !s.attachments.contains_key(&image.lease) {
            return Err(unavailable());
        }
        if let Some(c) = s.contexts.get(session) {
            match *c.done.borrow() {
                Completion::Pending => {
                    return Err(error(
                        "clipboard_busy",
                        "A native clipboard paste is already active",
                    ));
                }
                Completion::Cancelled => {
                    return Err(error(
                        "clipboard_input_uncertain",
                        "Native clipboard input is uncertain; restart the session before another image paste",
                    ));
                }
                Completion::Consumed => {}
            }
        }
        let marker = session.directory(&self.root).join("native-paste.pending");
        if marker.exists() {
            return Err(error(
                "clipboard_input_uncertain",
                "Native clipboard input is uncertain; restart the session before another image paste",
            ));
        }
        tower_pal::write_private(&marker, paste.as_bytes()).map_err(internal)?;
        let (done, _) = watch::channel(Completion::Pending);
        s.contexts.insert(session.clone(), Context { image, done });
        Ok(())
    }
    pub async fn helper(
        self: &Arc<Self>,
        session: Session,
        format: ImageFormat,
    ) -> Result<ClipboardImage> {
        {
            let s = self.state.lock().map_err(internal)?;
            if let Some(c) = s.contexts.get(&session) {
                if *c.done.borrow() != Completion::Pending
                    || c.image.wire.format != format
                    || !s.attachments.contains_key(&c.image.lease)
                {
                    return Err(unavailable());
                }
                return Ok(c.image.wire.clone());
            }
        }
        if session
            .directory(&self.root)
            .join("native-paste.pending")
            .exists()
        {
            return Err(error(
                "clipboard_input_uncertain",
                "Native clipboard input is uncertain; restart the session before reading another image",
            ));
        }
        self.fetch(session, None, Some(format)).await
    }
    pub fn consumed(&self, session: &Session, paste: &str) -> Result<()> {
        let s = self.state.lock().map_err(internal)?;
        let image = s
            .images
            .get(paste)
            .filter(|i| i.session == *session)
            .ok_or_else(unavailable)?;
        if !s.attachments.contains_key(&image.lease) {
            return Err(unavailable());
        }
        if let Some(c) = s.contexts.get(session)
            && c.image.wire.paste_id == paste
        {
            if *c.done.borrow() != Completion::Pending {
                return Err(unavailable());
            }
            tower_pal::remove_private(&session.directory(&self.root).join("native-paste.pending"))
                .map_err(internal)?;
            c.done.send_replace(Completion::Consumed);
        }
        Ok(())
    }
    pub async fn wait(&self, session: &Session, paste: &str) -> Result<()> {
        let mut receiver = {
            let s = self.state.lock().map_err(internal)?;
            let c = s
                .contexts
                .get(session)
                .filter(|c| c.image.wire.paste_id == paste)
                .ok_or_else(unavailable)?;
            c.done.subscribe()
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let state = *receiver.borrow_and_update();
                match state {
                    Completion::Consumed => return Ok(()),
                    Completion::Cancelled => return Err(unavailable()),
                    Completion::Pending => {
                        receiver.changed().await.map_err(|_| unavailable())?;
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            error(
                "clipboard_timeout",
                "Native clipboard helper did not finish",
            )
        })?
    }
    pub fn finish(&self, session: &Session, paste: &str, success: bool) -> Result<()> {
        let mut s = self.state.lock().map_err(internal)?;
        if s.contexts
            .get(session)
            .is_some_and(|c| c.image.wire.paste_id == paste)
        {
            if success
                && s.contexts
                    .get(session)
                    .is_some_and(|c| *c.done.borrow() == Completion::Consumed)
            {
                s.contexts.remove(session);
            } else if let Some(c) = s.contexts.get(session) {
                c.done.send_replace(Completion::Cancelled);
            }
        }
        if s.images
            .get(paste)
            .is_some_and(|image| image.session == *session)
        {
            s.images.remove(paste);
        }
        Ok(())
    }
    fn stage(
        &self,
        session: &Session,
        id: &str,
        format: ImageFormat,
        bytes: &[u8],
    ) -> Result<ClipboardImage> {
        let _guard = self.files.lock().map_err(internal)?;
        tower_pal::private_dir(&self.root).map_err(internal)?;
        let (count, size) = usage(&self.root, 0)?;
        if count >= 256 || size + bytes.len() as u64 > 256 * 1024 * 1024 {
            return Err(error(
                "clipboard_capacity",
                "Clipboard staging is full; end unused sessions",
            ));
        }
        let directory = session.directory(&self.root);
        tower_pal::private_dir(directory.parent().expect("session directory"))
            .and_then(|_| tower_pal::private_dir(&directory))
            .map_err(internal)?;
        let path = directory.join(format!("{id}.{}", format.extension()));
        if path.to_string_lossy().chars().any(char::is_control) {
            return Err(error(
                "invalid_params",
                "Clipboard path contains control characters",
            ));
        }
        tower_pal::write_private(&path, bytes).map_err(internal)?;
        Ok(ClipboardImage {
            paste_id: id.into(),
            path: path.to_string_lossy().into_owned(),
            format,
            bytes: bytes.len() as u64,
        })
    }
    pub fn retain(&self, sessions: &[Session]) -> Result<()> {
        {
            let mut s = self.state.lock().map_err(internal)?;
            let leases = s
                .attachments
                .iter()
                .filter(|(_, a)| !sessions.contains(&a.session))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            Self::invalidate(&mut s, &leases);
            s.contexts.retain(|key, _| sessions.contains(key));
            s.tickets
                .retain(|_, t| sessions.contains(&t.session) && t.expires > Instant::now());
            s.images
                .retain(|_, i| sessions.contains(&i.session) && i.expires > Instant::now());
        }
        let _guard = self.files.lock().map_err(internal)?;
        if !self.root.exists() {
            return Ok(());
        }
        tower_pal::require_private_dir(&self.root).map_err(internal)?;
        let keep: BTreeSet<_> = sessions.iter().map(|s| s.directory(&self.root)).collect();
        for parent in std::fs::read_dir(&self.root).map_err(internal)? {
            let parent = parent.map_err(internal)?.path();
            tower_pal::require_private_dir(&parent).map_err(internal)?;
            for dir in std::fs::read_dir(&parent).map_err(internal)? {
                let dir = dir.map_err(internal)?.path();
                if !keep.contains(&dir) {
                    tower_pal::remove_private_tree(&dir).map_err(internal)?;
                }
            }
            if std::fs::read_dir(&parent)
                .map_err(internal)?
                .next()
                .is_none()
            {
                std::fs::remove_dir(&parent).map_err(internal)?;
            }
        }
        Ok(())
    }
}
fn usage(path: &Path, depth: u8) -> Result<(usize, u64)> {
    if depth > 3 {
        return Err(internal("staging depth"));
    }
    tower_pal::require_private_dir(path).map_err(internal)?;
    let mut count = 0;
    let mut size = 0;
    for entry in std::fs::read_dir(path).map_err(internal)? {
        let entry = entry.map_err(internal)?;
        let meta = std::fs::symlink_metadata(entry.path()).map_err(internal)?;
        if meta.is_dir() {
            let (c, n) = usage(&entry.path(), depth + 1)?;
            count += c;
            size += n;
        } else if meta.is_file() {
            count += 1;
            size += meta.len();
        } else {
            return Err(internal("invalid staging entry"));
        }
        if count > 256 {
            return Err(error("clipboard_capacity", "Too many staged images"));
        }
    }
    Ok((count, size))
}
fn validate_image(format: ImageFormat, bytes: &[u8]) -> Result<()> {
    let be = |s: &[u8]| u32::from_be_bytes(s.try_into().unwrap()) as usize;
    let le = |s: &[u8]| u32::from_le_bytes(s.try_into().unwrap()) as usize;
    let dimensions = |w: usize, h: usize| {
        w > 0 && h > 0 && w <= 16384 && h <= 16384 && w * h <= 64 * 1024 * 1024
    };
    let valid = match format {
        ImageFormat::Png if bytes.len() >= 45 && bytes.starts_with(b"\x89PNG\r\n\x1a\n") => {
            if be(&bytes[8..12]) != 13
                || &bytes[12..16] != b"IHDR"
                || !dimensions(be(&bytes[16..20]), be(&bytes[20..24]))
            {
                false
            } else {
                let mut at = 33;
                let mut data = false;
                let mut end = false;
                while at + 12 <= bytes.len() {
                    let size = be(&bytes[at..at + 4]);
                    if size > bytes.len() - at - 12 {
                        break;
                    }
                    let kind = &bytes[at + 4..at + 8];
                    at += size + 12;
                    if kind == b"IDAT" && size > 0 {
                        data = true;
                    }
                    if kind == b"IEND" {
                        end = size == 0 && at == bytes.len();
                        break;
                    }
                    if kind == b"IHDR" {
                        break;
                    }
                }
                data && end
            }
        }
        ImageFormat::Bmp if bytes.len() >= 54 && bytes.starts_with(b"BM") => {
            let header = le(&bytes[14..18]);
            let offset = le(&bytes[10..14]);
            let height =
                i32::from_le_bytes(bytes[22..26].try_into().unwrap()).unsigned_abs() as usize;
            let width = le(&bytes[18..22]);
            le(&bytes[2..6]) == bytes.len()
                && header >= 40
                && header <= bytes.len() - 14
                && offset >= header + 14
                && offset < bytes.len()
                && dimensions(width, height)
                && bytes[26..28] == [1, 0]
                && matches!(
                    u16::from_le_bytes(bytes[28..30].try_into().unwrap()),
                    1 | 4 | 8 | 16 | 24 | 32
                )
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(error(
            "clipboard_invalid_image",
            "Provider returned an invalid image",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn png() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aHioAAAAASUVORK5CYII=").unwrap()
    }
    fn session() -> Session {
        Session {
            id: "session".into(),
            generation: "generation-1".into(),
        }
    }
    fn offer(
        b: &Arc<Broker>,
        name: &str,
    ) -> (
        ClipboardClient,
        ProviderGuard,
        mpsc::Receiver<ProviderRequest>,
    ) {
        b.offer(ClipboardOffer {
            name: name.into(),
            formats: vec![ImageFormat::Png],
        })
        .unwrap()
    }
    fn attach(b: &Arc<Broker>, c: &ClipboardClient) -> (ClipboardLease, LeaseGuard) {
        let t = b.ticket(session(), c).unwrap();
        b.attach(
            &ClipboardAttach {
                id: session().id,
                generation: session().generation,
                client_id: t.client_id,
                ticket: t.ticket,
            },
            &session(),
        )
        .unwrap()
    }
    async fn read_request(rx: &mut mpsc::Receiver<ProviderRequest>) -> String {
        match tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            ProviderRequest::Read { transfer_id, .. } => transfer_id,
            _ => panic!("expected read"),
        }
    }
    fn send(b: &Broker, c: &ClipboardClient, id: &str, bytes: &[u8]) {
        b.chunk(
            &c.client_id,
            ClipboardChunk {
                transfer_id: id.into(),
                sequence: 0,
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
                done: true,
                unavailable: false,
            },
        )
        .unwrap();
    }
    async fn fetch(
        b: &Arc<Broker>,
        c: &ClipboardClient,
        lease: Option<&Secret>,
        rx: &mut mpsc::Receiver<ProviderRequest>,
    ) -> ClipboardImage {
        let broker = b.clone();
        let lease = lease.cloned();
        let task = tokio::spawn(async move { broker.fetch(session(), lease.as_ref(), None).await });
        let id = read_request(rx).await;
        send(b, c, &id, &png());
        task.await.unwrap().unwrap()
    }
    #[tokio::test]
    async fn last_attach_fallback_and_explicit_paste_source_are_independent() {
        let dir = tempfile::tempdir().unwrap();
        let b = Arc::new(Broker::new(dir.path().join("images")));
        let (a, _pa, mut ra) = offer(&b, "A");
        let (a_lease, _la) = attach(&b, &a);
        let (c, _pc, mut rc) = offer(&b, "B");
        let (_, lc) = attach(&b, &c);
        assert_eq!(
            b.status(&session()).unwrap()["source"]["client_id"],
            c.client_id
        );
        let explicit = fetch(&b, &a, Some(&a_lease.lease), &mut ra).await;
        assert_eq!(
            tower_pal::read_private(Path::new(&explicit.path)).unwrap(),
            png()
        );
        assert_eq!(
            b.status(&session()).unwrap()["source"]["client_id"],
            c.client_id
        );
        fetch(&b, &c, None, &mut rc).await;
        drop(lc);
        assert_eq!(
            b.status(&session()).unwrap()["source"]["client_id"],
            a.client_id
        );
        fetch(&b, &a, None, &mut ra).await;
    }
    #[tokio::test]
    async fn disconnect_cancels_pinned_transfer_without_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let b = Arc::new(Broker::new(dir.path().join("images")));
        let (a, _pa, mut ra) = offer(&b, "A");
        let (a_lease, la) = attach(&b, &a);
        let (c, _pc, mut rc) = offer(&b, "B");
        let (_, _lc) = attach(&b, &c);
        let copy = b.clone();
        let task =
            tokio::spawn(async move { copy.fetch(session(), Some(&a_lease.lease), None).await });
        let id = read_request(&mut ra).await;
        drop(la);
        assert_eq!(
            task.await.unwrap().unwrap_err().data.code,
            "clipboard_unavailable"
        );
        assert!(rc.try_recv().is_err());
        assert!(
            b.chunk(
                &a.client_id,
                ClipboardChunk {
                    transfer_id: id,
                    sequence: 0,
                    data: String::new(),
                    done: true,
                    unavailable: true
                }
            )
            .is_err()
        );
    }
    #[test]
    fn tickets_are_secret_single_use_and_generation_bound() {
        let dir = tempfile::tempdir().unwrap();
        let b = Arc::new(Broker::new(dir.path().join("images")));
        let (a, _pa, _ra) = offer(&b, "A");
        let mut bad = a.clone();
        bad.client_token = Secret("wrong".into());
        assert!(b.ticket(session(), &bad).is_err());
        let t = b.ticket(session(), &a).unwrap();
        let p = ClipboardAttach {
            id: session().id,
            generation: session().generation,
            client_id: t.client_id,
            ticket: t.ticket,
        };
        let (_, guard) = b.attach(&p, &session()).unwrap();
        assert!(b.attach(&p, &session()).is_err());
        drop(guard);
        let t = b.ticket(session(), &a).unwrap();
        let mut p = ClipboardAttach {
            id: session().id,
            generation: session().generation,
            client_id: t.client_id,
            ticket: t.ticket,
        };
        p.generation = "new".into();
        assert!(b.attach(&p, &session()).is_err());
    }
    #[tokio::test]
    async fn native_helper_uses_frozen_image_and_uncertainty_survives_daemon_restart() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("images");
        let b = Arc::new(Broker::new(root.clone()));
        let (a, _pa, mut ra) = offer(&b, "A");
        let (lease, la) = attach(&b, &a);
        let image = fetch(&b, &a, Some(&lease.lease), &mut ra).await;
        b.activate(&session(), &image.paste_id).unwrap();
        let (c, _pc, _rc) = offer(&b, "B");
        let (_, _lc) = attach(&b, &c);
        assert_eq!(
            b.helper(session(), ImageFormat::Png)
                .await
                .unwrap()
                .paste_id,
            image.paste_id
        );
        drop(la);
        assert!(b.wait(&session(), &image.paste_id).await.is_err());
        assert!(b.helper(session(), ImageFormat::Png).await.is_err());
        b.finish(&session(), &image.paste_id, true).unwrap();
        assert!(b.helper(session(), ImageFormat::Png).await.is_err());
        let restarted = Arc::new(Broker::new(root));
        assert_eq!(
            restarted
                .helper(session(), ImageFormat::Png)
                .await
                .unwrap_err()
                .data
                .code,
            "clipboard_input_uncertain"
        );
        restarted.retain(&[]).unwrap();
        assert!(!Path::new(&image.path).exists());
    }
    #[tokio::test]
    async fn confirmed_consumption_releases_context_and_retains_file_until_session_end() {
        let dir = tempfile::tempdir().unwrap();
        let b = Arc::new(Broker::new(dir.path().join("images")));
        let (a, _pa, mut ra) = offer(&b, "A");
        let (lease, _la) = attach(&b, &a);
        let image = fetch(&b, &a, Some(&lease.lease), &mut ra).await;
        b.activate(&session(), &image.paste_id).unwrap();
        b.consumed(&session(), &image.paste_id).unwrap();
        b.wait(&session(), &image.paste_id).await.unwrap();
        b.finish(&session(), &image.paste_id, true).unwrap();
        assert!(Path::new(&image.path).exists());
        b.retain(&[session()]).unwrap();
        assert!(Path::new(&image.path).exists());
        let image = fetch(&b, &a, Some(&lease.lease), &mut ra).await;
        b.activate(&session(), &image.paste_id).unwrap();
        b.retain(&[]).unwrap();
        assert!(b.session_for_lease(&lease.lease).is_err());
        assert!(!Path::new(&image.path).exists());
    }
    #[tokio::test]
    async fn malformed_chunk_fails_transfer_without_exposing_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("images");
        let b = Arc::new(Broker::new(root.clone()));
        let (a, _pa, mut ra) = offer(&b, "A");
        let (lease, _la) = attach(&b, &a);
        let copy = b.clone();
        let task =
            tokio::spawn(async move { copy.fetch(session(), Some(&lease.lease), None).await });
        let id = read_request(&mut ra).await;
        b.chunk(
            &a.client_id,
            ClipboardChunk {
                transfer_id: id,
                sequence: 1,
                data: String::new(),
                done: true,
                unavailable: false,
            },
        )
        .unwrap();
        assert_eq!(task.await.unwrap().unwrap_err().data.code, "invalid_params");
        assert!(!root.exists());
        let bytes = png();
        for n in 0..bytes.len() {
            assert!(validate_image(ImageFormat::Png, &bytes[..n]).is_err());
        }
        validate_image(ImageFormat::Png, &bytes).unwrap();
        let mut oversized = bytes.clone();
        oversized[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(validate_image(ImageFormat::Png, &oversized).is_err());
    }
}
