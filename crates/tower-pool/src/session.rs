use crate::{input::Input, terminal::Terminal};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, watch};
use tower_pal::PtyProcess;
use tower_pool_proto::{Launch, Reply, Session, Size};
const MAX_INPUT: usize = 1024 * 1024;
pub enum Out {
    Data(Arc<[u8]>),
    Control(Reply),
}
pub struct Snapshot {
    pub size: Size,
    pub truncated: bool,
    pub limited: bool,
    pub buffer: Vec<u8>,
}
type ExitWatch = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
pub enum Command {
    Clipboard {
        id: u64,
        lease: tower_protocol::ClipboardLease,
        connection: tower_pal::IpcStream,
    },
    Inject {
        proof: tower_pool_proto::InputProof,
        text: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Start {
        size: Size,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Attach {
        size: Size,
        term: String,
        out: mpsc::Sender<Out>,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Input {
        id: u64,
        bytes: Vec<u8>,
    },
    Resize {
        id: u64,
        size: Size,
    },
    Detach {
        id: u64,
    },
    Snapshot(oneshot::Sender<Snapshot>),
    Kill(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}
#[derive(Clone)]
pub struct Handle {
    pub tx: mpsc::Sender<Command>,
    pub info: watch::Receiver<Session>,
    pub launch: Launch,
}
struct Client {
    size: Size,
    input: Input,
    out: mpsc::Sender<Out>,
}
struct Submission {
    reply: oneshot::Sender<Result<(), String>>,
    proof: tower_pool_proto::InputProof,
}
enum InputOrigin {
    Client,
    Reply,
    Message(Box<Submission>),
    Clipboard(u64),
}
struct InputUnit {
    bytes: Vec<u8>,
    origin: InputOrigin,
}
impl InputUnit {
    fn is_reply(&self) -> bool {
        matches!(self.origin, InputOrigin::Reply)
    }
}
struct Actor {
    defaults: crate::preferences::Profile,
    clipboard: crate::clipboard::Clipboard,
    launch: Launch,
    pty: Option<PtyProcess>,
    terminal: Terminal,
    clients: BTreeMap<u64, Client>,
    next: u64,
    info: Session,
    state: watch::Sender<Session>,
    changes: Changes,
    rx: mpsc::Receiver<Command>,
    input: VecDeque<InputUnit>,
    offset: usize,
    input_bytes: usize,
    reply_bytes: usize,
    read_closed: bool,
    pending_exit: Option<i32>,
    killing: Option<Instant>,
    kill_replies: Vec<oneshot::Sender<()>>,
    last_input: Option<u64>,
    consumed_idle: Option<u64>,
}
/// Pool-wide revision, bumped whenever any session's state changes.
pub type Changes = std::sync::Arc<watch::Sender<u64>>;
pub fn reserve(launch: Launch, defaults: crate::preferences::Profile, changes: Changes) -> Handle {
    let size = launch.size.unwrap_or(defaults.size);
    let info = Session {
        name: launch.name.clone(),
        state: "reserved".into(),
        pid: None,
        pid_start_time: None,
        attach_count: 0,
        size,
        exit_code: None,
    };
    let (state, receiver) = watch::channel(info.clone());
    let (tx, rx) = mpsc::channel(64);
    let actor = Actor {
        defaults,
        clipboard: crate::clipboard::Clipboard::new(launch.name.clone(), launch.clipboard.clone()),
        launch: launch.clone(),
        pty: None,
        terminal: Terminal::new(size),
        clients: BTreeMap::new(),
        next: 0,
        info,
        state,
        changes,
        rx,
        input: VecDeque::new(),
        offset: 0,
        input_bytes: 0,
        reply_bytes: 0,
        read_closed: false,
        pending_exit: None,
        killing: None,
        kill_replies: Vec::new(),
        last_input: None,
        consumed_idle: None,
    };
    tokio::spawn(actor.run());
    Handle {
        tx,
        info: receiver,
        launch,
    }
}
impl Actor {
    fn remove_client(&mut self, id: u64) {
        self.clients.remove(&id);
        self.clipboard.detached(id);
    }
    fn clipboard_action(&mut self, action: crate::clipboard::Action) {
        match action {
            crate::clipboard::Action::Input { id, bytes } => {
                if self.clients.contains_key(&id)
                    && self.killing.is_none()
                    && self.pending_exit.is_none()
                    && bytes.len() <= MAX_INPUT - self.input_bytes
                {
                    self.input_bytes += bytes.len();
                    self.input.push_back(InputUnit {
                        bytes,
                        origin: InputOrigin::Clipboard(id),
                    });
                } else {
                    self.remove_client(id);
                    self.resize();
                }
            }
            crate::clipboard::Action::Finished { id, code, input } => {
                if let Some(c) = self.clients.get(&id) {
                    let _ = c.out.try_send(Out::Control(Reply::Clipboard {
                        state: if code.is_some() {
                            "failed"
                        } else {
                            "submitted"
                        }
                        .into(),
                        code,
                    }));
                }
                if !self.enqueue(input) {
                    self.remove_client(id);
                    self.resize();
                }
            }
            crate::clipboard::Action::Ignore => {}
        }
    }
    async fn start(&mut self, size: Size, attached_term: Option<&str>) -> io::Result<()> {
        if self.pty.is_some() {
            return Ok(());
        }
        if self.info.state == "exited" {
            return Err(io::Error::other("session has exited"));
        }
        let mut launch = self.launch.clone();
        let term = launch
            .term
            .as_deref()
            .or(attached_term)
            .unwrap_or(&self.defaults.term);
        launch.env.insert("TERM".into(), term.into());
        launch.env.insert("COLORTERM".into(), "truecolor".into());
        let pty = tokio::task::spawn_blocking(move || {
            PtyProcess::spawn(
                &launch.argv,
                &launch.env,
                Path::new(&launch.cwd),
                size.rows,
                size.cols,
            )
        })
        .await
        .map_err(io::Error::other)??;
        self.info.state = "running".into();
        self.info.pid = Some(pty.pid());
        self.info.pid_start_time = Some(pty.start_time);
        self.info.size = size;
        self.terminal.resize(size);
        self.pty = Some(pty);
        self.publish();
        Ok(())
    }
    fn publish(&self) {
        self.state.send_replace(self.info.clone());
        self.changes
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
    fn resize(&mut self) {
        let size = if self.clients.is_empty() {
            self.info.size
        } else {
            Size {
                rows: self
                    .clients
                    .values()
                    .map(|c| c.size.rows)
                    .min()
                    .unwrap_or(1),
                cols: self
                    .clients
                    .values()
                    .map(|c| c.size.cols)
                    .min()
                    .unwrap_or(1),
            }
        };
        if size != self.info.size {
            if let Some(pty) = &self.pty
                && pty.resize(size.rows, size.cols).is_err()
            {
                return;
            }
            self.info.size = size;
            self.terminal.resize(size);
        }
        self.info.attach_count = self.clients.len() as u32;
        self.publish();
    }
    fn broadcast(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let bytes: Arc<[u8]> = bytes.into();
        let before = self.clients.len();
        let gone = self
            .clients
            .iter()
            .filter(|(_, c)| c.out.try_send(Out::Data(Arc::clone(&bytes))).is_err())
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in gone {
            self.remove_client(id);
        }
        if self.clients.len() != before {
            self.resize();
        }
    }
    fn enqueue(&mut self, units: Vec<Vec<u8>>) -> bool {
        let total = units.iter().map(Vec::len).sum::<usize>();
        if total > MAX_INPUT - self.input_bytes || self.input.len() + units.len() > 4096 {
            return false;
        }
        self.input_bytes += total;
        self.input.extend(
            units
                .into_iter()
                .filter(|v| !v.is_empty())
                .map(|bytes| InputUnit {
                    bytes,
                    origin: InputOrigin::Client,
                }),
        );
        true
    }
    fn enqueue_reply(&mut self, bytes: Vec<u8>) {
        self.reply_bytes += bytes.len();
        let unit = InputUnit {
            bytes,
            origin: InputOrigin::Reply,
        };
        // Replies can precede untouched user input, but cannot split a control
        // sequence or paste that has already started writing to the child.
        let mut at = usize::from(self.offset > 0);
        while self.input.get(at).is_some_and(|unit| unit.is_reply()) {
            at += 1;
        }
        self.input.insert(at, unit);
    }
    fn exited(&mut self, code: i32) {
        self.info.state = "exited".into();
        self.info.exit_code = Some(code);
        self.info.attach_count = 0;
        self.publish();
        for c in self.clients.values() {
            let _ = c.out.try_send(Out::Control(Reply::Exited {
                name: self.info.name.clone(),
                code,
            }));
        }
        self.clients.clear();
        self.clipboard.clear();
        self.clear_input();
        self.pty.take();
        for reply in self.kill_replies.drain(..) {
            let _ = reply.send(());
        }
        self.killing = None;
    }
    fn clear_input(&mut self) {
        for (index, unit) in self.input.drain(..).enumerate() {
            if let InputOrigin::Message(message) = unit.origin {
                let _ = message.reply.send(Err(if index == 0 && self.offset > 0 {
                    "delivery_unknown"
                } else {
                    "delivery_failed"
                }
                .into()));
            }
        }
        self.clipboard.clear();
        self.input_bytes = 0;
        self.reply_bytes = 0;
        self.offset = 0;
    }
    fn reap(&mut self) {
        if let Some(p) = &mut self.pty {
            match p.poll_exit() {
                Ok(Some(code)) => {
                    self.clear_input();
                    if self.read_closed {
                        self.exited(code);
                    } else {
                        self.pending_exit = Some(code);
                    }
                }
                Err(_) => {
                    let _ = p.signal(true);
                }
                _ => {}
            }
        }
    }
    async fn run(mut self) {
        let mut bytes = vec![0; 32 * 1024];
        let mut exit: Option<(u32, ExitWatch)> = None;
        loop {
            match self.pty.as_ref().map(|p| (p.pid(), p.start_time)) {
                Some((pid, start)) if exit.as_ref().is_none_or(|(watched, _)| *watched != pid) => {
                    exit = Some((
                        pid,
                        match tower_pal::ProcessExit::watch(pid, start) {
                            Ok(watch) => Box::pin(watch.exited()) as ExitWatch,
                            Err(_) => Box::pin(std::future::pending()),
                        },
                    ));
                }
                None => exit = None,
                _ => {}
            }
            if self.offset == 0
                && let Some(proof) = self.input.front().and_then(|u| match &u.origin {
                    InputOrigin::Message(m) => Some(&m.proof),
                    _ => None,
                })
                && let Err(error) = self.input_gate(proof)
            {
                if let Some(unit) = self.input.pop_front() {
                    self.input_bytes -= unit.bytes.len();
                    if let InputOrigin::Message(message) = unit.origin {
                        let _ = message.reply.send(Err(error.into()));
                    }
                }
                continue;
            }
            if self.offset==0 && self.input.front().is_some_and(|u|matches!(u.origin,InputOrigin::Clipboard(id) if !self.clients.contains_key(&id))) {
                if let Some(unit)=self.input.pop_front(){self.input_bytes-=unit.bytes.len();}continue;
            }
            let pty = self.pty.as_ref();
            let pending = self.input.front().map(|v| &v.bytes[self.offset..]);
            let wake = self
                .clients
                .values()
                .filter_map(|c| c.input.deadline())
                .chain(self.killing)
                .min();
            tokio::select! {
                read = async { pty.unwrap().read(&mut bytes).await },
                    if pty.is_some() && !self.read_closed && self.reply_bytes < 128 * 1024 => {
                    match read {
                        Ok(0) | Err(_) => {
                            self.read_closed = true;
                            if let Some(code) = self.pending_exit.take() { self.exited(code); }
                            else { self.reap(); }
                        }
                        Ok(n) => {
                            let out = self.terminal.process(&bytes[..n]);
                            if !out.replies.is_empty() && self.killing.is_none() && self.pending_exit.is_none() {
                                self.enqueue_reply(out.replies);
                            }
                            self.broadcast(out.bytes);
                            // A continuously readable PTY can otherwise keep
                            // its writers from draining their bounded queues.
                            tokio::task::yield_now().await;
                        }
                    }
                }
                written = async { pty.unwrap().write(pending.unwrap()).await },
                    if pty.is_some() && pending.is_some() => {
                    match written {
                        Ok(n) if n > 0 => {
                            self.offset += n;
                            if self.input.front().unwrap().is_reply() { self.reply_bytes -= n; }
                            else { self.input_bytes -= n; }
                            if self.offset == self.input.front().unwrap().bytes.len() {
                                if let Some(unit)=self.input.pop_front() {match unit.origin {InputOrigin::Message(message)=>{let _=message.reply.send(Ok(()));},InputOrigin::Clipboard(id)=>self.clipboard.submitted(id),_=>{}}}
                                self.offset = 0;
                            }
                        }
                        _ => self.clear_input(),
                    }
                }
                () = async { exit.as_mut().unwrap().1.as_mut().await }, if pty.is_some() && exit.is_some() && self.pending_exit.is_none() => {
                    // A finished future must not be polled again; keep the PID so it is not re-armed.
                    if let Some((_, fired)) = &mut exit { *fired = Box::pin(std::future::pending()); }
                    self.reap();
                }
                _ = tokio::time::sleep_until(wake.unwrap_or_else(Instant::now).into()), if pty.is_some() && wake.is_some() => {
                    let now = Instant::now();
                    let closed=self.clients.iter().filter(|(_,c)|c.out.is_closed()).map(|(id,_)|*id).collect::<Vec<_>>();
                    if !closed.is_empty(){for id in closed{self.remove_client(id);}self.resize();}
                    let mut expired = Vec::new();
                    let mut overflow = Vec::new();
                    for (&id, client) in &mut self.clients {
                        if let Some(unit) = client.input.expire(now)
                            && self.killing.is_none() && self.pending_exit.is_none() { expired.push((id, unit)); }
                    }
                    for (id, unit) in expired {
                        match self.clipboard.push(id, vec![unit], MAX_INPUT - self.input_bytes) {
                            Ok(units) => { if !self.enqueue(units) { overflow.push(id); } },
                            _ => overflow.push(id),
                        }
                    }
                    if !overflow.is_empty() {
                        for id in overflow { self.remove_client(id); }
                        self.resize();
                    }
                    if self.killing.is_some_and(|deadline| now >= deadline)
                        && let Some(p) = &self.pty {
                        let _ = p.signal(true);
                        self.killing = Some(now + Duration::from_secs(1));
                    }
                }
                event = self.clipboard.receive.recv() => {
                    if let Some(event)=event {let action=self.clipboard.event(event,MAX_INPUT-self.input_bytes);self.clipboard_action(action);}
                }
                command = self.rx.recv() => {
                    let Some(command) = command else { break };
                    if !self.command(command).await { break; }
                }
            }
        }
    }
    fn input_gate(&self, proof: &tower_pool_proto::InputProof) -> Result<(), &'static str> {
        if self.info.pid != Some(proof.pid)
            || self.info.pid_start_time != Some(proof.pid_start_time)
        {
            return Err("stale_session");
        }
        let now = tower_pal::monotonic_ms();
        if proof.checked_at > now
            || now.saturating_sub(proof.checked_at) > 500
            || proof.idle_since > proof.checked_at
        {
            return Err("stale_input_evidence");
        }
        if self.info.state != "running"
            || self.killing.is_some()
            || self.pending_exit.is_some()
            || self.read_closed
            || !self.terminal.mode(2004)
        {
            return Err("target_busy_pty");
        }
        if self
            .last_input
            .is_some_and(|at| at >= proof.idle_since || now.saturating_sub(at) < 2000)
        {
            return Err("user_typing");
        }
        Ok(())
    }
    async fn command(&mut self, command: Command) -> bool {
        match command {
            Command::Inject { proof, text, reply } => {
                let result = self.input_gate(&proof).and_then(|_| {
                    if self.consumed_idle.is_some_and(|at| proof.idle_since <= at)
                        || !self.input.is_empty()
                        || self.clients.values().any(|c| c.input.deadline().is_some())
                    {
                        return Err("target_busy_pty");
                    }
                    if text.len() > 40 * 1024
                        || text
                            .chars()
                            .any(|c| c.is_control() && c != '\n' && c != '\t')
                    {
                        return Err("unsafe_pty_text");
                    }
                    Ok(())
                });
                if let Err(error) = result {
                    let _ = reply.send(Err(error.into()));
                } else if !reply.is_closed() {
                    let bytes = format!("\x1b[200~{text}\x1b[201~\r").into_bytes();
                    self.consumed_idle = Some(proof.idle_since);
                    self.input_bytes += bytes.len();
                    self.input.push_back(InputUnit {
                        bytes,
                        origin: InputOrigin::Message(Box::new(Submission { reply, proof })),
                    });
                }
            }
            Command::Clipboard {
                id,
                lease,
                connection,
            } => {
                if self.clients.contains_key(&id) {
                    self.clipboard.bind(id, lease, connection);
                }
            }
            Command::Start { size, reply } => {
                let result = self.start(size, None).await.map_err(|e| e.to_string());
                let _ = reply.send(result);
            }
            Command::Attach {
                size,
                term,
                out,
                reply,
            } => {
                if self.clients.len() >= 16 || self.info.state == "exited" || self.killing.is_some()
                {
                    let _ = reply.send(Err("session is unavailable".into()));
                    return true;
                }
                if self.pty.is_none()
                    && let Err(error) = self
                        .start(self.launch.size.unwrap_or(size), Some(&term))
                        .await
                {
                    let _ = reply.send(Err(error.to_string()));
                    return true;
                }
                self.next += 1;
                let id = self.next;
                self.clients.insert(
                    id,
                    Client {
                        size,
                        input: Input::default(),
                        out: out.clone(),
                    },
                );
                self.resize();
                let restored = self.terminal.restore();
                if out
                    .try_send(Out::Control(Reply::Attached {
                        client_id: id,
                        size: self.info.size,
                    }))
                    .is_err()
                    || out.try_send(Out::Data(restored.into())).is_err()
                {
                    self.remove_client(id);
                    self.resize();
                    let _ = reply.send(Err("client disconnected".into()));
                } else if reply.send(Ok(id)).is_err() {
                    self.remove_client(id);
                    self.resize();
                }
            }
            Command::Input { id, bytes } => {
                if self.killing.is_some() || self.pending_exit.is_some() {
                    return true;
                }
                if let Some(client) = self.clients.get_mut(&id) {
                    if !bytes.is_empty() {
                        self.last_input = Some(tower_pal::monotonic_ms());
                    }
                    let units = client.input.push(&bytes, Instant::now());
                    match units {
                        Err(()) => {
                            self.remove_client(id);
                            self.resize();
                        }
                        Ok(units) => {
                            match self.clipboard.push(id, units, MAX_INPUT - self.input_bytes) {
                                Ok(units) => {
                                    if !self.enqueue(units) {
                                        self.remove_client(id);
                                        self.resize();
                                    }
                                }
                                Err(code) => {
                                    if let Some(c) = self.clients.get(&id) {
                                        let _ = c.out.try_send(Out::Control(Reply::Clipboard {
                                            state: "failed".into(),
                                            code: Some(code.clone()),
                                        }));
                                    }
                                    if code == "input_overflow" {
                                        self.remove_client(id);
                                        self.resize();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Command::Resize { id, size } => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.size = size;
                    self.resize();
                }
            }
            Command::Detach { id } => {
                self.remove_client(id);
                self.resize();
            }
            Command::Snapshot(reply) => {
                let _ = reply.send(Snapshot {
                    size: self.info.size,
                    truncated: self.terminal.truncated,
                    limited: self.terminal.limited,
                    buffer: self.terminal.snapshot(),
                });
            }
            Command::Kill(reply) => {
                // Input backpressure must not prevent reading the final output
                // and EOF after termination. No queued input can be useful now.
                self.clear_input();
                if let Some(p) = &self.pty {
                    let _ = p.signal(false);
                    self.killing
                        .get_or_insert(Instant::now() + Duration::from_secs(1));
                    self.kill_replies.push(reply);
                } else {
                    self.exited(0);
                    let _ = reply.send(());
                }
            }
            Command::Stop(reply) => {
                self.exited(137);
                let _ = reply.send(());
                return false;
            }
        }
        true
    }
}
