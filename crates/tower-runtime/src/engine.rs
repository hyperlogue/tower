use crate::{
    Binding, Journal, Result, error, internal, millis, parse_time, random_id, timestamp, wire,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    sync::mpsc,
};
use tower_protocol::{ExecutionCommand, ExecutionError, ExecutionResult, RuntimeRequest};

pub(crate) struct LiveRequest {
    pub wire: RuntimeRequest,
    pub after_sequence: u64,
    pub method: String,
    pub consumed: bool,
    pub announced: Option<(String, u64)>,
    pub deny_decision: String,
    pub peer_retired: Option<tokio::time::Instant>,
}
pub(crate) type Requests = Arc<Mutex<BTreeMap<String, LiveRequest>>>;
pub(crate) enum Control {
    Command(ExecutionCommand),
    Stop(String),
}
pub(crate) struct Handle {
    pub normal: mpsc::Sender<ExecutionCommand>,
    pub control: mpsc::Sender<Control>,
    pub requests: Requests,
}
pub(crate) fn channels() -> (
    Arc<Handle>,
    mpsc::Receiver<ExecutionCommand>,
    mpsc::Receiver<Control>,
) {
    let (normal, n) = mpsc::channel(16);
    let (control, c) = mpsc::channel(16);
    (
        Arc::new(Handle {
            normal,
            control,
            requests: Arc::new(Mutex::new(BTreeMap::new())),
        }),
        n,
        c,
    )
}
fn public(v: Value) -> Result<tower_protocol::Value> {
    serde_json::from_value(v).map_err(internal)
}
fn failure(code: &str, stage: &str, effect: bool) -> ExecutionError {
    ExecutionError {
        code: code.into(),
        message: match code {
            "command_expired" => "Command expired before dispatch",
            "denied" => "Current policy does not authorize this operation",
            _ => "Native operation did not complete",
        }
        .into(),
        stage: stage.into(),
        may_have_effect: effect,
    }
}
pub(crate) fn validate_reply(request: &LiveRequest, p: &Value) -> Result<Value> {
    if request.consumed
        || p["source_request_id"] != request.wire.source_request_id
        || p["request_revision"] != request.wire.request_revision
        || p["generation"] != request.wire.generation
    {
        return Err(error("stale_request", "Request is no longer live"));
    }
    let r = &p["response"];
    if request.wire.kind == "elicitation" {
        let payload = serde_json::to_value(&request.wire.payload).map_err(internal)?;
        return crate::controls::answer(&request.method, &payload, r);
    }
    let option = if r.pointer("/outcome/outcome").and_then(Value::as_str) == Some("cancelled") {
        "reject_once"
    } else if r.pointer("/outcome/outcome").and_then(Value::as_str) == Some("selected") {
        r.pointer("/outcome/optionId")
            .and_then(Value::as_str)
            .unwrap_or("")
    } else {
        ""
    };
    let payload = serde_json::to_value(&request.wire.payload).map_err(internal)?;
    if !payload["options"]
        .as_array()
        .is_some_and(|a| a.iter().any(|o| o["optionId"] == option))
    {
        return Err(error(
            "invalid_response",
            "Choose a current offered permission option",
        ));
    }
    let allow = option == "allow_once";
    match request.method.as_str() {
        "session/request_permission" => Ok(r.clone()),
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Ok(json!({"decision":if allow{"accept"}else{&request.deny_decision}}))
        }
        _ => Err(error(
            "unsupported",
            "Native request has no supported reply",
        )),
    }
}
pub(crate) fn cleanup_command(c: &ExecutionCommand) -> bool {
    if c.kind == "cancel" {
        return true;
    }
    let p = serde_json::to_value(&c.payload).unwrap_or(Value::Null);
    c.kind == "reply"
        && (p
            .pointer("/response/outcome/outcome")
            .and_then(Value::as_str)
            == Some("cancelled")
            || p.pointer("/response/outcome/optionId")
                .and_then(Value::as_str)
                == Some("reject_once")
            || matches!(
                p.pointer("/response/action").and_then(Value::as_str),
                Some("decline" | "cancel")
            ))
}
async fn authorize(paths: &tower_pal::Paths, b: &Binding, c: &ExecutionCommand) -> Result<()> {
    let token =
        tower_pal::read_private(&paths.state.join("runtime/owner.key")).map_err(internal)?;
    let token = String::from_utf8(token).map_err(internal)?;
    let mut socket = tower_pal::connect(&paths.socket).await.map_err(|_| {
        error(
            "authority_unavailable",
            "Tower authorization service is unavailable",
        )
    })?;
    let request = json!({"jsonrpc":"2.0","id":"runtime-authorize","method":"runtime.authorize","params":{"credential":token,"binding":b,"command_id":c.command_id,"kind":c.kind,"cleanup":cleanup_command(c),"expires_at":c.expires_at}});
    wire::json_line(&mut socket, &request).await?;
    let v = tokio::time::timeout(
        Duration::from_secs(5),
        wire::line(&mut BufReader::new(socket)),
    )
    .await
    .map_err(|_| error("authority_unavailable", "Tower authorization timed out"))??
    .ok_or_else(|| error("authority_unavailable", "Tower authorization closed"))?;
    if v["result"]["authorized"] == true {
        Ok(())
    } else {
        Err(error(
            "denied",
            "Current control connection and policy are required",
        ))
    }
}

pub(crate) async fn run(
    paths: tower_pal::Paths,
    journal: Arc<Journal>,
    binding: Binding,
    handle: Arc<Handle>,
    mut normal: mpsc::Receiver<ExecutionCommand>,
    mut control: mpsc::Receiver<Control>,
) {
    let result = run_inner(
        &paths,
        &journal,
        &binding,
        &handle,
        &mut normal,
        &mut control,
    )
    .await;
    if let Err(e) = result {
        let known_before_spawn = journal
            .metadata(&binding)
            .is_ok_and(|v| v.get("pid").is_none())
            && matches!(
                e.data.code.as_str(),
                "denied"
                    | "authority_unavailable"
                    | "native_unavailable"
                    | "environment_unavailable"
                    | "runtime_cancelled"
                    | "command_expired"
            );
        let _ = journal.fence(
            &binding,
            &e.data.code,
            if known_before_spawn {
                "confirmed"
            } else {
                "unknown"
            },
        );
    }
    if let Ok(mut requests) = handle.requests.lock() {
        for request in requests.values_mut() {
            request.wire.state = "stale".into();
            request.wire.payload = tower_protocol::Value::Object(Default::default());
            request.wire.request_revision += 1;
            request.consumed = true;
        }
    }
}
struct Process {
    child: tokio::process::Child,
    identity: tower_pal::ProcessIdentity,
    reader: tokio::task::JoinHandle<()>,
}
async fn group_terminated(identity: tower_pal::ProcessIdentity) -> bool {
    for _ in 0..40 {
        if tower_pal::protocol_group_terminated(identity).unwrap_or(false) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = tower_pal::kill_protocol_group(self.identity);
        self.reader.abort();
    }
}
struct State<'a> {
    journal: &'a Journal,
    binding: &'a Binding,
    handle: &'a Handle,
    native: String,
    acp: String,
    metadata: Value,
    active: Option<String>,
    stop_command: Option<String>,
    streamed: std::collections::HashSet<String>,
    turn: Option<String>,
    pending: BTreeMap<String, (String, String)>,
    counter: u64,
    queue: VecDeque<ExecutionCommand>,
    dropped: u64,
    dropped_from: Option<String>,
    dropped_to: Option<String>,
    text: Option<BufferedText>,
}
struct BufferedText {
    kind: String,
    payload: Value,
    command: Option<String>,
    text: String,
    since: tokio::time::Instant,
}
impl State<'_> {
    fn ready(&self) -> bool {
        if self.binding.protocol == "acp" {
            !self.acp.is_empty() && !self.pending.values().any(|(_, kind)| kind == "session")
        } else {
            !self.native.is_empty()
        }
    }
    fn append(
        &mut self,
        kind: &str,
        mut payload: Value,
        command: Option<String>,
        critical: bool,
    ) -> Result<()> {
        if !critical
            && matches!(kind, "agent_message_chunk" | "agent_thought_chunk")
            && payload.pointer("/content/type").and_then(Value::as_str) == Some("text")
            && let Some(text) = payload
                .pointer("/content/text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        {
            payload["content"]["text"] = Value::Null;
            let mut rest = text.as_str();
            while !rest.is_empty() {
                if self.text.as_ref().is_some_and(|b| {
                    b.kind != kind
                        || b.payload != payload
                        || b.command != command
                        || b.text.len() >= 32 * 1024
                }) {
                    self.flush_text()?;
                }
                let buffer = self.text.get_or_insert_with(|| BufferedText {
                    kind: kind.into(),
                    payload: payload.clone(),
                    command: command.clone(),
                    text: String::new(),
                    since: tokio::time::Instant::now(),
                });
                let mut n = (32 * 1024 - buffer.text.len()).min(rest.len());
                while n > 0 && !rest.is_char_boundary(n) {
                    n -= 1;
                }
                if n == 0 {
                    self.flush_text()?;
                    continue;
                }
                buffer.text.push_str(&rest[..n]);
                rest = &rest[n..];
            }
            return Ok(());
        }
        self.flush_text()?;
        self.store_event(kind, payload, command, critical)
    }
    fn flush_text(&mut self) -> Result<()> {
        if let Some(mut b) = self.text.take() {
            b.payload["content"]["text"] = json!(b.text);
            self.store_event(&b.kind, b.payload, b.command, false)?;
        }
        Ok(())
    }
    fn dropped(&mut self) {
        self.dropped += 1;
        let at = timestamp();
        if self.dropped_from.is_none() {
            self.dropped_from = Some(at.clone());
        }
        self.dropped_to = Some(at);
    }
    fn store_event(
        &mut self,
        kind: &str,
        payload: Value,
        command: Option<String>,
        critical: bool,
    ) -> Result<()> {
        if !critical
            && !self.journal.bulk_capacity(
                self.binding,
                serde_json::to_vec(&payload).map_err(internal)?.len() + 1024,
            )?
        {
            self.dropped();
            return Ok(());
        }
        if self.dropped > 0 {
            let gap = json!({"reason":"native_output_dropped","coverage":"partial","native_range":{"from":null,"to":null,"count":null},"time_range":{"from":self.dropped_from,"to":self.dropped_to}});
            self.journal
                .append(self.binding, "gap", public(gap)?, None, true)?;
            self.dropped = 0;
            self.dropped_from = None;
            self.dropped_to = None;
        }
        match self
            .journal
            .append(self.binding, kind, public(payload)?, command, critical)
        {
            Ok(_) => Ok(()),
            Err(e) if !critical && e.data.code == "spool_capacity" => {
                self.dropped();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    fn state(&mut self, state: &str) -> Result<()> {
        if !self.native.is_empty() {
            self.metadata["native_session_id"] = json!(self.native);
        }
        if !self.acp.is_empty() {
            self.metadata["acp_session_id"] = json!(self.acp);
        }
        self.journal
            .update_metadata(self.binding, state, &self.metadata)?;
        let mut payload = json!({"state":match state {"working"=>"running","waiting_approval"|"waiting_input"=>"waiting",other=>other},"tower_session_id":self.binding.tower_session_id,"owner_id":self.binding.owner_id});
        if !self.native.is_empty() {
            payload["native_session_id"] = json!(self.native);
        }
        if !self.acp.is_empty() {
            payload["acp_session_id"] = json!(self.acp);
        }
        self.append("runtime_state", payload, None, true)
    }
    fn result(&self, outcome: Option<&str>) -> ExecutionResult {
        ExecutionResult {
            tower_session_id: Some(self.binding.tower_session_id.clone()),
            native_session_id: (!self.native.is_empty()).then(|| self.native.clone()),
            acp_session_id: (!self.acp.is_empty()).then(|| self.acp.clone()),
            owner_id: Some(self.binding.owner_id.clone()),
            outcome: outcome.map(str::to_owned),
        }
    }
    fn done(&self, id: &str, outcome: Option<&str>, failure: Option<ExecutionError>) -> Result<()> {
        self.journal.finish_dispatch(
            self.binding,
            id,
            true,
            Some(self.result(outcome)),
            failure,
        )?;
        Ok(())
    }
    async fn rpc(
        &mut self,
        input: &mut tokio::process::ChildStdin,
        method: &str,
        params: Value,
        command: &str,
        kind: &str,
    ) -> Result<()> {
        self.counter += 1;
        let id = format!("tower-{}", self.counter);
        self.pending
            .insert(id.clone(), (command.into(), kind.into()));
        wire::json_line(
            input,
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )
        .await
    }
    fn stale_requests(&self) -> Result<()> {
        let mut requests = self.handle.requests.lock().map_err(internal)?;
        for r in requests.values_mut().filter(|r| r.wire.state == "pending") {
            r.wire.state = "stale".into();
            r.wire.payload = tower_protocol::Value::Object(Default::default());
            r.wire.request_revision += 1;
            r.consumed = true;
        }
        Ok(())
    }
    fn end_turn(&mut self, reason: &str) -> Result<()> {
        if let Some(command) = self.active.take() {
            self.stale_requests()?;
            self.append(
                "turn_end",
                json!({"stop_reason":reason}),
                Some(command.clone()),
                true,
            )?;
            self.done(
                &command,
                Some(if reason == "cancelled" {
                    "cancelled"
                } else if reason == "error" {
                    "rejected"
                } else {
                    "completed"
                }),
                if reason == "error" {
                    Some(failure("native_error", "execution", true))
                } else {
                    None
                },
            )?;
            self.turn = None;
            self.state("idle")?;
        }
        Ok(())
    }
}

async fn run_inner(
    paths: &tower_pal::Paths,
    journal: &Journal,
    b: &Binding,
    handle: &Handle,
    normal: &mut mpsc::Receiver<ExecutionCommand>,
    control: &mut mpsc::Receiver<Control>,
) -> Result<()> {
    let launch = normal
        .recv()
        .await
        .ok_or_else(|| error("runtime_lost", "Launch was not supplied"))?;
    authorize(paths, b, &launch).await?;
    let env = tower_agents::environment::project_environment(
        std::path::Path::new(&b.cwd),
        &tower_pal::executable().map_err(internal)?,
        tower_pal::environment(),
    )
    .await
    .map_err(|_| {
        error(
            "environment_unavailable",
            "Project environment is unavailable",
        )
    })?;
    let mut env = env;
    let native_home = tower_agents::Harness::parse(&b.harness)
        .and_then(|h| tower_agents::inventory::namespace(h, &env));
    if b.resume_session_id.is_some() {
        let previous = journal
            .bindings(Some(&b.namespace()))?
            .into_iter()
            .rev()
            .find(|p| {
                p.runtime_id != b.runtime_id
                    && p.session_id == b.session_id
                    && journal.metadata(p).is_ok_and(|m| {
                        m[if b.protocol == "acp" {
                            "acp_session_id"
                        } else {
                            "native_session_id"
                        }]
                        .as_str()
                            == b.resume_session_id.as_deref()
                    })
            })
            .ok_or_else(|| {
                error(
                    "resume_unavailable",
                    "Previous native environment is unavailable",
                )
            })?;
        let metadata = journal.metadata(&previous)?;
        if metadata["native_home"].as_str() != native_home.as_deref() {
            return Err(error(
                "native_home_changed",
                "Native history location changed since the previous execution",
            ));
        }
    }
    for key in [
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_AGENT_ID",
        "TOWER_LAUNCHER_SOCKET",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_CHILD_SESSION",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "CLAUDE_PID",
        "CLAUDECODE",
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
    ] {
        env.remove(key);
    }
    env.insert("TOWER_AGENT_ID".into(), b.tower_session_id.clone());
    env.insert(
        "TOWER_SOCKET".into(),
        paths.socket.to_string_lossy().into_owned(),
    );
    let token = tower_pal::read_private(&paths.token_path(&b.tower_session_id).map_err(internal)?)
        .map_err(internal)?;
    env.insert(
        "TOWER_AGENT_TOKEN".into(),
        String::from_utf8(token).map_err(internal)?,
    );
    authorize(paths, b, &launch).await?;
    if control.try_recv().is_ok() {
        return Err(error(
            "runtime_cancelled",
            "Launch was cancelled before native dispatch",
        ));
    }
    if !journal.begin_dispatch(b, &launch.command_id)? {
        return Err(error("runtime_lost", "Launch dispatch is already fenced"));
    }
    let argv = if b.protocol == "codex-app-server" {
        vec!["codex".into(), "app-server".into(),"--stdio".into(),"-c".into(),format!("mcp_servers.tower.command={}",serde_json::to_string(&tower_pal::executable().map_err(internal)?.to_string_lossy()).map_err(internal)?),"-c".into(),"mcp_servers.tower.args=[\"mcp\"]".into(),"-c".into(),"mcp_servers.tower.env_vars=[\"TOWER_AGENT_ID\",\"TOWER_AGENT_TOKEN\",\"TOWER_SOCKET\"]".into()]
    } else {
        vec![
            tower_pal::executable()
                .map_err(internal)?
                .to_string_lossy()
                .into_owned(),
            "__acp".into(),
            b.harness.clone(),
        ]
    };
    let mut child = tower_pal::spawn_protocol(&argv, &env, std::path::Path::new(&b.cwd))
        .map_err(|_| error("native_unavailable", "Cannot start native agent"))?;
    let identity = tower_pal::process_identity(
        child
            .id()
            .ok_or_else(|| error("native_unavailable", "Missing native process"))?,
    )
    .map_err(internal)?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| error("native_protocol", "Missing native stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| error("native_protocol", "Missing native stdout"))?;
    let (tx, mut incoming) = mpsc::channel(32);
    let reader = tokio::spawn(async move {
        let mut r = BufReader::new(stdout);
        loop {
            match wire::line(&mut r).await {
                Ok(Some(v)) => {
                    if tx.send(Ok(v)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });
    let mut process = Process {
        child,
        identity,
        reader,
    };
    let mut s = State {
        journal,
        binding: b,
        handle,
        native: String::new(),
        acp: String::new(),
        metadata: json!({"pid":identity.pid,"pid_start_time":identity.start_time,"boot_id":tower_pal::boot_id().map_err(internal)?,"native_home":native_home}),
        active: None,
        stop_command: None,
        streamed: Default::default(),
        turn: None,
        pending: BTreeMap::new(),
        counter: 0,
        queue: VecDeque::new(),
        dropped: 0,
        dropped_from: None,
        dropped_to: None,
        text: None,
    };
    journal.update_metadata(b, "starting", &s.metadata)?;
    let init = if b.protocol == "codex-app-server" {
        json!({"clientInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})
    } else {
        json!({"protocolVersion":1,"clientInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")},"clientCapabilities":{"elicitation":{"form":{},"url":{}}}})
    };
    s.rpc(
        &mut input,
        "initialize",
        init,
        &launch.command_id,
        "initialize",
    )
    .await?;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let started = tokio::time::Instant::now();
    let result:Result<()>=async {loop {
        tokio::select! {biased;
            command=control.recv()=>{
                match command {
                    Some(Control::Stop(reason)) => {
                        s.stale_requests()?;
                        let _ = tower_pal::kill_protocol_group(identity);
                        process.child.wait().await.map_err(internal)?;
                        let terminated = group_terminated(identity).await;
                        journal.fence(b, &reason, if terminated { "confirmed" } else { "unknown" })?;
                        return Ok(());
                    }
                    Some(Control::Command(c)) => dispatch(paths, &mut s, &mut input, c).await?,
                    None => return Err(error("runtime_lost", "Runtime control channel closed")),
                }
            }
            message=incoming.recv()=>{
                let Some(message) = message else {
                    return Err(error("native_exited", "Native output closed"));
                };
                native(&mut s, &mut input, message?).await?;
            }
            command=normal.recv(),if s.queue.len()<16=>{
                if let Some(c) = command {
                    s.queue.push_back(c);
                } else {
                    return Err(error("runtime_lost", "Runtime command channel closed"));
                }
            }
            _=tick.tick()=>{
                if s.handle
                    .requests
                    .lock()
                    .map_err(internal)?
                    .values()
                    .any(|r| {
                        !r.consumed
                            && r.peer_retired
                                .is_some_and(|at| at.elapsed() >= Duration::from_secs(30))
                    })
                {
                    return Err(error(
                        "private_reply_lost",
                        "The peer retired a request without a native reply",
                    ));
                }
                if s.text
                    .as_ref()
                    .is_some_and(|b| b.since.elapsed() >= Duration::from_millis(50))
                {
                    s.flush_text()?;
                }
                if !s.ready() && started.elapsed() > Duration::from_secs(60) {
                    return Err(error("native_timeout", "Native initialization timed out"));
                }
                if s.ready()
                    && s.active.is_none()
                    && let Some(c) = s.queue.pop_front()
                {
                    dispatch(paths, &mut s, &mut input, c).await?;
                }
            }
        }
    }}.await;
    s.stale_requests()?;
    let _ = input.shutdown().await;
    let _ = tower_pal::kill_protocol_group(identity);
    let reaped = tokio::time::timeout(Duration::from_secs(5), process.child.wait())
        .await
        .is_ok_and(|r| r.is_ok());
    let terminated = reaped && group_terminated(identity).await;
    if let Err(e) = result {
        if let Some(command) = s.stop_command.take() {
            if terminated {
                s.end_turn("cancelled")?;
                s.done(&command, Some("stopped"), None)?;
            } else {
                s.done(
                    &command,
                    None,
                    Some(failure("dispatch_unknown", "execution", true)),
                )?;
            }
        }
        journal.fence(
            b,
            &e.data.code,
            if terminated { "confirmed" } else { "unknown" },
        )?;
    }
    Ok(())
}

async fn dispatch(
    paths: &tower_pal::Paths,
    s: &mut State<'_>,
    input: &mut tokio::process::ChildStdin,
    c: ExecutionCommand,
) -> Result<()> {
    let checked = async {
        if parse_time(&c.expires_at)? <= millis() {
            return Err(error("command_expired", "Command expired"));
        }
        authorize(paths, s.binding, &c).await?;
        Ok(())
    }
    .await;
    if let Err(e) = checked {
        s.journal.finish_dispatch(
            s.binding,
            &c.command_id,
            false,
            None,
            Some(failure(&e.data.code, "dispatch", false)),
        )?;
        return Ok(());
    }
    if !s.journal.begin_dispatch(s.binding, &c.command_id)? {
        return Ok(());
    }
    let p = serde_json::to_value(&c.payload).map_err(internal)?;
    let codex = s.binding.protocol == "codex-app-server";
    let result:Result<()>=async {
        match c.kind.as_str() {
            "prompt" => {
                if s.active.is_some() {
                    return Err(error("target_busy", "A prompt is already running"));
                }
                let blocks = p["content"]
                    .as_array()
                    .filter(|a| !a.is_empty())
                    .ok_or_else(|| error("invalid_params", "Prompt content is required"))?;
                if blocks.iter().any(|b| {
                    b["type"] != "text" || !b["text"].is_string() || b.get("asset_id").is_some()
                }) {
                    return Err(error("unsupported_content", "Only text input is enabled"));
                }
                s.active = Some(c.command_id.clone());
                s.state("working")?;
                s.append(
                    "user_message",
                    json!({"content":blocks}),
                    Some(c.command_id.clone()),
                    false,
                )?;
                if codex {
                    let content: Vec<_> = blocks
                        .iter()
                        .map(|b| json!({"type":"text","text":b["text"],"text_elements":[]}))
                        .collect();
                    let mut params = json!({"threadId":s.native,"input":content});
                    if let Some(config) = s.metadata.get("config").and_then(Value::as_object) {
                        for (k, v) in config {
                            if k != "mode" {
                                params[k] = v.clone();
                            }
                        }
                        if let Some(mode) = config.get("mode") {
                            let model = config
                                .get("model")
                                .or_else(|| s.metadata.get("model"))
                                .filter(|v| v.is_string())
                                .ok_or_else(|| {
                                    error(
                                        "unsupported_config",
                                        "Native model identity is required for mode changes",
                                    )
                                })?;
                            params["collaborationMode"] = json!({"mode":mode,"settings":{"model":model,"reasoning_effort":config.get("effort").or_else(||s.metadata.get("effort")),"developer_instructions":null}});
                        }
                    }
                    s.rpc(input, "turn/start", params, &c.command_id, "prompt_start")
                        .await?;
                } else {
                    s.rpc(
                        input,
                        "session/prompt",
                        json!({"sessionId":s.acp,"prompt":blocks}),
                        &c.command_id,
                        "prompt_end",
                    )
                    .await?;
                }
            }
            "cancel" => {
                s.stale_requests()?;
                if s.binding.harness == "antigravity" {
                    return Err(error(
                        "stop_runtime",
                        "Native cancellation stops this runtime",
                    ));
                }
                if codex {
                    if let Some(turn) = s.turn.clone() {
                        s.rpc(
                            input,
                            "turn/interrupt",
                            json!({"threadId":s.native,"turnId":turn}),
                            &c.command_id,
                            "cancel",
                        )
                        .await?;
                    } else if s.active.is_some() {
                        return Err(error(
                            "turn_starting",
                            "Wait for native turn identity before interrupting",
                        ));
                    } else {
                        s.done(&c.command_id, Some("cancelled"), None)?;
                    }
                } else {
                    wire::json_line(input,&json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":s.acp}})).await?;
                    s.done(&c.command_id, None, None)?;
                }
            }
            "reply" => {
                let key = p["source_request_id"]
                    .as_str()
                    .ok_or_else(|| error("invalid_params", "Request occurrence required"))?;
                let (id, result) = {
                    let mut live = s.handle.requests.lock().map_err(internal)?;
                    let r = live.get_mut(key).ok_or_else(|| {
                        error("stale_request", "Request owner no longer has private state")
                    })?;
                    let result = validate_reply(r, &p)?;
                    let id = serde_json::to_value(&r.wire.native_request_id).map_err(internal)?;
                    live.remove(key);
                    (id, result)
                };
                wire::json_line(input, &json!({"jsonrpc":"2.0","id":id,"result":result})).await?;
                s.done(&c.command_id, None, None)?;
                s.state("working")?;
            }
            "configure" if codex => {
                let config = p["config"]
                    .as_object()
                    .ok_or_else(|| error("invalid_params", "Configuration object required"))?;
                for (k, v) in config {
                    if !matches!(k.as_str(), "model" | "effort" | "mode")
                        || !v.as_str().is_some_and(|s| {
                            !s.is_empty() && s.len() < 128 && !s.chars().any(char::is_control)
                        })
                    {
                        return Err(error(
                            "unsupported_config",
                            "Only model, effort, and mode settings are enabled",
                        ));
                    }
                    if k == "mode" && !matches!(v.as_str(), Some("plan" | "default")) {
                        return Err(error("invalid_params", "Mode must be plan or default"));
                    }
                    if k == "effort"
                        && !matches!(
                            v.as_str(),
                            Some("none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max")
                        )
                    {
                        return Err(error("invalid_params", "Invalid reasoning effort"));
                    }
                }
                s.metadata["config"] = json!(config);
                s.journal.update_metadata(s.binding, "idle", &s.metadata)?;
                s.done(&c.command_id, Some("configured"), None)?;
            }
            _ => {
                return Err(error(
                    "unsupported",
                    "Command is unavailable for this runtime",
                ));
            }
        }
        Ok(())
    }.await;
    if let Err(e) = result {
        if e.data.code == "stop_runtime" {
            s.stop_command = Some(c.command_id);
            return Err(e);
        }
        if e.data.code == "dispatch_unknown" {
            return Err(e);
        }
        s.journal.finish_dispatch(
            s.binding,
            &c.command_id,
            false,
            None,
            Some(failure(&e.data.code, "dispatch", false)),
        )?;
        if s.active.as_deref() == Some(&c.command_id) {
            s.end_turn("error")?;
        }
    }
    Ok(())
}
async fn native(s: &mut State<'_>, input: &mut tokio::process::ChildStdin, v: Value) -> Result<()> {
    let codex = s.binding.protocol == "codex-app-server";
    if let Some(method) = v["method"].as_str() {
        let p = &v["params"];
        if let Some(id) = v.get("id") {
            if !id.as_str().is_some_and(crate::identifier)
                && !id
                    .as_i64()
                    .is_some_and(|n| n.unsigned_abs() <= 9_007_199_254_740_991)
            {
                return Err(error(
                    "native_protocol",
                    "Native request identity must be a bounded string or safe integer",
                ));
            }
            let target = if codex {
                p["threadId"].as_str()
            } else {
                p["sessionId"].as_str()
            };
            let owned = target == Some(if codex { &s.native } else { &s.acp })
                && (!codex || (s.turn.is_some() && p["turnId"].as_str() == s.turn.as_deref()));
            let supported = matches!(
                method,
                "session/request_permission"
                    | "item/commandExecution/requestApproval"
                    | "item/fileChange/requestApproval"
                    | "item/tool/requestUserInput"
                    | "mcpServer/elicitation/request"
                    | "elicitation/create"
            );
            if !owned || s.active.is_none() || !supported {
                wire::json_line(input,&json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"This runtime cannot answer this request"}})).await?;
                return Ok(());
            }
            let source = random_id("request_")?;
            let elicitation = match crate::controls::elicitation(method, p) {
                Ok(value) => value,
                Err(e) => {
                    wire::json_line(input,&json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":e.message}})).await?;
                    return Ok(());
                }
            };
            let kind = if elicitation.is_some() {
                "elicitation"
            } else {
                "permission"
            };
            let mode = elicitation
                .as_ref()
                .and_then(|p| p["mode"].as_str())
                .map(str::to_owned);
            let choices = p.get("availableDecisions").and_then(Value::as_array);
            let offered =
                |decision: &str| choices.is_none_or(|values| values.iter().any(|v| v == decision));
            let deny_decision = if codex && !offered("decline") {
                if offered("cancel") {
                    "cancel"
                } else {
                    return Err(error(
                        "native_protocol",
                        "Native approval has no safe denial choice",
                    ));
                }
            } else {
                "decline"
            };
            let payload = if let Some(payload) = elicitation {
                payload
            } else if codex {
                let mut options = Vec::new();
                if offered("accept") {
                    options.push(
                        json!({"optionId":"allow_once","kind":"allow_once","name":"Allow once"}),
                    );
                }
                options.push(json!({"optionId":"reject_once","kind":"reject_once","name":if deny_decision=="cancel"{"Deny and cancel turn"}else{"Deny"}}));
                json!({"toolCall":{"toolCallId":p["itemId"],"title":p.get("command").or_else(||p.get("reason")).cloned().unwrap_or(json!("Approve native operation")),"rawInput":p},"options":options})
            } else {
                p.clone()
            };
            let request = RuntimeRequest {
                runtime_id: s.binding.runtime_id.clone(),
                generation: s.binding.generation.clone(),
                owner_id: s.binding.owner_id.clone(),
                source_request_id: source.clone(),
                request_revision: 1,
                native_request_id: serde_json::from_value(id.clone()).map_err(internal)?,
                kind: kind.into(),
                state: "pending".into(),
                payload: public(payload)?,
            };
            let request_bytes = serde_json::to_vec(&request).map_err(internal)?.len();
            if request_bytes > 256 * 1024 {
                return Err(error(
                    "control_capacity",
                    "Private request exceeds its size limit",
                ));
            }
            let mut marker = json!({"redacted":true,"kind":kind,"source_request_id":source,"request_revision":1});
            if let Some(mode) = mode {
                marker["mode"] = json!(mode);
            }
            s.append(
                if kind == "permission" {
                    "permission_request"
                } else {
                    "elicitation_request"
                },
                marker,
                s.active.clone(),
                true,
            )?;
            s.state(if kind == "permission" {
                "waiting_approval"
            } else {
                "waiting_input"
            })?;
            let after_sequence = s.journal.latest_event_sequence(s.binding)?;
            {
                let mut requests = s.handle.requests.lock().map_err(internal)?;
                if requests
                    .values()
                    .any(|r| !r.consumed && r.wire.native_request_id == request.native_request_id)
                {
                    return Err(error(
                        "native_request_conflict",
                        "Native request identity was reused before completion",
                    ));
                }
                let bytes = requests
                    .values()
                    .try_fold(request_bytes, |total, r| {
                        serde_json::to_vec(&r.wire).map(|v| total + v.len())
                    })
                    .map_err(internal)?;
                if requests.len() >= 32 || bytes > 1024 * 1024 {
                    return Err(error(
                        "control_capacity",
                        "Too many pending native requests",
                    ));
                }
                requests.insert(
                    source.clone(),
                    LiveRequest {
                        wire: request,
                        after_sequence,
                        method: method.into(),
                        consumed: false,
                        announced: None,
                        deny_decision: deny_decision.into(),
                        peer_retired: None,
                    },
                );
            }
            return Ok(());
        }
        if codex {
            if p.get("threadId")
                .and_then(Value::as_str)
                .is_some_and(|id| id != s.native)
            {
                return Ok(());
            }
            match method {
                "turn/started" => {
                    if s.active.is_some() {
                        s.turn = p
                            .pointer("/turn/id")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                    }
                }
                "turn/completed" => {
                    if s.active.is_some()
                        && p.pointer("/turn/id").and_then(Value::as_str) == s.turn.as_deref()
                    {
                        let status = p
                            .pointer("/turn/status")
                            .and_then(Value::as_str)
                            .unwrap_or("failed");
                        s.end_turn(match status {
                            "completed" => "end_turn",
                            "interrupted" => "cancelled",
                            _ => "error",
                        })?;
                    }
                }
                "item/agentMessage/delta" => {
                    if s.active.is_some()
                        && let Some(text) = p["delta"].as_str()
                    {
                        if let Some(id) = p["itemId"].as_str() {
                            s.streamed.insert(id.into());
                        }
                        s.append(
                            "agent_message_chunk",
                            json!({"content":{"type":"text","text":text},"item_id":p["itemId"]}),
                            s.active.clone(),
                            false,
                        )?;
                    }
                }
                "item/started" | "item/completed" => {
                    let item = &p["item"];
                    if method == "item/completed"
                        && item["type"] == "agentMessage"
                        && s.active.is_some()
                    {
                        let id = item["id"].as_str().unwrap_or("");
                        if !s.streamed.remove(id)
                            && let Some(text) = item["text"].as_str()
                        {
                            s.append(
                                "agent_message_chunk",
                                json!({"content":{"type":"text","text":text},"item_id":id}),
                                s.active.clone(),
                                false,
                            )?;
                        }
                    }
                    if matches!(
                        item["type"].as_str(),
                        Some("commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall")
                    ) {
                        s.append(
                            if method == "item/started" {
                                "tool_call"
                            } else {
                                "tool_call_update"
                            },
                            item.clone(),
                            s.active.clone(),
                            false,
                        )?;
                    }
                }
                "serverRequest/resolved" => {
                    let id = &p["requestId"];
                    let mut requests = s.handle.requests.lock().map_err(internal)?;
                    for r in requests.values_mut() {
                        if serde_json::to_value(&r.wire.native_request_id).map_err(internal)? == *id
                            && !r.consumed
                        {
                            r.consumed = true;
                            r.wire.state = "stale".into();
                            r.wire.payload = tower_protocol::Value::Object(Default::default());
                            r.wire.request_revision += 1;
                        }
                    }
                }
                _ => {}
            }
        } else if method == "_tower/metadata" && p["sessionId"] == s.acp {
            let mut identity_learned = false;
            if let Some(id) = p.pointer("/metadata/native_session_id") {
                let id = id
                    .as_str()
                    .filter(|s| crate::identifier(s))
                    .ok_or_else(|| {
                        error("native_protocol", "Invalid native conversation identity")
                    })?;
                if !s.native.is_empty() && s.native != id {
                    return Err(error(
                        "native_identity_changed",
                        "Native conversation identity changed",
                    ));
                }
                identity_learned = s.native.is_empty();
                s.native = id.into();
            }
            if let Some(model) = p
                .pointer("/metadata/model")
                .and_then(Value::as_str)
                .filter(|s| s.len() <= 200)
            {
                s.metadata["model"] = json!(model);
            }
            if let Some(tokens) = p
                .pointer("/metadata/context_tokens")
                .and_then(Value::as_u64)
            {
                s.metadata["context_tokens"] = json!(tokens);
            }
            let (state, _, _) = s.journal.state(s.binding)?;
            if identity_learned {
                s.state(&state)?;
            } else {
                s.journal.update_metadata(s.binding, &state, &s.metadata)?;
            }
        } else if method == "session/update" && p["sessionId"] == s.acp {
            let update = &p["update"];
            if let Some(kind) = update["sessionUpdate"].as_str()
                && matches!(
                    kind,
                    "agent_message_chunk"
                        | "agent_thought_chunk"
                        | "tool_call"
                        | "tool_call_update"
                        | "plan"
                )
            {
                s.append(kind, update.clone(), s.active.clone(), false)?;
            }
        } else if method == "_tower/request_cancelled" {
            let mut requests = s.handle.requests.lock().map_err(internal)?;
            for r in requests.values_mut() {
                if serde_json::to_value(&r.wire.native_request_id).map_err(internal)? == p["id"]
                    && !r.consumed
                {
                    r.consumed = true;
                    r.wire.state = "cancelled".into();
                    r.wire.payload = tower_protocol::Value::Object(Default::default());
                    r.wire.request_revision += 1;
                }
            }
        }
        return Ok(());
    }
    let Some(id) = v["id"].as_str() else {
        return Err(error("native_protocol", "Unknown native response"));
    };
    let Some((command, kind)) = s.pending.remove(id) else {
        return Ok(());
    };
    if v.get("error").is_some() {
        s.done(
            &command,
            None,
            Some(failure(
                "native_rejected",
                "execution",
                kind == "prompt_end",
            )),
        )?;
        if matches!(kind.as_str(), "initialize" | "session") {
            return Err(error("native_initialize", "Native session setup failed"));
        }
        if s.active.as_deref() == Some(&command) {
            s.end_turn("error")?;
        }
        return Ok(());
    }
    let result = &v["result"];
    match kind.as_str() {
        "initialize" => {
            if codex {
                wire::json_line(input, &json!({"jsonrpc":"2.0","method":"initialized"})).await?;
                let mut params = json!({"cwd":s.binding.cwd});
                let method = if let Some(id) = &s.binding.resume_session_id {
                    params["threadId"] = json!(id);
                    "thread/resume"
                } else {
                    "thread/start"
                };
                s.rpc(input, method, params, &command, "session").await?;
            } else {
                if result["protocolVersion"] != 1 {
                    return Err(error(
                        "protocol_incompatible",
                        "ACP bridge requires version 1",
                    ));
                }
                let mut params = json!({"cwd":s.binding.cwd,"mcpServers":[]});
                let method = if let Some(id) = &s.binding.resume_session_id {
                    let previous = s
                        .journal
                        .bindings(Some(&s.binding.namespace()))?
                        .into_iter()
                        .rev()
                        .find(|b| {
                            b.runtime_id != s.binding.runtime_id
                                && b.session_id == s.binding.session_id
                                && s.journal
                                    .metadata(b)
                                    .is_ok_and(|m| m["acp_session_id"].as_str() == Some(id))
                        })
                        .ok_or_else(|| {
                            error("resume_unavailable", "Previous runtime binding unavailable")
                        })?;
                    let metadata = s.journal.metadata(&previous)?;
                    if metadata["acp_session_id"] != *id {
                        return Err(error(
                            "resume_unavailable",
                            "ACP resume identity does not match prior execution",
                        ));
                    }
                    params["sessionId"] = json!(id);
                    params["_meta"] =
                        json!({"tower.nativeSessionId":metadata["native_session_id"]});
                    s.acp = id.clone();
                    "session/load"
                } else {
                    "session/new"
                };
                s.rpc(input, method, params, &command, "session").await?;
            }
        }
        "session" => {
            if codex {
                if result["model"].is_string() {
                    s.metadata["model"] = result["model"].clone();
                    s.metadata["effort"] = result["reasoningEffort"].clone();
                }
                s.native = result
                    .pointer("/thread/id")
                    .and_then(Value::as_str)
                    .filter(|v| crate::identifier(v))
                    .ok_or_else(|| error("native_protocol", "Missing native thread"))?
                    .into();
            } else {
                if s.acp.is_empty() {
                    s.acp = result["sessionId"]
                        .as_str()
                        .filter(|v| crate::identifier(v))
                        .ok_or_else(|| error("native_protocol", "Missing ACP session"))?
                        .into();
                }
                if let Some(id) = result.pointer("/_meta/tower.nativeSessionId") {
                    s.native = id
                        .as_str()
                        .filter(|v| crate::identifier(v))
                        .ok_or_else(|| error("native_protocol", "Invalid native conversation"))?
                        .into();
                }
            }
            if codex
                && s.binding
                    .resume_session_id
                    .as_ref()
                    .is_some_and(|id| id != &s.native)
            {
                return Err(error(
                    "native_identity_changed",
                    "Native resume changed conversation identity",
                ));
            }
            s.state("idle")?;
            s.done(&command, None, None)?;
        }
        "prompt_start" => {
            s.turn = result
                .pointer("/turn/id")
                .and_then(Value::as_str)
                .filter(|v| crate::identifier(v))
                .map(str::to_owned);
            if s.turn.is_none() {
                return Err(error("native_protocol", "Missing native turn"));
            }
        }
        "prompt_end" => s.end_turn(if result["stopReason"] == "cancelled" {
            "cancelled"
        } else {
            "end_turn"
        })?,
        "cancel" => s.done(&command, None, None)?,
        _ => {}
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn binding() -> Binding {
        Binding {
            origin: "https://example.invalid".into(),
            machine_id: "machine".into(),
            session_id: "session".into(),
            runtime_id: "runtime".into(),
            generation: "generation".into(),
            tower_session_id: "tower-id".into(),
            owner_id: "owner".into(),
            protocol: "codex-app-server".into(),
            harness: "codex".into(),
            cwd: "/work".into(),
            resume_session_id: None,
        }
    }
    fn state<'a>(journal: &'a Journal, binding: &'a Binding, handle: &'a Handle) -> State<'a> {
        State {
            journal,
            binding,
            handle,
            native: "native".into(),
            acp: String::new(),
            metadata: json!({}),
            active: Some("prompt".into()),
            stop_command: None,
            streamed: Default::default(),
            turn: Some("turn".into()),
            pending: Default::default(),
            counter: 0,
            queue: Default::default(),
            dropped: 0,
            dropped_from: None,
            dropped_to: None,
            text: None,
        }
    }
    #[test]
    fn text_fragments_are_batched_without_changing_content_or_event_order() {
        let root = tempfile::tempdir().unwrap();
        let journal = Journal::open(&root.path().join("runtime.db"), Default::default()).unwrap();
        let binding = binding();
        journal
            .admit(
                &binding,
                &crate::command(&binding, "launch", json!({})).unwrap(),
            )
            .unwrap();
        let (handle, _, _) = channels();
        let mut state = state(&journal, &binding, &handle);
        for _ in 0..100 {
            state
                .append(
                    "agent_message_chunk",
                    json!({"content":{"type":"text","text":"Rust 🦀"}}),
                    Some("prompt".into()),
                    false,
                )
                .unwrap();
        }
        assert!(journal.batch(&binding).unwrap().is_none());
        state
            .append(
                "turn_end",
                json!({"stop_reason":"end_turn"}),
                Some("prompt".into()),
                true,
            )
            .unwrap();
        let events = journal.batch(&binding).unwrap().unwrap().events;
        assert_eq!(events.len(), 2);
        let payload = serde_json::to_value(&events[0].payload).unwrap();
        assert_eq!(payload["content"]["text"], "Rust 🦀".repeat(100));
        assert_eq!(events[1].kind, "turn_end");
    }
    #[test]
    fn full_bulk_storage_coalesces_loss_without_exhausting_control_reserve() {
        let root = tempfile::tempdir().unwrap();
        let journal = Journal::open(
            &root.path().join("runtime.db"),
            crate::Limits {
                runtime_spool_bytes: 8,
                host_spool_bytes: 8,
                ..Default::default()
            },
        )
        .unwrap();
        let binding = binding();
        journal
            .admit(
                &binding,
                &crate::command(&binding, "launch", json!({})).unwrap(),
            )
            .unwrap();
        let (handle, _, _) = channels();
        let mut state = state(&journal, &binding, &handle);
        for _ in 0..100 {
            state
                .append(
                    "agent_message_chunk",
                    json!({"content":{"type":"text","text":"output"}}),
                    Some("prompt".into()),
                    false,
                )
                .unwrap();
            state.flush_text().unwrap();
        }
        assert!(journal.batch(&binding).unwrap().is_none());
        state
            .append(
                "turn_end",
                json!({"stop_reason":"end_turn"}),
                Some("prompt".into()),
                true,
            )
            .unwrap();
        let events = journal.batch(&binding).unwrap().unwrap().events;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "gap");
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].kind, "turn_end");
    }
}
