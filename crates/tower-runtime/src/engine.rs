use crate::requests::{LiveRequest, Requests};
use crate::{
    Binding, Journal, Result, error, internal, millis, parse_time, random_id, timestamp, wire,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    sync::mpsc,
};
use tower_protocol::{ExecutionCommand, ExecutionError, ExecutionResult, RuntimeRequest};

pub(crate) enum Control {
    Command(ExecutionCommand),
    Stop(String),
    /// Leaves the native session resumable for the next server: the
    /// runtime stays live and its running turn continues after the restart.
    Suspend,
}
/// How an engine task begins.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    /// The first normal command launches or resumes the native session.
    Launch,
    /// Reopens the native session of a suspended or orphaned live runtime.
    Continue,
}
use tower_agents::runtime::CONTINUE;
/// What a continued engine restores from the previous server.
struct Continuation {
    active: Option<String>,
    turn: Option<String>,
    waiting: bool,
    /// Codex items already reported; absent after a crash.
    completed: Option<std::collections::HashSet<String>>,
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
            requests: Requests::default(),
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
/// What every runtime task needs from the server.
#[derive(Clone)]
pub(crate) struct Context {
    pub paths: tower_pal::Paths,
    pub journal: Arc<Journal>,
    pub authority: crate::runtime::Authority,
}

pub(crate) async fn run(
    ctx: Context,
    binding: Binding,
    handle: Arc<Handle>,
    mut normal: mpsc::Receiver<ExecutionCommand>,
    mut control: mpsc::Receiver<Control>,
    start: Start,
) {
    let result = run_inner(&ctx, &binding, &handle, &mut normal, &mut control, start).await;
    let (paths, journal) = (&ctx.paths, &ctx.journal);
    if let Err(e) = result {
        let known_before_spawn = journal
            .metadata(&binding)
            .is_ok_and(|v| v.get("pid").is_none())
            && matches!(
                e.data.code.as_str(),
                "denied"
                    | "authority_unavailable"
                    | "native_unavailable"
                    | "app_server_unavailable"
                    | "app_server_changed"
                    | "native_configuration"
                    | "thread_loaded"
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
    let _ = handle.requests.retire_all();
    if journal
        .state(&binding)
        .is_ok_and(|(_, fenced, termination)| fenced && termination == "confirmed")
    {
        cleanup_environment(paths, &binding);
    }
}

fn environment_path(paths: &tower_pal::Paths, binding: &Binding) -> Result<std::path::PathBuf> {
    use sha2::Digest;
    let name = sha2::Sha256::digest(
        serde_json::to_vec(&(
            binding.namespace(),
            &binding.runtime_id,
            &binding.generation,
        ))
        .map_err(internal)?,
    )
    .iter()
    .map(|value| format!("{value:02x}"))
    .collect::<String>();
    Ok(paths.state.join("runtime/environments").join(name))
}
pub(crate) fn cleanup_environment(paths: &tower_pal::Paths, binding: &Binding) {
    if binding.protocol == "codex-app-server"
        && let Ok(path) = environment_path(paths, binding)
    {
        let _ = tower_pal::remove_private_tree(&path);
    }
}

type NativeInput = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;
type NativeOutput = Box<dyn tokio::io::AsyncRead + Unpin + Send>;
struct Process {
    child: Option<tokio::process::Child>,
    identity: tower_pal::ProcessIdentity,
    reader: tokio::task::JoinHandle<()>,
    connection: Option<tokio::task::JoinHandle<()>>,
    codex: Option<tower_agents::codex_service::Config>,
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
        if self.child.is_some() {
            let _ = tower_pal::kill_protocol_group(self.identity);
        }
        if let Some(connection) = &self.connection {
            connection.abort();
        }
        self.reader.abort();
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ResponseKind {
    Initialize,
    Session,
    PromptStart,
    PromptEnd,
    Cancel,
}

struct State<'a> {
    journal: &'a Journal,
    binding: &'a Binding,
    handle: &'a Handle,
    native: String,
    /// The native session to load instead of creating one.
    open: Option<String>,
    resume_native: Option<String>,
    acp: String,
    metadata: Value,
    native_config: Value,
    active: Option<String>,
    stop_command: Option<String>,
    stop_reason: Option<String>,
    suspending: bool,
    continuation: Option<Continuation>,
    /// Starts a continue turn once the interrupted Codex turn completes.
    nudge: bool,
    /// Bytes of each Codex agent message already streamed.
    streamed: BTreeMap<String, usize>,
    /// Codex items of the active turn already reported.
    completed: std::collections::HashSet<String>,
    turn: Option<String>,
    pending: BTreeMap<String, (Option<String>, ResponseKind)>,
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
            !self.acp.is_empty()
                && !self
                    .pending
                    .values()
                    .any(|(_, kind)| *kind == ResponseKind::Session)
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
        input: &mut NativeInput,
        method: &str,
        params: Value,
        command: Option<&str>,
        kind: ResponseKind,
    ) -> Result<()> {
        self.counter += 1;
        let id = format!("tower-{}", self.counter);
        self.pending
            .insert(id.clone(), (command.map(str::to_owned), kind));
        wire::json_line(
            input,
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )
        .await
    }
    fn end_turn(&mut self, reason: &str) -> Result<()> {
        if let Some(command) = self.active.take() {
            self.handle.requests.retire_all()?;
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
            self.completed.clear();
            self.metadata["active_command"] = Value::Null;
            self.metadata["turn"] = Value::Null;
            self.state("idle")?;
        }
        Ok(())
    }
    /// Records the delivered turn so that the next server can continue it.
    fn persist_turn(&mut self) -> Result<()> {
        self.metadata["active_command"] = json!(self.active);
        self.metadata["turn"] = json!(self.turn);
        let (state, _, _) = self.journal.state(self.binding)?;
        self.journal
            .update_metadata(self.binding, &state, &self.metadata)
    }
    fn turn_params(&self, content: Vec<Value>) -> Result<Value> {
        let mut params = json!({"threadId":self.native,"input":content});
        if let Some(config) = self.metadata.get("config").and_then(Value::as_object) {
            for (k, v) in config {
                if k != "mode" {
                    params[k] = v.clone();
                }
            }
            if let Some(mode) = config.get("mode") {
                let model = config
                    .get("model")
                    .or_else(|| self.metadata.get("model"))
                    .filter(|v| v.is_string())
                    .ok_or_else(|| {
                        error(
                            "unsupported_config",
                            "Native model identity is required for mode changes",
                        )
                    })?;
                params["collaborationMode"] = json!({"mode":mode,"settings":{"model":model,"reasoning_effort":config.get("effort").or_else(||self.metadata.get("effort")),"developer_instructions":null}});
            }
        }
        Ok(params)
    }
    async fn continue_turn(&mut self, input: &mut NativeInput) -> Result<()> {
        let command = self.active.clone();
        let text = json!({"type":"text","text":CONTINUE});
        if self.binding.protocol == "codex-app-server" {
            self.turn = None;
            let mut text = text;
            text["text_elements"] = json!([]);
            let params = self.turn_params(vec![text])?;
            self.rpc(
                input,
                "turn/start",
                params,
                command.as_deref(),
                ResponseKind::PromptStart,
            )
            .await
        } else {
            self.rpc(
                input,
                "session/prompt",
                json!({"sessionId":self.acp,"prompt":[text]}),
                command.as_deref(),
                ResponseKind::PromptEnd,
            )
            .await?;
            self.persist_turn()
        }
    }
}

async fn run_inner(
    ctx: &Context,
    b: &Binding,
    handle: &Handle,
    normal: &mut mpsc::Receiver<ExecutionCommand>,
    control: &mut mpsc::Receiver<Control>,
    start: Start,
) -> Result<()> {
    let (paths, journal) = (&ctx.paths, &*ctx.journal);
    let launch = match start {
        Start::Launch => {
            let launch = normal
                .recv()
                .await
                .ok_or_else(|| error("runtime_lost", "Launch was not supplied"))?;
            ctx.authority.check(b, &launch).await?;
            Some(launch)
        }
        Start::Continue => None,
    };
    let previous = journal.metadata(b)?;
    let env = tower_agents::environment::project_environment(
        std::path::Path::new(&b.cwd),
        &tower_pal::Program::Cli.path().map_err(internal)?,
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
    let native_home = if b.protocol == "codex-app-server" {
        Some(
            tower_agents::codex_host::namespace(paths)
                .map_err(|_| error("invalid_config", "Invalid host Codex configuration"))?,
        )
    } else {
        tower_agents::Harness::parse(&b.harness)
            .and_then(|h| tower_agents::inventory::namespace(h, &env))
    };
    let recorded = |key: &str| {
        previous[key]
            .as_str()
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| error("resume_unavailable", "Native session identity unavailable"))
    };
    let (open, resume_native) = if launch.is_none() {
        if previous["native_home"].as_str() != native_home.as_deref() {
            return Err(error(
                "native_home_changed",
                "Native history location changed since the previous execution",
            ));
        }
        if b.protocol == "acp" {
            (
                Some(recorded("acp_session_id")?),
                Some(recorded("native_session_id")?),
            )
        } else {
            (Some(recorded("native_session_id")?), None)
        }
    } else if b.resume_session_id.is_some() {
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
        (
            b.resume_session_id.clone(),
            metadata["native_session_id"].as_str().map(str::to_owned),
        )
    } else {
        (None, None)
    };
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
    let prepared_codex = if b.protocol == "codex-app-server" {
        // A continued thread may still be loaded and running its turn.
        let unloaded = launch.as_ref().and(b.resume_session_id.as_deref());
        Some(prepare_codex(paths, b, &env, unloaded).await?)
    } else {
        None
    };
    if let Some(launch) = &launch {
        ctx.authority.check(b, launch).await?;
        if control.try_recv().is_ok() {
            return Err(error(
                "runtime_cancelled",
                "Launch was cancelled before native dispatch",
            ));
        }
        if !journal.begin_dispatch(b, &launch.command_id)? {
            return Err(error("runtime_lost", "Launch dispatch is already fenced"));
        }
    }
    let NativeSetup {
        mut input,
        output: stdout,
        child,
        identity,
        connection,
        codex,
        config: native_config,
    } = if let Some(prepared) = prepared_codex {
        prepared
    } else {
        let argv = vec![
            tower_pal::Program::Server
                .path()
                .map_err(internal)?
                .to_string_lossy()
                .into_owned(),
            "__acp".into(),
            b.harness.clone(),
        ];
        let mut child = tower_pal::spawn_protocol(&argv, &env, std::path::Path::new(&b.cwd))
            .map_err(|_| error("native_unavailable", "Cannot start native agent"))?;
        let identity = tower_pal::process_identity(
            child
                .id()
                .ok_or_else(|| error("native_unavailable", "Missing native process"))?,
        )
        .map_err(internal)?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| error("native_protocol", "Missing native stdin"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| error("native_protocol", "Missing native stdout"))?;
        NativeSetup {
            input: Box::new(input),
            output: Box::new(output),
            child: Some(child),
            identity,
            connection: None,
            codex: None,
            config: Value::Null,
        }
    };
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
        connection,
        codex,
    };
    let (state, _, _) = journal.state(b)?;
    let continuation = launch.is_none().then(|| Continuation {
        active: previous["active_command"].as_str().map(str::to_owned),
        turn: previous["turn"].as_str().map(str::to_owned),
        waiting: state.starts_with("waiting_"),
        completed: previous["completed"].as_array().map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        }),
    });
    let streamed = serde_json::from_value(previous["streamed"].clone()).unwrap_or_default();
    let mut metadata = if launch.is_some() {
        json!({})
    } else {
        let mut metadata = previous;
        if let Some(fields) = metadata.as_object_mut() {
            for key in ["completed", "streamed"] {
                fields.remove(key);
            }
        }
        metadata
    };
    for (key, value) in [
        ("pid", json!(identity.pid)),
        ("pid_start_time", json!(identity.start_time)),
        ("shared_process", json!(b.protocol == "codex-app-server")),
        ("boot_id", json!(tower_pal::boot_id().map_err(internal)?)),
        ("native_home", json!(native_home)),
    ] {
        metadata[key] = value;
    }
    let mut s = State {
        journal,
        binding: b,
        handle,
        native: if b.protocol == "acp" {
            resume_native.clone().unwrap_or_default()
        } else {
            String::new()
        },
        open,
        resume_native,
        acp: String::new(),
        metadata,
        native_config,
        active: None,
        stop_command: None,
        stop_reason: None,
        suspending: false,
        continuation,
        nudge: false,
        streamed,
        completed: Default::default(),
        turn: None,
        pending: BTreeMap::new(),
        counter: 0,
        queue: VecDeque::new(),
        dropped: 0,
        dropped_from: None,
        dropped_to: None,
        text: None,
    };
    journal.update_metadata(
        b,
        if launch.is_some() { "starting" } else { &state },
        &s.metadata,
    )?;
    let init = if b.protocol == "codex-app-server" {
        json!({"clientInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})
    } else {
        json!({"protocolVersion":1,"clientInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")},"clientCapabilities":{"elicitation":{"form":{},"url":{}}}})
    };
    s.rpc(
        &mut input,
        "initialize",
        init,
        launch.as_ref().map(|l| l.command_id.as_str()),
        ResponseKind::Initialize,
    )
    .await?;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let started = tokio::time::Instant::now();
    let result: Result<()> = async {
        loop {
            tokio::select! {biased;
                command=control.recv()=>{
                    match command {
                        Some(Control::Stop(reason)) => {
                            s.handle.requests.retire_all()?;
                            s.stop_reason = Some(reason);
                            return Ok(());
                        }
                        // Before the native session exists there is nothing
                        // to continue, so the restart stops it instead.
                        Some(Control::Suspend) if !s.ready() || s.continuation.is_some() => {
                            s.handle.requests.retire_all()?;
                            s.stop_reason = Some("server_restart".into());
                            return Ok(());
                        }
                        Some(Control::Suspend) => {
                            s.suspending = true;
                            return Ok(());
                        }
                        Some(Control::Command(c)) => dispatch(ctx, &mut s, &mut input, c).await?,
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
                    s.handle.requests.check_reply_timeout()?;
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
                        dispatch(ctx, &mut s, &mut input, c).await?;
                    }
                }
            }
        }
    }
    .await;
    s.handle.requests.retire_all()?;
    if s.suspending && result.is_ok() {
        return suspend(process, &mut s, &mut input, &mut incoming).await;
    }
    let terminated = stop_process(&mut process, &mut s, &mut input, &mut incoming).await;
    if let Some(reason) = s.stop_reason.take() {
        if reason == "server_restart" {
            // Lets the next server resume this session and continue the turn.
            s.metadata["server_restart"] = json!({"interrupted_turn": s.active.is_some()});
            let (state, _, _) = journal.state(b)?;
            journal.update_metadata(b, &state, &s.metadata)?;
        }
        if terminated && s.active.is_some() && !journal.state(b)?.1 {
            s.end_turn("cancelled")?;
        }
        journal.fence(b, &reason, if terminated { "confirmed" } else { "unknown" })?;
    }
    if let Err(e) = result {
        if let Some(command) = s.stop_command.take() {
            if terminated && !journal.state(b)?.1 {
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

struct NativeSetup {
    input: NativeInput,
    output: NativeOutput,
    child: Option<tokio::process::Child>,
    identity: tower_pal::ProcessIdentity,
    connection: Option<tokio::task::JoinHandle<()>>,
    codex: Option<tower_agents::codex_service::Config>,
    config: Value,
}
async fn prepare_codex(
    paths: &tower_pal::Paths,
    b: &Binding,
    env: &tower_pal::Environment,
    unloaded: Option<&str>,
) -> Result<NativeSetup> {
    let host = tower_agents::codex_host::ensure(paths)
        .await
        .map_err(|e| error("app_server_unavailable", &e.to_string()))?;
    let mut maintenance = tower_agents::codex_service::Client::connect(&host.config.endpoint)
        .await
        .map_err(|_| {
            error(
                "app_server_unavailable",
                "Cannot inspect host Codex app-server",
            )
        })?;
    if maintenance.process() != host.process {
        return Err(error(
            "app_server_changed",
            "Codex app-server changed during launch",
        ));
    }
    if let Some(native) = unloaded {
        maintenance
            .wait_unloaded(native, Duration::from_secs(5))
            .await
            .map_err(|_| {
                error(
                    "thread_loaded",
                    "Codex thread is still loaded; close its previous clients before resume",
                )
            })?;
    }
    let base = maintenance
        .request("config/read", json!({"cwd":b.cwd,"includeLayers":false}))
        .await
        .map_err(|_| error("native_configuration", "Cannot read Codex execution policy"))?;
    let root = environment_path(paths, b)?;
    let mut config = json!({"thread_unload_delay_secs":0});
    tower_agents::codex_environment::prepare(
        &root,
        &tower_pal::Program::Cli.path().map_err(internal)?,
        env,
        &base["config"],
        &mut config,
        &host.shell,
    )
    .map_err(|_| {
        error(
            "environment_unavailable",
            "Cannot prepare private Codex session environment",
        )
    })?;
    let (stream, connection, identity) = tower_agents::codex_host::channel(&host.config)
        .await
        .map_err(|_| error("app_server_unavailable", "Cannot connect Codex owner"))?;
    if identity != host.process {
        connection.abort();
        return Err(error(
            "app_server_changed",
            "Codex app-server changed during launch",
        ));
    }
    let Some(identity) = identity else {
        connection.abort();
        return Err(error(
            "app_server_unavailable",
            "Cannot verify Codex server process identity",
        ));
    };
    let (output, input) = tokio::io::split(stream);
    Ok(NativeSetup {
        input: Box::new(input),
        output: Box::new(output),
        child: None,
        identity,
        connection: Some(connection),
        codex: Some(host.config),
        config,
    })
}

/// Releases the native session for the next server without ending it. An
/// ACP agent process is stopped and later reloads its history; a Codex
/// thread keeps running in the shared app-server.
async fn suspend(
    mut process: Process,
    s: &mut State<'_>,
    input: &mut NativeInput,
    incoming: &mut mpsc::Receiver<Result<Value>>,
) -> Result<()> {
    s.flush_text()?;
    if process.codex.is_some() {
        let starting = |s: &State| {
            s.pending
                .values()
                .any(|(_, kind)| *kind == ResponseKind::PromptStart)
        };
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            while starting(s) {
                match incoming.recv().await {
                    Some(Ok(v)) if v.get("method").is_none() => native(s, input, v).await?,
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
            Ok::<_, tower_protocol::RpcError>(())
        })
        .await;
        if s.turn.is_none()
            && let Some(command) = s.active.take()
        {
            s.journal.finish_dispatch(
                s.binding,
                &command,
                true,
                None,
                Some(failure("dispatch_unknown", "execution", true)),
            )?;
        }
        if let Some(task) = process.connection.take() {
            task.abort();
            let _ = task.await;
        }
    } else if let Some(child) = &mut process.child {
        let _ = input.shutdown().await;
        let _ = tower_pal::kill_protocol_group(process.identity);
        let reaped = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .is_ok_and(|v| v.is_ok());
        if !(reaped && group_terminated(process.identity).await) {
            return Err(error(
                "stop_unconfirmed",
                "Native agent did not stop for the restart",
            ));
        }
        process.child = None;
    }
    // Commands written to the native session have taken effect; the
    // running prompt continues after the restart.
    for (_, receipt) in s.journal.unsettled(s.binding)? {
        if receipt.dispatch.state == "in_flight"
            && s.active.as_deref() != Some(receipt.command_id.as_str())
        {
            s.done(&receipt.command_id, None, None)?;
        }
    }
    s.metadata["completed"] = json!(s.completed);
    s.metadata["streamed"] = json!(s.streamed);
    s.persist_turn()
}
async fn stop_process(
    process: &mut Process,
    s: &mut State<'_>,
    input: &mut NativeInput,
    incoming: &mut mpsc::Receiver<Result<Value>>,
) -> bool {
    if let Some(child) = &mut process.child {
        let _ = input.shutdown().await;
        let _ = tower_pal::kill_protocol_group(process.identity);
        let reaped = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .is_ok_and(|v| v.is_ok());
        return reaped && group_terminated(process.identity).await;
    }
    let Some(config) = &process.codex else {
        return false;
    };
    if s.native.is_empty()
        && s.pending
            .values()
            .any(|(_, kind)| *kind == ResponseKind::Session)
    {
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            while s.native.is_empty()
                && s.pending
                    .values()
                    .any(|(_, kind)| *kind == ResponseKind::Session)
            {
                let v = incoming
                    .recv()
                    .await
                    .ok_or_else(|| error("native_protocol", "Native setup response lost"))??;
                if v.get("method").is_none() {
                    native(s, input, v).await?;
                }
            }
            Ok::<_, tower_protocol::RpcError>(())
        })
        .await;
        if !matches!(settled, Ok(Ok(()))) {
            return false;
        }
    }
    if s.native.is_empty() {
        if let Some(task) = process.connection.take() {
            task.abort();
            let _ = task.await;
        }
        return true;
    }
    let thread = s.native.clone();
    let turn = s.turn.clone();
    let endpoint = config.endpoint.clone();
    let identity = process.identity;
    let cleanup = async {
        let mut client = tower_agents::codex_service::Client::connect(&endpoint).await?;
        if client.process() != Some(identity) {
            return Err(tower_agents::codex_service::Error::Unconfirmed);
        }
        client.stop_thread(&thread, turn.as_deref()).await?;
        Ok::<_, tower_agents::codex_service::Error>(client)
    };
    tokio::pin!(cleanup);
    let mut live = true;
    let cleaned = loop {
        tokio::select! {
            result=&mut cleanup=>break result,
            message=incoming.recv(),if live=>match message {
                Some(Ok(v))=>{
                    if v.get("method").is_some() && v.get("id").is_some() {continue;}
                    let _=native(s,input,v).await;
                }
                _=>live=false,
            }
        }
    };
    if let Some(task) = process.connection.take() {
        task.abort();
        let _ = task.await;
    }
    let _ = input.shutdown().await;
    match cleaned {
        Ok(mut client) => client
            .wait_unloaded(&thread, Duration::from_secs(5))
            .await
            .is_ok(),
        Err(_) => false,
    }
}
async fn dispatch(
    ctx: &Context,
    s: &mut State<'_>,
    input: &mut NativeInput,
    c: ExecutionCommand,
) -> Result<()> {
    let checked = async {
        if parse_time(&c.expires_at)? <= millis() {
            return Err(error("command_expired", "Command expired"));
        }
        ctx.authority.check(s.binding, &c).await?;
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
                    let params = s.turn_params(content)?;
                    s.rpc(input, "turn/start", params, Some(&c.command_id), ResponseKind::PromptStart)
                        .await?;
                } else {
                    s.rpc(
                        input,
                        "session/prompt",
                        json!({"sessionId":s.acp,"prompt":blocks}),
                        Some(&c.command_id),
                        ResponseKind::PromptEnd,
                    )
                    .await?;
                    s.persist_turn()?;
                }
            }
            "cancel" => {
                s.handle.requests.retire_all()?;
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
                            Some(&c.command_id),
                            ResponseKind::Cancel,
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
                let (id, result) = s.handle.requests.take_reply(&p)?;
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
async fn native(s: &mut State<'_>, input: &mut NativeInput, v: Value) -> Result<()> {
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
            if codex && target != Some(s.native.as_str()) {
                return Ok(());
            }
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
            let request = LiveRequest::new(request, method, deny_decision)?;
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
            s.handle.requests.insert(request, after_sequence)?;
            return Ok(());
        }
        if codex {
            if p.get("threadId").and_then(Value::as_str) != Some(s.native.as_str()) {
                return Ok(());
            }
            match method {
                "thread/closed" => {
                    return Err(error("native_thread_closed", "Native thread was unloaded"));
                }
                "thread/status/changed"
                    if p.pointer("/status/type").and_then(Value::as_str) == Some("systemError") =>
                {
                    s.metadata["error"] = json!("native_system_error");
                    s.state("unknown")?;
                }
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
                        && std::mem::take(&mut s.nudge)
                    {
                        s.continue_turn(input).await?;
                    } else if s.active.is_some()
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
                            *s.streamed.entry(id.into()).or_default() += text.len();
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
                    if method == "item/completed" {
                        completed_item(s, item)?;
                    } else if is_tool(item) && s.active.is_some() {
                        s.append("tool_call", item.clone(), s.active.clone(), false)?;
                    }
                }
                "serverRequest/resolved" => {
                    s.handle.requests.retire_native(&p["requestId"], "stale")?;
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
            s.handle.requests.retire_native(&p["id"], "cancelled")?;
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
        if matches!(kind, ResponseKind::Initialize | ResponseKind::Session) {
            if let Some(command) = &command {
                s.done(
                    command,
                    None,
                    Some(failure("native_rejected", "execution", false)),
                )?;
            }
            return Err(error("native_initialize", "Native session setup failed"));
        }
        if let Some(command) = &command {
            s.done(
                command,
                None,
                Some(failure(
                    "native_rejected",
                    "execution",
                    kind == ResponseKind::PromptEnd,
                )),
            )?;
        }
        if s.active.is_some() && s.active == command {
            s.end_turn("error")?;
        }
        return Ok(());
    }
    let result = &v["result"];
    match kind {
        ResponseKind::Initialize => {
            if codex {
                wire::json_line(input, &json!({"jsonrpc":"2.0","method":"initialized"})).await?;
                let mut params = json!({"cwd":s.binding.cwd,"config":s.native_config});
                let method = if let Some(id) = &s.open {
                    params["threadId"] = json!(id);
                    "thread/resume"
                } else {
                    "thread/start"
                };
                s.rpc(
                    input,
                    method,
                    params,
                    command.as_deref(),
                    ResponseKind::Session,
                )
                .await?;
            } else {
                if result["protocolVersion"] != 1 {
                    return Err(error(
                        "protocol_incompatible",
                        "ACP bridge requires version 1",
                    ));
                }
                let mut params = json!({"cwd":s.binding.cwd,"mcpServers":[]});
                let method = if let Some(id) = &s.open {
                    params["sessionId"] = json!(id);
                    params["_meta"] = json!({"tower.nativeSessionId":s.resume_native});
                    s.acp = id.clone();
                    "session/load"
                } else {
                    "session/new"
                };
                s.rpc(
                    input,
                    method,
                    params,
                    command.as_deref(),
                    ResponseKind::Session,
                )
                .await?;
            }
        }
        ResponseKind::Session => {
            if codex {
                if result["model"].is_string() {
                    s.metadata["model"] = result["model"].clone();
                    s.metadata["effort"] = result["reasoningEffort"].clone();
                }
                s.metadata["native_ephemeral"] =
                    json!(result.pointer("/thread/ephemeral") == Some(&Value::Bool(true)));
                s.metadata["native_resumable"] =
                    json!(s.open.is_some() && s.metadata["native_ephemeral"] != true);
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
                    let id = id
                        .as_str()
                        .filter(|v| crate::identifier(v))
                        .ok_or_else(|| error("native_protocol", "Invalid native conversation"))?;
                    if !s.native.is_empty() && s.native != id {
                        return Err(error(
                            "native_identity_changed",
                            "Native resume changed conversation identity",
                        ));
                    }
                    s.native = id.into();
                }
            }
            if codex && s.open.as_ref().is_some_and(|id| id != &s.native) {
                return Err(error(
                    "native_identity_changed",
                    "Native resume changed conversation identity",
                ));
            }
            match (s.continuation.take(), command) {
                (Some(continuation), _) => restore(s, input, continuation, result).await?,
                (None, command) => {
                    s.state("idle")?;
                    if let Some(command) = command {
                        s.done(&command, None, None)?;
                    }
                }
            }
        }
        ResponseKind::PromptStart => {
            s.metadata["native_resumable"] = json!(s.metadata["native_ephemeral"] != true);
            s.journal
                .update_metadata(s.binding, "working", &s.metadata)?;
            s.turn = result
                .pointer("/turn/id")
                .and_then(Value::as_str)
                .filter(|v| crate::identifier(v))
                .map(str::to_owned);
            if s.turn.is_none() {
                return Err(error("native_protocol", "Missing native turn"));
            }
            s.persist_turn()?;
        }
        ResponseKind::PromptEnd => s.end_turn(if result["stopReason"] == "cancelled" {
            "cancelled"
        } else {
            "end_turn"
        })?,
        ResponseKind::Cancel => {
            if let Some(command) = command {
                s.done(&command, None, None)?;
            }
        }
    }
    Ok(())
}
fn is_tool(item: &Value) -> bool {
    matches!(
        item["type"].as_str(),
        Some("commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall")
    )
}
/// Reports a completed Codex item once, including the unstreamed rest of
/// an agent message.
fn completed_item(s: &mut State<'_>, item: &Value) -> Result<()> {
    let Some(id) = item["id"].as_str() else {
        return Ok(());
    };
    if s.active.is_none() || !s.completed.insert(id.into()) {
        return Ok(());
    }
    if item["type"] == "agentMessage" {
        let streamed = s.streamed.remove(id).unwrap_or(0);
        if let Some(rest) = item["text"]
            .as_str()
            .and_then(|text| text.get(streamed..))
            .filter(|rest| !rest.is_empty())
        {
            s.append(
                "agent_message_chunk",
                json!({"content":{"type":"text","text":rest},"item_id":id}),
                s.active.clone(),
                false,
            )?;
        }
    } else if is_tool(item) {
        s.append("tool_call_update", item.clone(), s.active.clone(), false)?;
    }
    Ok(())
}
/// Re-establishes the turn the previous server left running, reporting
/// output the native session produced in between.
async fn restore(
    s: &mut State<'_>,
    input: &mut NativeInput,
    c: Continuation,
    opened: &Value,
) -> Result<()> {
    for (_, receipt) in s.journal.unsettled(s.binding)? {
        if receipt.dispatch.state == "in_flight"
            && c.active.as_deref() != Some(receipt.command_id.as_str())
        {
            // Only a crash leaves these: delivery cannot be confirmed.
            s.journal.finish_dispatch(
                s.binding,
                &receipt.command_id,
                true,
                None,
                Some(failure("dispatch_unknown", "execution", true)),
            )?;
        }
    }
    s.queue.extend(s.journal.pending_commands(s.binding)?);
    let Some(command) = c.active else {
        return s.state("idle");
    };
    s.active = Some(command);
    if s.binding.protocol != "codex-app-server" {
        s.state("working")?;
        return s.continue_turn(input).await;
    }
    let turn = c.turn.as_deref().and_then(|id| {
        opened
            .pointer("/thread/turns")
            .and_then(Value::as_array)?
            .iter()
            .find(|t| t["id"] == id)
    });
    let Some(turn) = turn else {
        s.state("working")?;
        return s.continue_turn(input).await;
    };
    s.turn = c.turn;
    match c.completed {
        Some(completed) => {
            s.completed = completed;
            for item in turn["items"].as_array().into_iter().flatten() {
                completed_item(s, item)?;
            }
        }
        None => s.dropped(),
    }
    match turn["status"].as_str() {
        Some("inProgress") => {
            s.state("working")?;
            if c.waiting {
                // Its approval request went to the previous connection.
                s.nudge = true;
                let params = json!({"threadId":s.native,"turnId":s.turn});
                s.rpc(input, "turn/interrupt", params, None, ResponseKind::Cancel)
                    .await?;
            }
            Ok(())
        }
        Some("completed") => s.end_turn("end_turn"),
        Some("interrupted") => s.end_turn("cancelled"),
        _ => s.end_turn("error"),
    }
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
            open: None,
            resume_native: None,
            acp: String::new(),
            metadata: json!({}),
            native_config: Value::Null,
            active: Some("prompt".into()),
            stop_command: None,
            stop_reason: None,
            suspending: false,
            continuation: None,
            nudge: false,
            streamed: Default::default(),
            completed: Default::default(),
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
