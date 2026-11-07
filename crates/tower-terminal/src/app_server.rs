use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tower_agents::{
    Harness,
    codex_service::{self, Client, Config, ThreadStatus},
    config::Invocation,
    events::{self, AgentEvent},
    runtime::LauncherPlan,
};

pub struct Submission {
    pub thread: String,
    pub text: String,
    pub result:
        tokio::sync::oneshot::Sender<tower_sessions::Result<tower_protocol::DeliveryReceipt>>,
}
#[derive(Default)]
struct State {
    selected: Option<(u64, String)>,
    pending: BTreeSet<(u64, String)>,
    selecting: bool,
    uncertain: bool,
    options: BTreeMap<String, Value>,
}
struct Shared {
    state: Mutex<State>,
    paused: AtomicBool,
    gate: RwLock<()>,
    next_snapshot: AtomicU64,
    process: Option<tower_pal::ProcessIdentity>,
    config: Config,
    shell: PathBuf,
    root: PathBuf,
    cwd: PathBuf,
    env: tower_pal::Environment,
    generation: String,
    executable: PathBuf,
}
pub struct Server {
    pub process: Option<tower_pal::ProcessIdentity>,
    relay: tokio::task::JoinHandle<()>,
    shared: Arc<Shared>,
    pub messages: mpsc::Sender<Submission>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.relay.abort();
    }
}
impl Server {
    pub fn available(&self) -> bool {
        !self.shared.paused.load(Ordering::Acquire) && !self.relay.is_finished()
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.shared.paused.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(30), async {
            // Stop admission before waiting for replies; the reader must keep running.
            let gate = self.shared.gate.write().await;
            drop(gate);
            let settled = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    {
                        let state = self.shared.state.lock().unwrap();
                        if state.uncertain {
                            return Err(io::Error::other("Codex submission outcome is unknown"));
                        }
                        if state.pending.is_empty() {
                            return Ok(state.selected.as_ref().map(|(_, id)| id.clone()));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .map_err(|_| io::Error::other("Codex requests have not settled"))??;
            let Some(thread) = settled else {
                self.relay.abort();
                let _ = (&mut self.relay).await;
                return Ok(());
            };
            let mut control = self.shared.control().await?;
            control
                .stop_thread(&thread, None)
                .await
                .map_err(io::Error::other)?;
            self.relay.abort();
            let _ = (&mut self.relay).await;
            control
                .wait_unloaded(&thread, Duration::from_secs(5))
                .await
                .map_err(io::Error::other)
        })
        .await
        .map_err(|_| io::Error::other("Codex cleanup timed out"))?
    }
}
impl Shared {
    async fn control(&self) -> io::Result<Client> {
        let client = Client::connect(&self.config.endpoint)
            .await
            .map_err(io::Error::other)?;
        if !same_process(self.process, client.process()) {
            return Err(io::Error::other(
                "Codex service identity changed or is unavailable",
            ));
        }
        Ok(client)
    }
    fn lost(&self, connection: u64) {
        let mut state = self.state.lock().unwrap();
        if state.pending.iter().any(|(id, _)| *id == connection) {
            state.uncertain = true;
            self.paused.store(true, Ordering::Release);
        }
        if state
            .selected
            .as_ref()
            .is_some_and(|(id, _)| *id == connection)
        {
            self.paused.store(true, Ordering::Release);
        }
    }
}
fn same_process(
    a: Option<tower_pal::ProcessIdentity>,
    b: Option<tower_pal::ProcessIdentity>,
) -> bool {
    matches!((a,b),(Some(a),Some(b)) if a.pid==b.pid && a.start_time==b.start_time)
}
pub async fn start(
    plan: &LauncherPlan,
    invocation: &mut Invocation,
    events: mpsc::Sender<AgentEvent>,
    config: Config,
) -> io::Result<Server> {
    let host = tower_agents::codex_host::ensure_configured(&tower_pal::Paths::discover()?, config)
        .await
        .map_err(io::Error::other)?;
    if host.process.is_none() {
        return Err(io::Error::other("Cannot verify Codex service identity"));
    }
    let relay_path = Path::new(&plan.control_socket).with_extension("relay.sock");
    let listener = tower_pal::IpcListener::bind(&relay_path).await?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State::default()),
        paused: AtomicBool::new(false),
        gate: RwLock::new(()),
        next_snapshot: AtomicU64::new(0),
        process: host.process,
        config: host.config,
        shell: host.shell,
        root: Path::new(&plan.state_dir).join("codex-environments"),
        cwd: PathBuf::from(&plan.spec.cwd),
        env: invocation.env.clone(),
        generation: plan.generation.clone(),
        executable: tower_pal::Program::Cli.path()?,
    });
    let (messages, mut submissions) = mpsc::channel::<Submission>(16);
    let relay_shared = shared.clone();
    let relay = tokio::spawn(async move {
        let mut workers = tokio::task::JoinSet::new();
        let mut connections = BTreeMap::new();
        let mut next_connection = 0u64;
        loop {
            tokio::select! {
                result = workers.join_next(), if !workers.is_empty() => {
                    match result {
                        Some(Ok(id))=>{connections.remove(&id);}
                        Some(Err(_))=>{relay_shared.state.lock().unwrap().uncertain=true;relay_shared.paused.store(true,Ordering::Release);}
                        None=>{},
                    }
                }
                accepted = listener.accept() => {
                    let Ok((stream,_))=accepted else {break};
                    if relay_shared.paused.load(Ordering::Acquire) || connections.len()>=8 {continue;}
                    let id=next_connection;next_connection=next_connection.saturating_add(1);
                    let (tx,rx)=mpsc::channel(16);connections.insert(id,tx);
                    let shared=relay_shared.clone();let events=events.clone();
                    workers.spawn(async move {
                        let _=bridge(id,stream,shared.clone(),events,rx).await;
                        shared.lost(id);id
                    });
                }
                Some(submission)=submissions.recv()=> {
                    let selected=relay_shared.state.lock().unwrap().selected.clone();
                    let target=selected.filter(|(_,thread)|thread==&submission.thread).and_then(|(id,_)|connections.get(&id));
                    if relay_shared.paused.load(Ordering::Acquire) {
                        let _=submission.result.send(Err(delivery_unavailable()));
                    } else if let Some(target)=target {
                        if let Err(error)=target.try_send(submission) {let _=error.into_inner().result.send(Err(delivery_unavailable()));}
                    } else {let _=submission.result.send(Err(delivery_unavailable()));}
                }
            }
        }
    });
    invocation.argv.splice(
        1..1,
        [
            "--remote".into(),
            format!("unix://{}", relay_path.display()),
        ],
    );
    Ok(Server {
        process: shared.process,
        relay,
        shared,
        messages,
    })
}
fn delivery_unavailable() -> tower_protocol::RpcError {
    tower_protocol::RpcError::application(
        "delivery_failed",
        "Target thread is unavailable or stopping",
    )
}
fn rpc_key(id: &Value) -> io::Result<String> {
    if id.is_null()
        || id.as_i64().is_some()
        || id.as_u64().is_some()
        || id.as_str().is_some_and(|id| {
            !id.is_empty() && id.len() <= 160 && !id.chars().any(char::is_control)
        })
    {
        Ok(id.to_string())
    } else {
        Err(io::Error::other("Invalid or oversized Codex request ID"))
    }
}

fn native_thread(value: &Value) -> Option<&str> {
    value["params"]["threadId"].as_str().or_else(|| {
        (value["method"] == "thread/started")
            .then(|| value["params"]["thread"]["id"].as_str())
            .flatten()
    })
}
fn native_visible(value: &Value, selected: Option<&str>) -> bool {
    if let Some(thread) = native_thread(value) {
        return Some(thread) == selected;
    }
    matches!(
        value["method"].as_str(),
        Some(
            "account/updated"
                | "account/gatewayOAuth/changed"
                | "account/rateLimits/updated"
                | "account/login/completed"
                | "skills/changed"
                | "app/list/updated"
                | "warning"
                | "guardianWarning"
                | "configWarning"
                | "deprecationNotice"
                | "windows/worldWritableWarning"
        )
    ) && value["id"].is_null()
}

fn lifecycle(value: &Value) -> bool {
    matches!(
        value["method"].as_str(),
        Some("thread/start" | "thread/resume" | "thread/fork")
    ) && !background(&value["params"])
        && !(value["method"] == "thread/fork" && value["params"]["ephemeral"] == true)
}
fn read_only(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "thread/read"
            | "thread/list"
            | "thread/loaded/list"
            | "thread/goal/get"
            | "thread/backgroundTerminals/list"
            | "config/read"
            | "configRequirements/read"
            | "model/list"
            | "skills/list"
            | "app/list"
            | "mcpServerStatus/list"
            | "account/read"
            | "account/rateLimits/read"
            | "environment/info"
            | "experimentalFeature/list"
    )
}
fn mutation_allowed(value: &Value, selected: Option<&str>) -> bool {
    let Some(method) = value["method"].as_str() else {
        return true;
    };
    if matches!(method, "command/exec" | "process/spawn") {
        return false;
    }
    if lifecycle(value) || read_only(method) {
        return true;
    }
    // Config writes are host-wide, while thread controls belong to this relay only.
    if method.starts_with("config/") {
        return false;
    }
    method == "initialized"
        || value["params"]["threadId"]
            .as_str()
            .is_some_and(|id| Some(id) == selected)
}
async fn reject(
    client: &mut codex_service::Socket,
    id: &Value,
    message: &'static str,
) -> io::Result<()> {
    if id.is_null() {
        return Ok(());
    }
    send(
        client,
        &json!({"id":id,"error":{"code":-32600,"message":message}}),
    )
    .await
}
async fn send(socket: &mut codex_service::Socket, value: &Value) -> io::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(serde_json::to_string(value)?.into())),
    )
    .await
    .map_err(|_| io::Error::other("Codex relay write timed out"))?
    .map_err(io::Error::other)
}
async fn prepare_thread(shared: &Shared, value: &mut Value) -> io::Result<Value> {
    let original = value["params"].clone();
    let target = value["params"]["threadId"].as_str().map(str::to_owned);
    let (selected, known) = {
        let state = shared.state.lock().unwrap();
        (
            state.selected.as_ref().map(|(_, id)| id.clone()),
            target
                .as_ref()
                .and_then(|id| state.options.get(id))
                .cloned(),
        )
    };
    let mut control = shared.control().await?;
    if let Some(previous) = selected
        .as_ref()
        .filter(|id| value["method"] != "thread/resume" || Some(*id) != target.as_ref())
        && control
            .thread_status(previous)
            .await
            .map_err(io::Error::other)?
            != ThreadStatus::NotLoaded
    {
        return Err(io::Error::other(
            "Close the current Codex conversation before selecting another",
        ));
    }
    let existing = if let Some(id) = &target {
        let result = control
            .request("thread/read", json!({"threadId":id,"includeTurns":false}))
            .await
            .map_err(io::Error::other)?;
        if result["thread"]["status"]["type"] != "notLoaded" && value["method"] == "thread/resume" {
            let Some(options) = known.filter(|_| selected.as_ref() == target.as_ref()) else {
                return Err(io::Error::other(
                    "Codex thread is already loaded by another owner",
                ));
            };
            if !same_options(&original, &options) {
                return Err(io::Error::other(
                    "Unload the Codex thread before changing launch options",
                ));
            }
            return Ok(options);
        }
        Some(result)
    } else {
        None
    };
    let cwd = value["params"]["cwd"]
        .as_str()
        .or_else(|| existing.as_ref().and_then(|v| v["thread"]["cwd"].as_str()))
        .map(PathBuf::from)
        .unwrap_or_else(|| shared.cwd.clone());
    let cwd = std::fs::canonicalize(&cwd)?;
    let mut env = tower_agents::environment::project_environment(
        &cwd,
        &shared.executable,
        shared.env.clone(),
    )
    .await?;
    for (key, value) in &shared.env {
        if key.starts_with("TOWER_") {
            env.insert(key.clone(), value.clone());
        }
    }
    tower_clipboard::shim::install(
        shared
            .root
            .parent()
            .ok_or_else(|| io::Error::other("Invalid session path"))?,
        &shared.executable,
        &mut env,
        &shared.generation,
    )?;
    let base = control
        .request("config/read", json!({"cwd":cwd,"includeLayers":false}))
        .await
        .map_err(io::Error::other)?;
    let snapshot = shared.next_snapshot.fetch_add(1, Ordering::Relaxed);
    if snapshot >= 256 {
        return Err(io::Error::other("Codex environment snapshot limit reached"));
    }
    if !value["params"].is_object() {
        value["params"] = json!({});
    }
    if value["params"]["config"].is_null() {
        value["params"]["config"] = json!({});
    }
    tower_agents::codex_environment::prepare(
        &shared.root.join(snapshot.to_string()),
        &shared.executable,
        &env,
        &base["config"],
        &mut value["params"]["config"],
        &shared.shell,
    )?;
    value["params"]["config"]["thread_unload_delay_secs"] = json!(0);
    value["params"]["cwd"] = json!(cwd);
    Ok(original)
}
fn same_options(request: &Value, original: &Value) -> bool {
    request.as_object().is_some_and(|params| {
        params.iter().all(|(key, value)| {
            key == "threadId"
                || value.is_null()
                || (key == "config" && value.as_object().is_some_and(|v| v.is_empty()))
                || original.get(key) == Some(value)
        })
    })
}
async fn bridge(
    connection: u64,
    stream: tower_pal::IpcStream,
    shared: Arc<Shared>,
    events: mpsc::Sender<AgentEvent>,
    mut submissions: mpsc::Receiver<Submission>,
) -> io::Result<()> {
    let limits = WebSocketConfig::default()
        .max_message_size(Some(16 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
        .max_write_buffer_size(17 * 1024 * 1024);
    let mut client = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::accept_async_with_config(stream, Some(limits)),
    )
    .await
    .map_err(|_| io::Error::other("Relay handshake timed out"))?
    .map_err(io::Error::other)?;
    let upstream = codex_service::connect(&shared.config.endpoint)
        .await
        .map_err(io::Error::other)?;
    if !same_process(shared.process, upstream.process) {
        return Err(io::Error::other("Codex service identity changed"));
    }
    let mut upstream = upstream.socket;
    let mut monitor = Monitor::default();
    let mut messages = BTreeMap::new();
    let mut selections = BTreeMap::new();
    let mut requests = BTreeMap::new();
    let mut native_requests = BTreeSet::new();
    let mut next_message = 0u64;
    let mut client_open = true;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _=tick.tick()=>{},
            Some(submission)=submissions.recv()=> {
                let _gate=shared.gate.read().await;
                if shared.paused.load(Ordering::Acquire) || monitor.thread.as_deref()!=Some(&submission.thread) || messages.len()>=16 {
                    let _=submission.result.send(Err(delivery_unavailable()));continue;
                }
                let id=format!("tower:message:{next_message}");next_message=next_message.saturating_add(1);
                let request=json!({"id":id,"method":"turn/start","params":{"threadId":submission.thread,"input":[{"type":"text","text":submission.text,"text_elements":[]}]}});
                shared.state.lock().unwrap().pending.insert((connection,json!(id).to_string()));
                messages.insert(id,submission.result);
                monitor.observe(&request,false);
                send(&mut upstream,&request).await?;
            }
            frame=client.next(),if client_open=> {
                let frame=match frame {Some(Ok(frame))=>frame,_=>{client_open=false;continue}};
                match frame {
                    Message::Text(text)=> {
                        let mut value:Value=serde_json::from_str(&text)?;
                        let id=value["id"].clone();let key=rpc_key(&id)?;
                        if id.as_str().is_some_and(|id|id.starts_with("tower:message:")) {return Err(io::Error::other("Reserved relay request ID"));}
                        let method=value["method"].as_str().map(str::to_owned);
                        if method.is_none() && !native_requests.remove(&key) {continue;}
                        let _gate=shared.gate.read().await;
                        if shared.paused.load(Ordering::Acquire) {
                            if method.is_some() {reject(&mut client,&id,"Tower is stopping this Codex session").await?;}
                            continue;
                        }
                        let selected=shared.state.lock().unwrap().selected.as_ref().map(|(_,id)|id.clone());
                        let changes_cwd = value["params"]["cwd"].as_str().is_some_and(|cwd| {
                            let state=shared.state.lock().unwrap();
                            selected.as_ref().and_then(|id|state.options.get(id)).and_then(|options|options["cwd"].as_str()).is_some_and(|old|cwd!=old)
                        }) && !lifecycle(&value) && !method.as_deref().is_some_and(read_only);
                        if changes_cwd {
                            reject(&mut client,&id,"Restart the conversation to change its approved project environment").await?;continue;
                        }
                        if !mutation_allowed(&value,selected.as_deref()) {
                            reject(&mut client,&id,"This operation is outside the managed conversation; use a normal terminal for standalone shell commands").await?;continue;
                        }
                        let is_selection=lifecycle(&value);
                        if is_selection {
                            let busy={let state=shared.state.lock().unwrap();state.selecting || state.uncertain};
                            if busy {reject(&mut client,&id,"A Codex conversation change is still pending").await?;continue;}
                            match tokio::time::timeout(Duration::from_secs(20),prepare_thread(&shared,&mut value)).await {
                                Ok(Ok(options))=>{selections.insert(key.clone(),options);}
                                _=>{reject(&mut client,&id,"Cannot apply Codex conversation configuration; the thread may still be loaded or its environment is unavailable").await?;continue;}
                            }
                        }
                        if let Some(event)=monitor.observe(&value,false) {publish(&events,event).await?;}
                        if let Some(method)=method && !id.is_null() {
                            if requests.len()>=128 || requests.contains_key(&key) {return Err(io::Error::other("Too many or duplicate relay requests"));}
                            let tracked=!read_only(&method);
                            requests.insert(key.clone(),tracked);
                            if tracked {let mut state=shared.state.lock().unwrap();state.pending.insert((connection,key));state.selecting|=is_selection;}
                        }
                        send(&mut upstream,&value).await?;
                    }
                    Message::Close(_)=>client_open=false,
                    Message::Ping(_)=>client.flush().await.map_err(io::Error::other)?,
                    _=>{},
                }
            }
            frame=upstream.next()=> {
                let Some(frame)=frame else {return Ok(())};
                let value:Value=match frame.map_err(io::Error::other)? {
                    Message::Text(text)=>serde_json::from_str(&text)?,
                    Message::Ping(_)=>{upstream.flush().await.map_err(io::Error::other)?;continue;}
                    Message::Close(_)=>return Ok(()), _=>continue,
                };
                let key=rpc_key(&value["id"])?;
                if value.get("method").is_none() {
                    if let Some(reply)=value["id"].as_str().and_then(|id|messages.remove(id)) {
                        shared.state.lock().unwrap().pending.remove(&(connection,key));
                        let result=if value.get("error").is_some() {Err(tower_protocol::RpcError::application("delivery_failed","Codex rejected the turn"))}
                            else if value.pointer("/result/turn/id").and_then(Value::as_str).is_some() {Ok(tower_protocol::DeliveryReceipt{method:"native".into(),acknowledgement:"harness".into()})}
                            else {Err(tower_protocol::RpcError::application("delivery_unknown","Unexpected Codex turn response; do not automatically retry"))};
                        if let Some(event)=monitor.observe(&value,true) {publish(&events,event).await?;}
                        let _=reply.send(result);continue;
                    }
                    requests.remove(&key);
                    let selected_options=selections.remove(&key);
                    let event=monitor.observe(&value,true);
                    {let mut state=shared.state.lock().unwrap();
                        if let Some(options)=selected_options {
                            state.selecting=false;
                            if let Some(event)=&event && let Some(id)=&event.native_id {
                                state.selected=Some((connection,id.clone()));
                                let mut options=options;
                                for field in ["model","modelProvider","approvalPolicy","approvalsReviewer","sandbox","reasoningEffort"] {
                                    if !value["result"][field].is_null() {options[field]=value["result"][field].clone();}
                                }
                                if !value["result"]["thread"]["cwd"].is_null() {options["cwd"]=value["result"]["thread"]["cwd"].clone();}
                                state.options.clear();
                                state.options.insert(id.clone(),options);
                            } else if value.get("error").is_none() {state.uncertain=true;shared.paused.store(true,Ordering::Release);}
                        }
                        state.pending.remove(&(connection,key));
                    }
                    if let Some(event)=event {publish(&events,event).await?;}
                } else {
                    let selected=shared.state.lock().unwrap().selected.as_ref().map(|(_,id)|id.clone());
                    let native_thread=native_thread(&value);
                    if !native_visible(&value,selected.as_deref()) {continue;}
                    if value["method"]=="thread/settings/updated" && let Some(thread)=native_thread {
                        let mut state=shared.state.lock().unwrap();
                        if let Some(options)=state.options.get_mut(thread)
                            && let Some(settings)=value["params"]["threadSettings"].as_object() {
                            for (key,value) in settings {options[key]=value.clone();}
                        }
                    }
                    if !value["id"].is_null() {
                        if native_thread.is_none() || native_requests.len()>=128 {continue;}
                        native_requests.insert(key);
                    }
                    if let Some(event)=monitor.observe(&value,true) {publish(&events,event).await?;}
                }
                if client_open && send(&mut client,&value).await.is_err() {client_open=false;}
            }
        }
        if !client_open {
            let state = shared.state.lock().unwrap();
            if state
                .selected
                .as_ref()
                .is_none_or(|(id, _)| *id != connection)
                && !state.pending.iter().any(|(id, _)| *id == connection)
            {
                return Ok(());
            }
        }
    }
}
async fn publish(events: &mpsc::Sender<AgentEvent>, event: AgentEvent) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(2), events.send(event))
        .await
        .map_err(|_| io::Error::other("Codex observation queue is full"))?
        .map_err(io::Error::other)
}
#[derive(Default)]
struct Monitor {
    pending: BTreeMap<String, String>,
    approvals: BTreeSet<String>,
    turns: BTreeSet<String>,
    thread: Option<String>,
    ephemeral: bool,
}
impl Monitor {
    fn observe(&mut self, value: &Value, server: bool) -> Option<AgentEvent> {
        let method = value["method"].as_str();
        let key = value.get("id").map(Value::to_string);
        if !server {
            if let (Some(method), Some(key)) = (method, key.as_ref()) {
                if matches!(method, "thread/start" | "thread/resume" | "thread/fork")
                    && self.pending.len() < 64
                    && !background(&value["params"])
                    && !(method == "thread/fork"
                        && value["params"]["ephemeral"] == true
                        && value["params"]["threadId"].as_str() == self.thread.as_deref())
                {
                    self.pending.insert(key.clone(), method.into());
                } else if method == "turn/start"
                    && value["params"]["threadId"].as_str() == self.thread.as_deref()
                    && self.turns.len() < 64
                {
                    self.turns.insert(key.clone());
                }
            } else if key.as_ref().is_some_and(|key| self.approvals.remove(key))
                && self.approvals.is_empty()
            {
                return self.event("PermissionResult", json!({}));
            }
            return None;
        }
        if method.is_none() {
            let key = key?;
            if self.turns.remove(&key) {
                if value["result"]["turn"]["id"].is_string() && !self.ephemeral {
                    let mut event = self.event("metadata", json!({}))?;
                    event.resumable = Some(true);
                    return Some(event);
                }
                return None;
            }
            let method = self.pending.remove(&key)?;
            let thread = &value["result"]["thread"];
            let id = thread["id"].as_str().filter(|s| events::native_id(s))?;
            if background(thread) {
                return None;
            }
            self.thread = Some(id.into());
            self.ephemeral = thread["ephemeral"] == true;
            let mut payload = json!({"model":value["result"]["model"],"session_title":thread["name"],"cwd":thread["cwd"]});
            if let Some(effort) = value["result"].get("reasoningEffort") {
                payload["effort"] = effort.clone();
            }
            let mut event = self.event("SessionStart", payload)?;
            event.authoritative_status = status(&thread["status"]);
            event.resumable = Some(
                !self.ephemeral
                    && method != "thread/start"
                    && thread["path"].as_str().is_some_and(|path| !path.is_empty()),
            );
            return Some(event);
        }
        let p = &value["params"];
        if self.thread.is_none() || p["threadId"].as_str() != self.thread.as_deref() {
            return None;
        }
        match method? {
            "turn/started" => self.event("UserPromptSubmit", json!({})),
            "turn/completed" => self.event(
                if p["turn"]["status"] == "failed" {
                    "StopFailure"
                } else {
                    "Stop"
                },
                json!({}),
            ),
            "thread/status/changed" => {
                let mut e = self.event("metadata", json!({}))?;
                e.authoritative_status = status(&p["status"]);
                Some(e)
            }
            "thread/name/updated" => {
                self.event("metadata", json!({"session_title":p["threadName"]}))
            }
            "thread/settings/updated" => {
                let mut payload = json!({"model":p["threadSettings"]["model"]});
                if let Some(effort) = p["threadSettings"].get("effort") {
                    payload["effort"] = effort.clone();
                }
                self.event("metadata", payload)
            }
            "thread/tokenUsage/updated" => self.event(
                "metadata",
                json!({"context_tokens":p["tokenUsage"]["last"]["totalTokens"]}),
            ),
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                if let Some(key) = key {
                    self.approvals.insert(key);
                }
                self.event("PermissionRequest", json!({}))
            }
            "item/tool/requestUserInput" | "mcpServer/elicitation/request" => {
                if let Some(key) = key {
                    self.approvals.insert(key);
                }
                let mut e = self.event("PreToolUse", json!({"tool_name":"AskUserQuestion"}))?;
                e.authoritative_status = Some("waiting_input".into());
                Some(e)
            }
            "item/started" | "item/completed" => {
                let kind = p["item"]["type"].as_str()?;
                if matches!(kind, "agentMessage" | "userMessage" | "reasoning") {
                    return None;
                }
                self.event(
                    if method == Some("item/started") {
                        "PreToolUse"
                    } else {
                        "PostToolUse"
                    },
                    json!({"tool_name":kind,"tool_call_id":p["item"]["id"]}),
                )
            }
            _ => None,
        }
    }
    fn event(&self, name: &str, mut payload: Value) -> Option<AgentEvent> {
        payload["session_id"] = json!(self.thread);
        events::parse(Harness::Codex, name, &serde_json::to_vec(&payload).ok()?).ok()
    }
}
fn background(value: &Value) -> bool {
    value["threadSource"]
        .as_str()
        .is_some_and(|source| source != "user")
}
fn status(value: &Value) -> Option<String> {
    let status = match value["type"].as_str()? {
        "idle" => "idle",
        "active" => {
            if value["activeFlags"]
                .as_array()
                .is_some_and(|flags| flags.iter().any(|v| v == "waitingOnApproval"))
            {
                "waiting_approval"
            } else if value["activeFlags"]
                .as_array()
                .is_some_and(|flags| flags.iter().any(|v| v == "waitingOnUserInput"))
            {
                "waiting_input"
            } else {
                "working"
            }
        }
        "systemError" => "unknown",
        _ => return None,
    };
    Some(status.into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_broadcasts_cannot_select_or_describe_another_thread() {
        let foreign = json!({"method":"thread/started","params":{"thread":{"id":"foreign"}}});
        assert!(!native_visible(&foreign, Some("owned")));
        assert!(!native_visible(&foreign, None));
        assert!(native_visible(&foreign, Some("foreign")));
        assert!(!native_visible(
            &json!({"method":"thread/unknown","params":{}}),
            Some("owned")
        ));
        assert!(!native_visible(
            &json!({"method":"mcpServer/event/stream/notification","params":{}}),
            Some("owned")
        ));
        assert!(native_visible(
            &json!({"method":"configWarning","params":{"message":"fixture"}}),
            None
        ));
    }
    #[test]
    fn native_controls_stay_bound_to_the_selected_thread() {
        assert!(mutation_allowed(
            &json!({"method":"thread/read","params":{"threadId":"other"}}),
            Some("owned")
        ));
        assert!(mutation_allowed(
            &json!({"method":"turn/interrupt","params":{"threadId":"owned"}}),
            Some("owned")
        ));
        for value in [
            json!({"method":"turn/start","params":{"threadId":"other"}}),
            json!({"method":"command/exec","params":{"threadId":"owned"}}),
            json!({"method":"process/spawn"}),
            json!({"method":"config/value/write"}),
            json!({"method":"unknown/mutation"}),
        ] {
            assert!(!mutation_allowed(&value, Some("owned")));
        }
    }
    #[test]
    fn reattaching_cannot_change_a_loaded_threads_options() {
        let original = json!({"model":"original","cwd":"/work","config":{"x":1}});
        assert!(same_options(
            &json!({"threadId":"thread","cwd":"/work","model":null,"config":{}}),
            &original
        ));
        assert!(!same_options(
            &json!({"threadId":"thread","model":"changed"}),
            &original
        ));
        assert!(!same_options(
            &json!({"threadId":"thread","config":{"x":2}}),
            &original
        ));
    }
    #[test]
    fn resumability_requires_successful_native_history_materialization() {
        let mut monitor = Monitor::default();
        monitor.observe(&json!({"id":1,"method":"thread/start"}), false);
        let first=monitor.observe(&json!({"id":1,"result":{"thread":{"id":"root","path":"/planned/rollout","status":{"type":"idle"}}}}),true).unwrap();
        assert_eq!(first.resumable, Some(false));
        monitor.observe(
            &json!({"id":2,"method":"turn/start","params":{"threadId":"root"}}),
            false,
        );
        assert!(
            monitor
                .observe(&json!({"id":2,"error":{"code":-1}}), true)
                .is_none()
        );
        monitor.observe(
            &json!({"id":3,"method":"turn/start","params":{"threadId":"root"}}),
            false,
        );
        assert_eq!(
            monitor
                .observe(&json!({"id":3,"result":{"turn":{"id":"turn"}}}), true)
                .unwrap()
                .resumable,
            Some(true)
        );
        monitor.observe(
            &json!({"id":4,"method":"thread/resume","params":{"threadId":"root"}}),
            false,
        );
        assert_eq!(
            monitor
                .observe(
                    &json!({"id":4,"result":{"thread":{"id":"root","path":"/durable/rollout"}}}),
                    true
                )
                .unwrap()
                .resumable,
            Some(true)
        );
        monitor.observe(&json!({"id":5,"method":"thread/start"}), false);
        monitor.observe(
            &json!({"id":5,"result":{"thread":{"id":"ephemeral","ephemeral":true}}}),
            true,
        );
        monitor.observe(
            &json!({"id":6,"method":"turn/start","params":{"threadId":"ephemeral"}}),
            false,
        );
        assert!(
            monitor
                .observe(&json!({"id":6,"result":{"turn":{"id":"turn"}}}), true)
                .is_none()
        );
    }
    #[test]
    fn reasoning_effort_is_thread_scoped_and_explicit_null_clears_it() {
        let mut monitor = Monitor::default();
        monitor.observe(&json!({"id":1,"method":"thread/resume"}), false);
        let selected = monitor
            .observe(
                &json!({"id":1,"result":{"reasoningEffort":"high","thread":{"id":"root"}}}),
                true,
            )
            .unwrap();
        assert_eq!(selected.effort.as_deref(), Some("high"));
        assert!(monitor.observe(&json!({"method":"thread/settings/updated","params":{"threadId":"other","threadSettings":{"effort":"low"}}}),true).is_none());
        let update = monitor.observe(&json!({"method":"thread/settings/updated","params":{"threadId":"root","threadSettings":{"model":"new"}}}),true).unwrap();
        assert!(!update.clear_effort);
        let update = monitor.observe(&json!({"method":"thread/settings/updated","params":{"threadId":"root","threadSettings":{"effort":null}}}),true).unwrap();
        assert!(update.clear_effort);
        assert!(update.effort.is_none());
    }
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let mut random = [0u8; 16];
            getrandom::fill(&mut random).unwrap();
            let suffix = random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let path = std::env::temp_dir().join(format!("tower-relay-{suffix}"));
            tower_pal::private_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct Fake {
        _root: Directory,
        config: Config,
        task: tokio::task::JoinHandle<()>,
        calls: Arc<Mutex<Vec<Value>>>,
    }
    impl Drop for Fake {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn fake(answer: impl Fn(&Value) -> Value + Send + Sync + 'static) -> Fake {
        let root = Directory::new();
        let endpoint = root.0.join("native.sock");
        let listener = tower_pal::IpcListener::bind(&endpoint).await.unwrap();
        let answer = Arc::new(answer);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let answer = answer.clone();
                let recorded = recorded.clone();
                tasks.spawn(async move {
                    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    while let Some(Ok(Message::Text(text))) = socket.next().await {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        recorded.lock().unwrap().push(value.clone());
                        if value["method"] == "initialized" || value["method"].is_null() {
                            continue;
                        }
                        let mut response = if value["method"] == "initialize" {
                            json!({"result":{"userAgent":"codex-app-server/0.166.0"}})
                        } else {
                            answer(&value)
                        };
                        let notices = response.as_object_mut().unwrap().remove("_notices");
                        response["id"] = value["id"].clone();
                        if send(&mut socket, &response).await.is_err() {
                            break;
                        }
                        if let Some(Value::Array(notices)) = notices {
                            for notice in notices {
                                if send(&mut socket, &notice).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        Fake {
            _root: root,
            config: Config {
                endpoint,
                managed: false,
            },
            task,
            calls,
        }
    }
    fn shared(config: Config) -> Arc<Shared> {
        Arc::new(Shared {
            state: Mutex::new(State {
                selected: Some((0, "owned".into())),
                options: BTreeMap::from([("owned".into(), json!({"cwd":"/work"}))]),
                ..Default::default()
            }),
            paused: AtomicBool::new(false),
            gate: RwLock::new(()),
            next_snapshot: AtomicU64::new(0),
            process: Some(tower_pal::process_identity(tower_pal::pid()).unwrap()),
            config,
            shell: PathBuf::from("/bin/bash"),
            root: PathBuf::from("/unused"),
            cwd: PathBuf::from("/work"),
            env: Default::default(),
            generation: "fixture".into(),
            executable: PathBuf::from("/unused/tower"),
        })
    }
    #[tokio::test]
    async fn uncertain_submission_preserves_the_supervisor_and_closes_admission() {
        let shared = shared(Config {
            endpoint: PathBuf::from("/unused"),
            managed: false,
        });
        shared.state.lock().unwrap().uncertain = true;
        let (messages, _) = mpsc::channel(1);
        let mut server = Server {
            process: shared.process,
            relay: tokio::spawn(std::future::pending()),
            shared: shared.clone(),
            messages,
        };
        assert!(server.shutdown().await.is_err());
        assert!(shared.paused.load(Ordering::Acquire));
        assert!(!server.relay.is_finished());
    }
    #[tokio::test]
    async fn cleanup_closes_owner_connections_before_confirming_unload() {
        let closed = Arc::new(AtomicBool::new(false));
        let native_closed = closed.clone();
        let fixture=fake(move|request|match request["method"].as_str().unwrap(){
            "thread/read"=>json!({"result":{"thread":{"id":"owned","status":{"type":if native_closed.load(Ordering::Acquire){"notLoaded"}else{"idle"}}}}}),
            "thread/goal/get"=>json!({"result":{"goal":null}}),
            "thread/turns/list"|"thread/backgroundTerminals/list"=>json!({"result":{"data":[],"nextCursor":null}}),
            _=>panic!("unexpected cleanup method"),
        }).await;
        struct Closed(Arc<AtomicBool>);
        impl Drop for Closed {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let guard = Closed(closed.clone());
        let relay = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        let shared = shared(fixture.config.clone());
        let (messages, _) = mpsc::channel(1);
        let mut server = Server {
            process: shared.process,
            relay,
            shared,
            messages,
        };
        server.shutdown().await.unwrap();
        assert!(closed.load(Ordering::Acquire));
        assert!(
            fixture
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|request| request["params"]["threadId"].is_null()
                    || request["params"]["threadId"] == "owned")
        );
    }
    #[tokio::test]
    async fn relay_never_forwards_or_answers_another_threads_approval() {
        let fixture=fake(|request|match request["method"].as_str().unwrap(){
            "thread/read"=>json!({"result":{"thread":{"status":{"type":"idle"}}}}),
            "thread/resume"=>json!({"result":{"thread":{"id":"owned","path":"/durable/history","status":{"type":"idle"}}},"_notices":[
                {"id":"foreign-approval","method":"item/commandExecution/requestApproval","params":{"threadId":"foreign"}},
                {"id":"owned-approval","method":"item/commandExecution/requestApproval","params":{"threadId":"owned"}}
            ]}),
            _=>panic!("unexpected fixture method"),
        }).await;
        let root = Directory::new();
        let path = root.0.join("relay.sock");
        let listener = tower_pal::IpcListener::bind(&path).await.unwrap();
        let shared = shared(fixture.config.clone());
        let (observations, mut observed) = mpsc::channel(16);
        let (_messages, submissions) = mpsc::channel(4);
        let worker = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            bridge(0, stream, shared, observations, submissions).await
        });
        let mut client = codex_service::connect(&path).await.unwrap().socket;
        send(
            &mut client,
            &json!({"id":1,"method":"initialize","params":{}}),
        )
        .await
        .unwrap();
        client.next().await.unwrap().unwrap();
        send(&mut client, &json!({"method":"initialized"}))
            .await
            .unwrap();
        send(
            &mut client,
            &json!({"id":2,"method":"thread/resume","params":{"threadId":"owned"}}),
        )
        .await
        .unwrap();
        let response = client.next().await.unwrap().unwrap();
        assert!(matches!(response, Message::Text(_)));
        let Message::Text(notice) = client.next().await.unwrap().unwrap() else {
            panic!("missing approval")
        };
        let notice: Value = serde_json::from_str(&notice).unwrap();
        assert_eq!(notice["id"], "owned-approval");
        send(
            &mut client,
            &json!({"id":"foreign-approval","result":{"decision":"accept"}}),
        )
        .await
        .unwrap();
        send(
            &mut client,
            &json!({"id":"owned-approval","result":{"decision":"decline"}}),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observed.recv().await.unwrap().kind == events::EventKind::ApprovalResolved {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            !fixture
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|value| value["id"] == "foreign-approval")
        );
        worker.abort();
        let _ = worker.await;
    }
    #[test]
    fn only_successful_foreground_thread_selection_can_change_identity() {
        let mut m = Monitor::default();
        assert!(m.observe(&json!({"method":"thread/status/changed","params":{"threadId":"child","status":{"type":"idle"}}}),true).is_none());
        m.observe(&json!({"id":1,"method":"thread/start"}), false);
        let e=m.observe(&json!({"id":1,"result":{"thread":{"id":"root","cwd":"/work","status":{"type":"idle"}}}}),true).unwrap();
        assert_eq!(e.native_id.as_deref(), Some("root"));
        m.observe(&json!({"id":2,"method":"thread/start","params":{"threadSource":"system","ephemeral":true}}),false);
        assert!(m.observe(&json!({"id":2,"result":{"thread":{"id":"recap","threadSource":"system","ephemeral":true}}}),true).is_none());
        assert_eq!(m.thread.as_deref(), Some("root"));
        assert!(
            m.observe(
                &json!({"method":"turn/started","params":{"threadId":"child"}}),
                true
            )
            .is_none()
        );
        let e=m.observe(&json!({"id":"a","method":"item/commandExecution/requestApproval","params":{"threadId":"root"}}),true).unwrap();
        assert_eq!(e.kind, events::EventKind::Approval);
        assert_eq!(
            m.observe(&json!({"id":"a","result":{}}), false)
                .unwrap()
                .kind,
            events::EventKind::ApprovalResolved
        );
    }
}
