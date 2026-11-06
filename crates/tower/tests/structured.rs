#[path = "support/structured.rs"]
mod fixture;
#[path = "support/remote_terminal.rs"]
mod remote_terminal;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
struct Fixture {
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-acp-")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::create_dir(root.path().join("bin")).unwrap();
        let node = tower_pal::find_program("node", &tower_pal::environment())
            .expect("Node is required for protocol fixtures");
        let script =
            fixture::NATIVE.replacen("#!/usr/bin/env node", &format!("#!{}", node.display()), 1);
        for name in ["codex", "claude", "agy"] {
            let path = root.path().join("bin").join(name);
            let source = if name == "codex" {
                include_str!("fixtures/codex-host.mjs").replacen(
                    "#!/usr/bin/env node",
                    &format!("#!{}", node.display()),
                    1,
                )
            } else {
                script.clone()
            };
            std::fs::write(&path, source).unwrap();
            tower_pal::make_executable(&path).unwrap();
        }
        Self { root }
    }
    fn command(&self, args: &[&str]) -> Command {
        self.program(env!("CARGO_BIN_EXE_tower-cli"), args)
    }
    fn program(&self, binary: impl AsRef<std::ffi::OsStr>, args: &[&str]) -> Command {
        let mut c = Command::new(binary);
        c.args(args)
            .env_clear()
            .env("HOME", self.root.path())
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.root.path().join("bin").display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("XDG_STATE_HOME", self.root.path().join("state"))
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("TOKIO_WORKER_THREADS", "2")
            .current_dir(self.root.path());
        c
    }
    fn rpc(&self, method: &str, p: Value) -> Value {
        let o = self
            .command(&["--json", "rpc", method, &p.to_string()])
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "{} {}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        serde_json::from_slice(&o.stdout).unwrap()
    }
    fn cli(&self, args: &[&str]) {
        let o = self.command(args).output().unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for (binary, args) in [
            ("codex", &["app-server", "daemon", "stop"][..]),
            (env!("CARGO_BIN_EXE_tower-cli"), &["daemon", "stop"][..]),
            (env!("CARGO_BIN_EXE_tower-terminal-pool"), &["stop"][..]),
        ] {
            let _ = self
                .program(binary, args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
struct Call {
    method: &'static str,
    params: Value,
    reply: oneshot::Sender<Value>,
}
struct Server {
    sender: mpsc::Sender<Call>,
    frames: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn call(&self, method: &'static str, params: Value) -> Value {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Call {
                method,
                params,
                reply: tx,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .unwrap()
            .unwrap()
    }
    async fn until(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(v) = self
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .rev()
                    .find(|v| predicate(v))
                    .cloned()
                {
                    return v;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn start(f: &Fixture) -> Self {
        Self::start_with_retirement(f, false).await
    }
    async fn start_with_retirement(f: &Fixture, retire: bool) -> Self {
        Self::start_modes(f, retire, false).await
    }
    async fn start_modes(f: &Fixture, retire: bool, terminal: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = f.root.path().join("config/tower");
        std::fs::create_dir_all(&config).unwrap();
        let scopes = if terminal {
            "['read','terminal:control']"
        } else {
            "['read','send','acp:control']"
        };
        std::fs::write(config.join("config.toml"),format!("[allgather]\nurl='ws://127.0.0.1:{port}/tower'\nmanaged=true\nscopes={scopes}\n[scan]\nenabled=false\n")).unwrap();
        let (sender, mut receive) = mpsc::channel::<Call>(32);
        let frames = Arc::new(Mutex::new(Vec::new()));
        let observed = frames.clone();
        let task = tokio::spawn(async move {
            let mut sequence = 0;
            let mut cursors: BTreeMap<String, (u64, Option<String>)> = BTreeMap::new();
            let mut retained: BTreeMap<(String, u64), Value> = BTreeMap::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let hello: Value =
                    serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data())
                        .unwrap();
                assert_eq!(
                    hello["params"]["harness_controls"]["codex"]["protocol"],
                    "codex-app-server"
                );
                let caps = if terminal {
                    json!([
                        "policy-sync.v1",
                        "session-status.v1",
                        "terminal-launch.v1",
                        "command-receipts.v1",
                        "session-stream.v1"
                    ])
                } else {
                    json!([
                        "policy-sync.v1",
                        "session-status.v1",
                        "managed-acp.v1",
                        "command-receipts.v1",
                        "session-stream.v1",
                        "session-resume.v1"
                    ])
                };
                socket.send(Message::Text(json!({"jsonrpc":"2.0","id":"hello","result":{"node_name":"fixture-machine","protocol_version":{"major":1,"minor":4},"capabilities":caps}}).to_string().into())).await.unwrap();
                socket.send(Message::Text(json!({"jsonrpc":"2.0","method":"policy.push","params":{"policy_id":"tower","revision":1,"rules":[]}}).to_string().into())).await.unwrap();
                let mut pending = BTreeMap::new();
                let mut ready_owners = BTreeMap::new();
                loop {
                    tokio::select! {
                     call=receive.recv()=>{let Some(call)=call else{return};sequence+=1;let id=format!("call-{sequence}");pending.insert(id.clone(),call.reply);if socket.send(Message::Text(json!({"jsonrpc":"2.0","id":id,"method":call.method,"params":call.params}).to_string().into())).await.is_err(){break;}}
                     message=socket.next()=>{
                      let Some(Ok(message))=message else{break};let Message::Text(text)=message else{if matches!(message,Message::Close(_)){break;}continue};let v:Value=serde_json::from_str(&text).unwrap();
                      observed.lock().unwrap().push(v.clone());
                      if let Some(id)=v["id"].as_str()&&v.get("method").is_none(){if let Some(reply)=pending.remove(id){let _=reply.send(v);}continue;}
                      let p=&v["params"];let runtime=p["runtime_id"].as_str().unwrap_or("").to_owned();
                      let result=match v["method"].as_str(){
                       Some("state.push")=>{socket.send(Message::Text(json!({"jsonrpc":"2.0","method":"sync.ack","params":{"cursor":p["seq"],"rejected":[]}}).to_string().into())).await.unwrap();continue;}
                       Some("command.receipt")=>json!({"command_id":p["command_id"],"receipt_revision":p["receipt_revision"]}),
                       Some("runtime.lost")=>{let mut result=p.clone();result["fenced"]=json!(true);result},
                       Some("session.request")=>{
                        if p["state"]=="pending" {
                         assert_eq!(ready_owners.get(&runtime),Some(&p["owner_id"]),"Private request precedes owner readiness ACK");
                         let ((_,marker_seq),_)=retained.iter().find(|((r,_),event)|r==&runtime&&event["payload"]["source_request_id"]==p["source_request_id"]).expect("Private request precedes its redacted event ACK");
                         assert!(retained.iter().any(|((r,seq),event)|r==&runtime&&seq>marker_seq&&event["kind"]=="runtime_state"&&event["payload"]["state"]=="waiting"&&event["payload"]["owner_id"]==p["owner_id"]),"Private request precedes active runtime state ACK");
                        }
                        json!({"source_request_id":p["source_request_id"],"request_revision":p["request_revision"],"state":if retire{json!("answered")}else{p["state"].clone()}})
                       },
                       Some("session.cursor")=>{let(c,key)=cursors.entry(runtime).or_default();json!({"runtime_id":p["runtime_id"],"generation":p["generation"],"source_key":p["source_key"],"cursor":c,"last_event_key":key,"coverage":"complete"})},
                       Some("session.events")=>{let(c,key)=cursors.entry(runtime.clone()).or_default();for e in p["events"].as_array().unwrap(){let seq=e["sequence"].as_u64().unwrap();if seq<=*c{assert_eq!(retained.get(&(runtime.clone(),seq)),Some(e));}else{assert_eq!(seq,*c+1);retained.insert((runtime.clone(),seq),e.clone());*c=seq;*key=Some(e["key"].as_str().unwrap().into());}
                        if e["kind"]=="runtime_state"&&matches!(e["payload"]["state"].as_str(),Some("idle"|"running"|"waiting")){ready_owners.insert(runtime.clone(),e["payload"]["owner_id"].clone());}}json!({"runtime_id":p["runtime_id"],"generation":p["generation"],"source_key":p["source_key"],"cursor":c})},
                       _=>continue,
                      };
                      if let Some(id)=v.get("id")&& socket.send(Message::Text(json!({"jsonrpc":"2.0","id":id,"result":result}).to_string().into())).await.is_err(){break;}
                     }
                    }
                }
            }
        });
        f.rpc("rules.replace",json!({"rules":[{"id":"allow","from":"*","to":"*","action":"allow","created_by":"fixture","created_at":1},{"id":"sessions","from":"*","to":"*","action":"allow","kind":"session","created_by":"fixture","created_at":1}]}));
        f.rpc(
            "allgather.login",
            json!({"key":"fixture-key","url":format!("ws://127.0.0.1:{port}/tower")}),
        );
        let server = Self {
            sender,
            frames,
            task,
        };
        server.until(|v| v["method"] == "policy.ack").await;
        server
    }
}
fn command(id: &str, runtime: &str, kind: &str, payload: Value) -> Value {
    json!({"command_id":id,"session_id":format!("logical-{runtime}"),"runtime_id":runtime,"generation":format!("generation-{runtime}"),"kind":kind,"payload":payload,"expires_at":"2099-01-01T00:00:00Z"})
}
async fn complete(s: &Server, id: &str) {
    s.until(|v| {
        v["method"] == "command.receipt"
            && v["params"]["command_id"] == id
            && v["params"]["dispatch"]["state"] == "finished"
    })
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_adapters_preserve_receipts_permissions_and_resume_after_restart() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    for (harness, protocol) in [
        ("codex", "codex-app-server"),
        ("claude-code", "acp"),
        ("antigravity", "acp"),
    ] {
        let runtime = harness;
        let launch = command(
            &format!("launch-{harness}"),
            runtime,
            "launch",
            json!({"harness":harness,"protocol":protocol,"generation":format!("generation-{runtime}"),"cwd":f.root.path()}),
        );
        let accepted = s.call("command.execute", launch.clone()).await;
        assert_eq!(
            accepted["result"]["acceptance"]["level"], "adapter",
            "{accepted}"
        );
        complete(&s, &format!("launch-{harness}")).await;
        let ready = s
            .until(|v| {
                v["method"] == "session.events"
                    && v["params"]["runtime_id"] == runtime
                    && v["params"]["events"].as_array().is_some_and(|a| {
                        a.iter().any(|e| {
                            e["kind"] == "runtime_state" && e["payload"]["state"] == "idle"
                        })
                    })
            })
            .await;
        let id = ready["params"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["kind"] == "runtime_state")
            .unwrap()["payload"]["tower_session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let prompt = command(
            &format!("prompt-{harness}"),
            runtime,
            "prompt",
            json!({"content":[{"type":"text","text":match harness {"antigravity"=>"hello","codex"=>"approve-backlog",_=>"approve"}}]}),
        );
        if harness == "antigravity" {
            let record = f.rpc("agent.get", json!({"id":id}));
            assert!(record["native_session_id"].is_null());
            for event in ready["params"]["events"].as_array().unwrap() {
                if event["kind"] == "runtime_state" {
                    assert!(event["payload"]["acp_session_id"].is_string());
                    assert!(event["payload"].get("native_session_id").is_none());
                }
            }
        }
        assert_eq!(
            s.call("command.execute", prompt.clone()).await["result"]["state"],
            "accepted"
        );
        if harness != "antigravity" {
            let request = s
                .until(|v| {
                    v["method"] == "session.request"
                        && v["params"]["runtime_id"] == runtime
                        && v["params"]["state"] == "pending"
                })
                .await;
            let p = &request["params"];
            if harness == "codex" {
                assert!(
                    s.frames
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|v| {
                            v["method"] == "session.events"
                                && v["params"]["runtime_id"] == runtime
                                && v["params"]["events"].as_array().is_some_and(|events| {
                                    events.iter().any(|e| e["kind"] == "tool_call")
                                })
                        })
                        .count()
                        >= 3
                );
            }
            let reply = command(
                &format!("reply-{harness}"),
                runtime,
                "reply",
                json!({"request_id":"visible-request","source_request_id":p["source_request_id"],"request_revision":p["request_revision"],"generation":p["generation"],"response":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}),
            );
            assert_eq!(
                s.call("command.execute", reply).await["result"]["state"],
                "accepted"
            );
        }
        complete(&s, &format!("prompt-{harness}")).await;
        let duplicate = s.call("command.execute", prompt).await;
        assert_eq!(duplicate["result"]["dispatch"]["state"], "finished");
        let before = f.rpc("agent.get", json!({"id":id}));
        assert!(
            before["native_session_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
        assert_eq!(before["cwd"], "~");
        // A server restart stops the server-owned process; Allgather resumes.
        f.cli(&["daemon", "stop"]);
        f.cli(&["daemon", "status"]);
        s.until(|v| {
            v["method"] == "runtime.lost"
                && v["params"]["runtime_id"] == runtime
                && v["params"]["termination"] == "confirmed"
                && v["params"]["reason"] == "server_restart"
        })
        .await;
        let resumed_runtime = format!("resumed-{runtime}");
        let native = ready["params"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["kind"] == "runtime_state")
            .unwrap()["payload"][if protocol == "acp" {
            "acp_session_id"
        } else {
            "native_session_id"
        }]
        .clone();
        let mut resume = command(
            &format!("resume-{harness}"),
            &resumed_runtime,
            "resume",
            json!({"harness":harness,"protocol":protocol,"generation":format!("generation-{resumed_runtime}"),"cwd":f.root.path(),"resume_session_id":native}),
        );
        resume["session_id"] = launch["session_id"].clone();
        let accepted = s.call("command.execute", resume).await;
        assert_eq!(accepted["result"]["state"], "accepted", "{accepted}");
        complete(&s, &format!("resume-{harness}")).await;
        let resumed = s
            .until(|v| {
                v["method"] == "session.events"
                    && v["params"]["runtime_id"] == resumed_runtime
                    && v["params"]["events"]
                        .as_array()
                        .is_some_and(|a| a.iter().any(|e| e["payload"]["state"] == "idle"))
            })
            .await;
        assert!(
            resumed["params"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["payload"]["tower_session_id"] == id)
        );
        let stale = command(
            &format!("stale-{harness}"),
            runtime,
            "prompt",
            json!({"content":[{"type":"text","text":"must not run"}]}),
        );
        assert_eq!(
            s.call("command.execute", stale).await["error"]["data"]["code"],
            "runtime_fenced"
        );
        let mut prompt = command(
            &format!("resumed-prompt-{harness}"),
            &resumed_runtime,
            "prompt",
            json!({"content":[{"type":"text","text":"resumed"}]}),
        );
        prompt["session_id"] = launch["session_id"].clone();
        assert_eq!(
            s.call("command.execute", prompt).await["result"]["state"],
            "accepted"
        );
        complete(&s, &format!("resumed-prompt-{harness}")).await;
        f.cli(&["kill", &id]);
    }
    let dispatches = std::fs::read_to_string(f.root.path().join("dispatches")).unwrap();
    assert_eq!(
        dispatches.lines().count(),
        6,
        "Duplicates must not reach native input"
    );
    for file in std::fs::read_dir(f.root.path().join("state/tower/runtime")).unwrap() {
        let path = file.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(path).unwrap();
            assert!(
                !bytes
                    .windows(b"PRIVATE-REQUEST-MARKER".len())
                    .any(|w| w == b"PRIVATE-REQUEST-MARKER")
            );
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_uses_the_independent_control_path_and_preserves_native_limits() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    for (harness, protocol) in [
        ("codex", "codex-app-server"),
        ("claude-code", "acp"),
        ("antigravity", "acp"),
    ] {
        let launch = command(
            &format!("launch-{harness}"),
            harness,
            "launch",
            json!({"harness":harness,"protocol":protocol,"generation":format!("generation-{harness}"),"cwd":f.root.path()}),
        );
        assert!(
            s.call("command.execute", launch)
                .await
                .get("error")
                .is_none()
        );
        complete(&s, &format!("launch-{harness}")).await;
        let prompt = command(
            &format!("waiting-{harness}"),
            harness,
            "prompt",
            json!({"content":[{"type":"text","text":"wait"}]}),
        );
        assert!(
            s.call("command.execute", prompt)
                .await
                .get("error")
                .is_none()
        );
        s.until(|v| {
            v["method"] == "session.events"
                && v["params"]["runtime_id"] == harness
                && v["params"]["events"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|e| e["kind"] == "user_message"))
        })
        .await;
        let cancel = command(&format!("cancel-{harness}"), harness, "cancel", json!({}));
        assert_eq!(
            s.call("command.execute", cancel).await["result"]["state"],
            "accepted"
        );
        complete(&s, &format!("cancel-{harness}")).await;
        complete(&s, &format!("waiting-{harness}")).await;
        let id = f
            .rpc("agent.list", json!({"filter":{"include_exited":true}}))
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["harness"] == harness)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        if harness == "antigravity" {
            s.until(|v| {
                v["method"] == "runtime.lost"
                    && v["params"]["runtime_id"] == harness
                    && v["params"]["termination"] == "confirmed"
            })
            .await;
        } else {
            let next = command(
                &format!("after-cancel-{harness}"),
                harness,
                "prompt",
                json!({"content":[{"type":"text","text":"after cancellation"}]}),
            );
            assert_eq!(
                s.call("command.execute", next).await["result"]["state"],
                "accepted"
            );
            complete(&s, &format!("after-cancel-{harness}")).await;
            f.cli(&["kill", &id]);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_crash_fences_uncertain_dispatch_without_replaying_the_prompt() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    let launch = command(
        "launch",
        "crash",
        "launch",
        json!({"harness":"codex","protocol":"codex-app-server","generation":"generation-crash","cwd":f.root.path()}),
    );
    assert!(
        s.call("command.execute", launch)
            .await
            .get("error")
            .is_none()
    );
    complete(&s, "launch").await;
    let prompt = command(
        "prompt",
        "crash",
        "prompt",
        json!({"content":[{"type":"text","text":"wait"}]}),
    );
    assert_eq!(
        s.call("command.execute", prompt.clone()).await["result"]["state"],
        "accepted"
    );
    s.until(|v| {
        v["method"] == "session.events"
            && v["params"]["events"]
                .as_array()
                .is_some_and(|a| a.iter().any(|e| e["kind"] == "user_message"))
    })
    .await;
    let terminal = f.rpc(
        "session.open",
        json!({"harness":"codex","protocol":"terminal","cwd":f.root.path(),"headless":true}),
    );
    let terminal_id = terminal["id"].as_str().unwrap();
    let terminal_pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = f.rpc("agent.get", json!({"id":terminal_id}));
            if record["status"] == "idle" {
                break record["pid"].clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let socket = tower_pal::connect(&f.root.path().join("state/tower/run/towerd.sock"))
        .await
        .unwrap();
    let server =
        tower_pal::process_identity(tower_pal::peer(&socket).unwrap().pid.unwrap()).unwrap();
    drop(socket);
    tower_pal::force_terminate_process(server.pid, server.start_time).unwrap();
    f.cli(&["daemon", "start"]);
    s.until(|v| v["method"] == "runtime.lost" && v["params"]["runtime_id"] == "crash")
        .await;
    let terminal_after = f.rpc("agent.get", json!({"id":terminal_id}));
    assert_eq!(terminal_after["status"], "idle");
    assert_eq!(terminal_after["pid"], terminal_pid);
    f.cli(&["kill", terminal_id]);
    let receipt = s
        .call(
            "command.status",
            json!({"command_id":"prompt","runtime_id":"crash","generation":"generation-crash"}),
        )
        .await;
    assert_eq!(receipt["result"]["state"], "accepted");
    assert_eq!(receipt["result"]["dispatch"]["state"], "unknown");
    assert_eq!(receipt["result"]["error"]["may_have_effect"], true);
    let duplicate = s.call("command.execute", prompt).await;
    assert_eq!(duplicate["result"]["dispatch"]["state"], "unknown");
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_questions_and_private_url_consent_are_validated_without_disk_payloads() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    for (harness, protocol) in [("codex", "codex-app-server"), ("claude-code", "acp")] {
        let launch = command(
            &format!("launch-{harness}"),
            harness,
            "launch",
            json!({"harness":harness,"protocol":protocol,"generation":format!("generation-{harness}"),"cwd":f.root.path()}),
        );
        assert!(
            s.call("command.execute", launch)
                .await
                .get("error")
                .is_none()
        );
        complete(&s, &format!("launch-{harness}")).await;
        for text in if harness == "codex" {
            vec!["question", "url"]
        } else {
            vec!["question"]
        } {
            let prompt = command(
                &format!("{text}-{harness}"),
                harness,
                "prompt",
                json!({"content":[{"type":"text","text":text}]}),
            );
            assert!(
                s.call("command.execute", prompt)
                    .await
                    .get("error")
                    .is_none()
            );
            let request = s
                .until(|v| {
                    v["method"] == "session.request"
                        && v["params"]["runtime_id"] == harness
                        && v["params"]["state"] == "pending"
                        && v["params"]["payload"]["mode"]
                            == if text == "url" { "url" } else { "form" }
                })
                .await;
            let p = &request["params"];
            let response = if text == "url" {
                json!({"action":"accept"})
            } else {
                json!({"action":"accept","content":{if harness=="codex"{"decision"}else{"question_0"}:"yes"}})
            };
            let mut reply = command(
                &format!("reply-{text}-{harness}"),
                harness,
                "reply",
                json!({"request_id":"visible","source_request_id":p["source_request_id"],"request_revision":p["request_revision"],"generation":p["generation"],"response":response}),
            );
            let mut invalid = reply.clone();
            invalid["command_id"] = json!(format!("bad-{text}-{harness}"));
            invalid["payload"]["response"]["content"] = json!({"token":"PRIVATE-REPLY-MARKER"});
            assert_eq!(
                s.call("command.execute", invalid).await["error"]["data"]["code"],
                "invalid_response"
            );
            assert_eq!(
                s.call("command.execute", reply.clone()).await["result"]["state"],
                "accepted"
            );
            complete(&s, &format!("{text}-{harness}")).await;
            reply["command_id"] = json!(format!("stale-{text}-{harness}"));
            assert_eq!(
                s.call("command.execute", reply).await["error"]["data"]["code"],
                "stale_request"
            );
        }
    }
    for file in std::fs::read_dir(f.root.path().join("state/tower/runtime")).unwrap() {
        let path = file.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(path).unwrap();
            for marker in [
                b"PRIVATE-FORM-MARKER".as_slice(),
                b"PRIVATE-URL-MARKER",
                b"PRIVATE-REPLY-MARKER",
            ] {
                assert!(!bytes.windows(marker.len()).any(|w| w == marker));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_private_reply_fences_the_owner_instead_of_waiting_forever() {
    let f = Fixture::new();
    let s = Server::start_with_retirement(&f, true).await;
    let launch = command(
        "launch",
        "lost-reply",
        "launch",
        json!({"harness":"codex","protocol":"codex-app-server","generation":"generation-lost-reply","cwd":f.root.path()}),
    );
    assert!(
        s.call("command.execute", launch)
            .await
            .get("error")
            .is_none()
    );
    complete(&s, "launch").await;
    let prompt = command(
        "prompt",
        "lost-reply",
        "prompt",
        json!({"content":[{"type":"text","text":"approve"}]}),
    );
    assert_eq!(
        s.call("command.execute", prompt).await["result"]["state"],
        "accepted"
    );
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if s.frames.lock().unwrap().iter().any(|v| {
                v["method"] == "runtime.lost" && v["params"]["reason"] == "private_reply_lost"
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let status=s.call("command.status",json!({"command_id":"prompt","runtime_id":"lost-reply","generation":"generation-lost-reply"})).await;
    assert_eq!(status["result"]["state"], "accepted");
    assert_eq!(status["result"]["dispatch"]["state"], "finished");
    assert_eq!(status["result"]["result"]["outcome"], "cancelled");
    assert!(
        s.frames
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["method"] == "runtime.lost" && v["params"]["termination"] == "confirmed")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_codex_threads_isolate_duplicate_approval_ids_and_cleanup() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    let mut requests = Vec::new();
    let mut ids = Vec::new();
    for runtime in ["left", "right"] {
        let launch = command(
            &format!("launch-{runtime}"),
            runtime,
            "launch",
            json!({
                "harness":"codex", "protocol":"codex-app-server", "generation":format!("generation-{runtime}"), "cwd":f.root.path()
            }),
        );
        assert_eq!(
            s.call("command.execute", launch).await["result"]["state"],
            "accepted"
        );
        complete(&s, &format!("launch-{runtime}")).await;
        let ready = s
            .until(|v| {
                v["method"] == "session.events"
                    && v["params"]["runtime_id"] == runtime
                    && v["params"]["events"].as_array().is_some_and(|a| {
                        a.iter().any(|e| {
                            e["kind"] == "runtime_state" && e["payload"]["state"] == "idle"
                        })
                    })
            })
            .await;
        let id = ready["params"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["kind"] == "runtime_state")
            .unwrap()["payload"]["tower_session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        ids.push(id);
        let prompt = command(
            &format!("prompt-{runtime}"),
            runtime,
            "prompt",
            json!({"content":[{"type":"text","text":"approve"}]}),
        );
        assert_eq!(
            s.call("command.execute", prompt).await["result"]["state"],
            "accepted"
        );
        requests.push(
            s.until(|v| {
                v["method"] == "session.request"
                    && v["params"]["runtime_id"] == runtime
                    && v["params"]["state"] == "pending"
            })
            .await["params"]
                .clone(),
        );
    }
    let left = f.rpc("agent.get", json!({"id":ids[0]}));
    let right = f.rpc("agent.get", json!({"id":ids[1]}));
    assert_eq!(left["pid"], right["pid"]);
    assert_ne!(left["native_session_id"], right["native_session_id"]);
    assert_eq!(left["shared_process"], true);
    assert_eq!(
        requests[0]["native_request_id"],
        requests[1]["native_request_id"]
    );
    assert_ne!(
        requests[0]["source_request_id"],
        requests[1]["source_request_id"]
    );
    for (index, runtime) in ["left", "right"].iter().enumerate() {
        let p = &requests[index];
        let reply = command(
            &format!("reply-{runtime}"),
            runtime,
            "reply",
            json!({"request_id":"visible-request","source_request_id":p["source_request_id"],"request_revision":p["request_revision"],"generation":p["generation"],"response":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}),
        );
        assert_eq!(
            s.call("command.execute", reply).await["result"]["state"],
            "accepted"
        );
        complete(&s, &format!("prompt-{runtime}")).await;
        f.cli(&["kill", &ids[index]]);
        s.until(|v| {
            v["method"] == "runtime.lost"
                && v["params"]["runtime_id"] == *runtime
                && v["params"]["termination"] == "confirmed"
        })
        .await;
        if index == 0 {
            let waiting=s.call("command.status",json!({"command_id":"prompt-right","runtime_id":"right","generation":"generation-right"})).await;
            assert_ne!(waiting["result"]["dispatch"]["state"], "finished");
            assert_eq!(
                f.rpc("agent.get", json!({"id":ids[1]}))["pid"],
                right["pid"]
            );
        }
    }
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        2
    );
}
