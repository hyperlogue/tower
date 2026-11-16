#[path = "support/cleanup.rs"]
mod cleanup;
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
        let config = root.path().join("config/tower");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("config.toml"), "[scan]\nenabled=false\n").unwrap();
        std::fs::create_dir(root.path().join("bin")).unwrap();
        let node = tower_pal::find_program("node", &tower_pal::environment())
            .expect("Node is required for protocol fixtures");
        let path = root.path().join("bin/codex");
        let source = include_str!("fixtures/codex-host.mjs").replacen(
            "#!/usr/bin/env node",
            &format!("#!{}", node.display()),
            1,
        );
        std::fs::write(&path, source).unwrap();
        tower_pal::make_executable(&path).unwrap();
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
        cleanup::finish(&mut self.root, "state", None);
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = f.root.path().join("config/tower");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("config.toml"),format!("[allgather]\nurl='ws://127.0.0.1:{port}/tower'\nmanaged=true\nscopes=['read','terminal:control']\n[scan]\nenabled=false\n")).unwrap();
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
                assert!(
                    hello["params"]["capabilities"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|c| c == "terminal-launch.v1")
                );
                let caps = json!([
                    "policy-sync.v1",
                    "session-status.v1",
                    "terminal-launch.v1",
                    "command-receipts.v1",
                    "session-stream.v1"
                ]);
                socket.send(Message::Text(json!({"jsonrpc":"2.0","id":"hello","result":{"node_name":"fixture-machine","protocol_version":{"major":1,"minor":4},"capabilities":caps}}).to_string().into())).await.unwrap();
                socket.send(Message::Text(json!({"jsonrpc":"2.0","method":"policy.push","params":{"policy_id":"tower","revision":1,"rules":[]}}).to_string().into())).await.unwrap();
                let mut pending = BTreeMap::new();
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
                                           Some("session.cursor")=>{let(c,key)=cursors.entry(runtime).or_default();json!({"runtime_id":p["runtime_id"],"generation":p["generation"],"source_key":p["source_key"],"cursor":c,"last_event_key":key,"coverage":"complete"})},
                                           Some("session.events")=>{let(c,key)=cursors.entry(runtime.clone()).or_default();for e in p["events"].as_array().unwrap(){let seq=e["sequence"].as_u64().unwrap();if seq<=*c{assert_eq!(retained.get(&(runtime.clone(),seq)),Some(e));}else{assert_eq!(seq,*c+1);retained.insert((runtime.clone(),seq),e.clone());*c=seq;*key=Some(e["key"].as_str().unwrap().into());}
                    }json!({"runtime_id":p["runtime_id"],"generation":p["generation"],"source_key":p["source_key"],"cursor":c})},
                                           _=>continue,
                                          };
                                          if let Some(id)=v.get("id")&& socket.send(Message::Text(json!({"jsonrpc":"2.0","id":id,"result":result}).to_string().into())).await.is_err(){break;}
                                         }
                                        }
                }
            }
        });
        f.rpc("rules.replace",json!({"rules":[{"id":"allow","from":"*","to":"*","action":"allow","kind":"message","created_by":"fixture","created_at":1},{"id":"sessions","from":"*","to":"*","action":"allow","kind":"session","created_by":"fixture","created_at":1}]}));
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
