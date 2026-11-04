use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tower_pal::PtyProcess;

struct Fixture {
    root: tempfile::TempDir,
    env: BTreeMap<String, String>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-cli-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = root.path();
        tower_pal::private_dir(path).unwrap();
        let env = [
            ("PATH".into(), std::env::var("PATH").unwrap()),
            ("HOME".into(), path.to_string_lossy().into_owned()),
            (
                "CODEX_HOME".into(),
                path.join("codex").to_string_lossy().into_owned(),
            ),
            (
                "XDG_STATE_HOME".into(),
                path.join("state").to_string_lossy().into_owned(),
            ),
            (
                "XDG_CONFIG_HOME".into(),
                path.join("config").to_string_lossy().into_owned(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                path.join("cache").to_string_lossy().into_owned(),
            ),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
            ("TERM".into(), "xterm-256color".into()),
        ]
        .into();
        Self { root, env }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower"));
        c.args(args)
            .env_clear()
            .envs(&self.env)
            .current_dir(self.root.path());
        c
    }
    fn call(&self, args: &[&str]) -> Value {
        let r = self.command(&["--json"]).args(args).output().unwrap();
        assert!(
            r.status.success(),
            "CLI {args:?}: {}\ndaemon: {}\npool: {}",
            String::from_utf8_lossy(&r.stderr),
            std::fs::read_to_string(self.root.path().join("state/tower/logs/towerd.log"))
                .unwrap_or_default(),
            std::fs::read_to_string(self.root.path().join("state/tower/logs/pool.log"))
                .unwrap_or_default()
        );
        serde_json::from_slice(&r.stdout).unwrap()
    }
    fn attach(&self, id: &str) -> PtyProcess {
        PtyProcess::spawn(
            &[
                env!("CARGO_BIN_EXE_tower").into(),
                "attach".into(),
                id.into(),
            ],
            &self.env,
            self.root.path(),
            24,
            80,
        )
        .unwrap()
    }
    async fn response(&self, id: &str, after: usize) -> usize {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let snapshot = self.call(&["snapshot", id]);
                assert_eq!(
                    snapshot["limited"], false,
                    "Codex emitted an unsupported terminal sequence"
                );
                let buffer = snapshot["buffer"].as_str().unwrap();
                let n = buffer
                    .rsplit("TOWER-TEST-REPLY-")
                    .next()
                    .unwrap()
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
                    .parse::<usize>()
                    .unwrap_or(0);
                if n > after {
                    return n;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "real Codex did not render response: {} ; {}",
                self.call(&["get", id]),
                self.call(&["snapshot", id])
            )
        })
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.command(&["__codex", "stop"]).output();
        let _ = self.command(&["daemon", "stop"]).output();
        if let Ok(mut s) =
            tower_pal::connect_blocking(&self.root.path().join("state/tower/run/pool.sock"))
        {
            let _ = tower_pool_proto::hello_sync(&mut s);
            let _ = tower_pool_proto::write_sync(&mut s, &tower_pool_proto::Request::Shutdown);
            let _ = tower_pool_proto::read_sync(&mut s);
        }
    }
}
async fn written(pty: &PtyProcess, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let n = pty.write(bytes).await.unwrap();
        assert!(n > 0);
        bytes = &bytes[n..];
    }
}
async fn restored(pty: &PtyProcess) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut seen = Vec::new();
        let mut buf = [0; 8192];
        loop {
            let n = pty.read(&mut buf).await.unwrap();
            assert!(n > 0);
            seen.extend_from_slice(&buf[..n]);
            assert!(seen.len() < 4 * 1024 * 1024);
            if seen.windows(5).any(|w| w == b"\x1b[>7u")
                && seen.windows(17).any(|w| w == b"TOWER-TEST-REPLY-")
            {
                break;
            }
        }
    })
    .await
    .expect("attach did not restore Codex screen and Kitty flags");
}
async fn gateway() -> (u16, tokio::task::JoinHandle<()>) {
    gateway_mode(false).await
}
async fn gateway_mode(tool: bool) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let counter = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let mut r = BufReader::new(socket);
                let mut line = String::new();
                r.read_line(&mut line).await.unwrap();
                let post = line.starts_with("POST /v1/responses ");
                let mut length = 0;
                loop {
                    line.clear();
                    r.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    assert!(
                        !line.to_ascii_lowercase().starts_with("authorization:"),
                        "fixture must receive no credentials"
                    );
                    if let Some((k, v)) = line.split_once(':')
                        && k.eq_ignore_ascii_case("content-length")
                    {
                        length = v.trim().parse::<usize>().unwrap();
                    }
                }
                assert!(length <= 2 * 1024 * 1024);
                r.read_exact(&mut vec![0; length]).await.unwrap();
                let response = if post {
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    let text = format!("TOWER-TEST-REPLY-{n}");
                    let item = if tool && n == 2 {
                        json!({"type":"function_call","id":"fc_fixture","call_id":"call_fixture","name":"exec_command","arguments":json!({"cmd":"printf APPROVAL-MARKER","sandbox_permissions":"require_escalated","justification":"Test native approval routing"}).to_string()})
                    } else {
                        json!({"type":"message","role":"assistant","id":format!("message_{n}"),"content":[{"type":"output_text","text":text}]})
                    };
                    let events = [
                        json!({"type":"response.created","response":{"id":format!("response_{n}")}}),
                        json!({"type":"response.output_item.done","item":item}),
                        json!({"type":"response.completed","response":{"id":format!("response_{n}"),"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
                    ];
                    let body = events
                        .iter()
                        .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
                        .collect::<String>();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                        .into()
                };
                r.get_mut().write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    (port, task)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Codex; uses only a local fixture API"]
async fn real_codex_shared_host_attach_restart_and_kitty_reattach() {
    let f = Fixture::new();
    let (port, api) = gateway().await;
    let home = f.root.path().join("codex");
    std::fs::create_dir(&home).unwrap();
    let project = serde_json::to_string(f.root.path().to_str().unwrap()).unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!(
            r#"model = "test-model"
model_provider = "fixture"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
[model_providers.fixture]
name = "Local terminal fixture"
base_url = "http://127.0.0.1:{port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
[projects.{project}]
trust_level = "trusted"
"#
        ),
    )
    .unwrap();
    let opened = f.call(&[
        "open",
        "codex",
        "--mode",
        "app-server",
        "--headless",
        "--cwd",
        f.root.path().to_str().unwrap(),
        "--",
        "Reply with ready.",
    ]);
    let id = opened["id"].as_str().unwrap();
    let first = f.response(id, 0).await;
    let delivery = f.call(&["send", id, "A Tower-delivered message: reply with ready."]);
    assert_eq!(delivery["method"], "native");
    assert_eq!(delivery["acknowledgement"], "harness");
    let first = f.response(id, first).await;
    let mut a = f.attach(id);
    restored(&a).await;
    let b = f.attach(id);
    restored(&b).await;
    let before = f.call(&["get", id]);
    assert_eq!(before["attach_count"], 2);
    f.call(&["daemon", "stop"]);
    written(&b, b"\x1b[200~Second prompt\x1b[201~\r").await;
    let after = f.call(&["get", id]);
    assert_eq!(before["pid"], after["pid"]);
    assert_eq!(after["attach_count"], 2);
    let second = f.response(id, first).await;
    written(&a, b"\x1b[32;5u\x1b[32;5:3u\x1b[113;5u").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(code) = a.poll_exit().unwrap() {
                assert_eq!(code, 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Kitty detach keys did not detach the client");
    let c = f.attach(id);
    restored(&c).await;
    written(&c, b"\x1b[200~Third prompt\x1b[201~\r").await;
    let third = f.response(id, second).await;
    assert_eq!(f.call(&["get", id])["pid"], before["pid"]);
    let row = f.call(&["get", id]);
    assert!(
        row["native_session_id"].is_string(),
        "missing native session identity: {row}"
    );
    assert!(
        !f.command(&["restart", id, "--mode", "native"])
            .output()
            .unwrap()
            .status
            .success()
    );
    f.call(&["restart", id]);
    assert_eq!(
        f.response(id, third - 1).await,
        third,
        "resume must restore the existing conversation"
    );
    let resumed_client = f.attach(id);
    restored(&resumed_client).await;
    written(
        &resumed_client,
        b"\x1b[200~Continue this conversation\x1b[201~\r",
    )
    .await;
    f.response(id, third).await;
    let resumed = f.call(&["get", id]);
    assert_eq!(resumed["native_session_id"], row["native_session_id"]);
    assert_eq!(resumed["pid"], row["pid"]);
    f.call(&["kill", id]);
    assert_eq!(f.call(&["get", id])["status"], "exited");
    api.abort();
    assert!(Path::new(&f.env["CODEX_HOME"]).is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Codex; checks cross-client native approval routing against a local fixture"]
async fn registered_app_server_delivery_preserves_the_tui_approval_path() {
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::AtomicBool;
    use tokio_tungstenite::tungstenite::Message;
    use tower_pool_proto::{Frame, Launch, Reply, Request, Size};
    let mut f = Fixture::new();
    let runtime = f.root.path().join("runtime");
    tower_pal::private_dir(&runtime).unwrap();
    f.env.insert(
        "XDG_RUNTIME_DIR".into(),
        runtime.to_string_lossy().into_owned(),
    );
    f.env
        .insert("TMPDIR".into(), runtime.to_string_lossy().into_owned());
    let (port, api) = gateway_mode(true).await;
    let home = f.root.path().join("codex");
    tower_pal::private_dir(&home).unwrap();
    tower_pal::private_dir(f.root.path()).unwrap();
    let project = serde_json::to_string(f.root.path().to_str().unwrap()).unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!(
            r#"model="gpt-5.3-codex"
model_provider="fixture"
approval_policy="on-request"
sandbox_mode="read-only"
check_for_update_on_startup=false
[model_providers.fixture]
name="Local approval fixture"
base_url="http://127.0.0.1:{port}/v1"
wire_api="responses"
request_max_retries=0
stream_max_retries=0
[projects.{project}]
trust_level="trusted"
"#
        ),
    )
    .unwrap();
    let server_path = f.root.path().join("server.sock");
    let endpoint = format!("unix://{}", server_path.display());
    let mut server = tower_pal::spawn_service(
        &[
            "codex".into(),
            "app-server".into(),
            "--listen".into(),
            endpoint.clone(),
        ],
        &f.env,
        f.root.path(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !server_path.exists() {
            assert!(server.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let (mut setup, _) = tokio_tungstenite::client_async(
        "ws://localhost/",
        tower_pal::connect(&server_path).await.unwrap(),
    )
    .await
    .unwrap();
    setup.send(Message::Text(json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"tower-test","version":"1"},"capabilities":{"experimentalApi":true}}}).to_string().into())).await.unwrap();
    loop {
        let frame = setup.next().await.unwrap().unwrap();
        if let Message::Text(text) = frame
            && serde_json::from_str::<Value>(&text).unwrap()["id"] == 1
        {
            break;
        }
    }
    setup
        .send(Message::Text(
            json!({"method":"initialized"}).to_string().into(),
        ))
        .await
        .unwrap();
    setup.send(Message::Text(json!({"id":2,"method":"thread/start","params":{"cwd":f.root.path(),"approvalPolicy":"on-request","sandbox":"read-only"}}).to_string().into())).await.unwrap();
    let thread = loop {
        let frame = setup.next().await.unwrap().unwrap();
        if let Message::Text(text) = frame {
            let v: Value = serde_json::from_str(&text).unwrap();
            if v["id"] == 2 {
                break v["result"]["thread"]["id"]
                    .as_str()
                    .expect("thread start response")
                    .to_owned();
            }
        }
    };
    setup.send(Message::Text(json!({"id":3,"method":"turn/start","params":{"threadId":thread,"input":[{"type":"text","text":"Bootstrap this conversation.","text_elements":[]}]}}).to_string().into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = setup.next().await.unwrap().unwrap();
            if let Message::Text(text) = frame
                && serde_json::from_str::<Value>(&text).unwrap()["method"] == "turn/completed"
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    setup.close(None).await.unwrap();
    let proxy_path = f.root.path().join("proxy.sock");
    let listener = tower_pal::IpcListener::bind(&proxy_path).await.unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let approval = Arc::new(AtomicBool::new(false));
    let selected = thread.clone();
    let ready_task = ready.clone();
    let approval_task = approval.clone();
    let proxy = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let mut client = tokio_tungstenite::accept_async(client).await.unwrap();
        let (mut upstream, _) = tokio_tungstenite::client_async(
            "ws://localhost/",
            tower_pal::connect(&server_path).await.unwrap(),
        )
        .await
        .unwrap();
        loop {
            tokio::select! {
                frame=client.next()=>{let Some(Ok(frame))=frame else{break};if upstream.send(frame).await.is_err(){break;}},
                frame=upstream.next()=>{let Some(Ok(frame))=frame else{break};if let Message::Text(text)=&frame && let Ok(v)=serde_json::from_str::<Value>(text){
                    if v.pointer("/result/thread/id").and_then(Value::as_str)==Some(&selected){ready_task.store(true,Ordering::SeqCst);}
                    if v["method"].as_str().is_some_and(|m|m.ends_with("/requestApproval")){approval_task.store(true,Ordering::SeqCst);}
                }
                if client.send(frame).await.is_err(){break;}}
            }
        }
    });
    assert!(
        f.command(&["__pool", "run", "--detach"])
            .status()
            .unwrap()
            .success()
    );
    let pool_path = f.root.path().join("state/tower/run/pool.sock");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pool_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pool = |request: &Request| {
        let mut stream = tower_pal::connect_blocking(&pool_path).unwrap();
        tower_pool_proto::hello_sync(&mut stream).unwrap();
        tower_pool_proto::write_sync(&mut stream, request).unwrap();
        let result = tower_pool_proto::read_sync(&mut stream)
            .unwrap()
            .json::<Reply>()
            .unwrap()
            .checked()
            .unwrap();
        (result, stream)
    };
    let (created, _) = pool(&Request::Create {
        launch: Launch {
            name: "external-ui".into(),
            argv: vec![
                "codex".into(),
                "--remote".into(),
                format!("unix://{}", proxy_path.display()),
                "resume".into(),
                thread.clone(),
            ],
            env: f.env.clone(),
            cwd: f.root.path().to_string_lossy().into_owned(),
            size: Some(Size::default()),
            clipboard: None,
        },
    });
    let Reply::Created { session } = created else {
        panic!()
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !ready.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("TUI did not resume the external thread");
    let registered = f.call(&[
        "register",
        "--harness",
        "codex",
        "--session-id",
        &thread,
        "--pid",
        &session.pid.unwrap().to_string(),
    ]);
    let id = registered["id"].as_str().unwrap();
    f.call(&["rpc","agent.delivery",&json!({"id":id,"token":registered["token"],"delivery":{"thread_id":thread,"app_server":endpoint,"codex_home":home}}).to_string()]);
    let sent = f.call(&["send", id, "Exercise the native approval path."]);
    assert_eq!(sent["method"], "native");
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        while !approval.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if observed.is_err() {
        let (header, mut stream) = pool(&Request::Snapshot {
            name: "external-ui".into(),
        });
        let mut bytes = Vec::new();
        loop {
            match tower_pool_proto::read_sync(&mut stream).unwrap() {
                Frame::Data(data) => bytes.extend(data),
                Frame::Json(_) => break,
                _ => {}
            }
        }
        panic!(
            "Native approval did not reach the TUI: {header:?}; {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let _ = pool(&Request::Kill {
        name: "external-ui".into(),
    });
    proxy.abort();
    let _ = server.start_kill();
    let _ = server.wait().await;
    api.abort();
}
