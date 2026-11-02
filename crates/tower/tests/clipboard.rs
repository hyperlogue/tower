use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tower_pool_proto::{self as wire, Frame, Reply, Request, Size};
use tower_protocol::{AttachInfo, CallerContext, ClipboardClient, ClipboardImage};

struct Fixture {
    root: tempfile::TempDir,
    env: BTreeMap<String, String>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-clip-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = root.path();
        std::fs::create_dir(path.join("bin")).unwrap();
        let node = tower_pal::find_program("node", &tower_pal::environment())
            .expect("Node is required for clipboard fixtures");
        let script = include_str!("fixtures/clipboard-agent.mjs").replacen(
            "#!/usr/bin/env node",
            &format!("#!{}", node.display()),
            1,
        );
        for name in ["codex", "claude"] {
            let bin = path.join("bin").join(name);
            std::fs::write(&bin, &script).unwrap();
            tower_pal::make_executable(&bin).unwrap();
        }
        let env = [
            ("HOME".into(), path.display().to_string()),
            (
                "PATH".into(),
                format!(
                    "{}:{}",
                    path.join("bin").display(),
                    std::env::var("PATH").unwrap()
                ),
            ),
            (
                "XDG_STATE_HOME".into(),
                path.join("s").display().to_string(),
            ),
            (
                "XDG_CONFIG_HOME".into(),
                path.join("c").display().to_string(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                path.join("cache").display().to_string(),
            ),
            ("TOWER_TEST_BIN".into(), env!("CARGO_BIN_EXE_tower").into()),
            ("TERM".into(), "xterm-256color".into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
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
        let out = self.command(&["--json"]).args(args).output().unwrap();
        assert!(
            out.status.success(),
            "{} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }
    fn socket(&self) -> PathBuf {
        self.root.path().join("s/tower/run/towerd.sock")
    }
    async fn attach(&self, id: &str, client: &ClipboardClient) -> tower_pal::IpcStream {
        let info: AttachInfo = tower_clipboard::client::call(
            &self.socket(),
            "session.attach_info",
            json!({"id":id,"client_id":client.client_id,"client_token":client.client_token}),
            &CallerContext::default(),
        )
        .await
        .unwrap();
        let mut stream = tower_pal::connect(std::path::Path::new(&info.pool_socket))
            .await
            .unwrap();
        wire::hello(&mut stream).await.unwrap();
        wire::write(
            &mut stream,
            &Request::Attach {
                name: info.pool_session,
                size: Size::default(),
                clipboard: info.clipboard,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            wire::read(&mut stream)
                .await
                .unwrap()
                .json::<Reply>()
                .unwrap()
                .checked()
                .unwrap(),
            Reply::Attached { .. }
        ));
        stream
    }
    async fn provider(&self, name: &str, image: &[u8]) -> Provider {
        let file = self.root.path().join(format!("{name}.png"));
        std::fs::write(&file, image).unwrap();
        let credentials = self.root.path().join(format!("{name}.json"));
        let child = self
            .command(&[
                "clipboard",
                "provide",
                "--name",
                name,
                "--credentials",
                credentials.to_str().unwrap(),
                "--",
                "cat",
                file.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(self.root.path().join(format!("{name}.log"))).unwrap())
            .spawn()
            .unwrap();
        let mut provider = Provider {
            child,
            client: None,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(bytes) = tower_pal::read_private(&credentials) {
                    provider.client = Some(serde_json::from_slice(&bytes).unwrap());
                    break;
                }
                assert!(
                    provider.child.try_wait().unwrap().is_none(),
                    "provider failed: {}",
                    std::fs::read_to_string(self.root.path().join(format!("{name}.log")))
                        .unwrap_or_default()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        provider
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for args in [["__pool", "stop"], ["daemon", "stop"]] {
            let _ = self
                .command(&["--no-start"])
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
struct Provider {
    child: Child,
    client: Option<ClipboardClient>,
}
impl Provider {
    fn identity(&self) -> &ClipboardClient {
        self.client.as_ref().unwrap()
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
async fn output(stream: &mut tower_pal::IpcStream, needle: &str) -> String {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut out = String::new();
        loop {
            match wire::read(stream).await.unwrap() {
                Frame::Data(b) => out.push_str(&String::from_utf8_lossy(&b)),
                Frame::Json(b) => {
                    let reply: Reply = serde_json::from_slice(&b).unwrap();
                    assert!(
                        !matches!(
                            reply,
                            Reply::Clipboard { code: Some(_), .. } | Reply::Error { .. }
                        ),
                        "{reply:?}"
                    );
                }
                _ => {}
            }
            if out.contains(needle) {
                return out;
            }
        }
    })
    .await
    .expect("terminal output timed out")
}
fn image(red: u8) -> Vec<u8> {
    fn chunk(out: &mut Vec<u8>, kind: &[u8], bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        let start = out.len();
        out.extend(kind);
        out.extend(bytes);
        let mut crc = 0xffffffffu32;
        for byte in &out[start..] {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ if crc & 1 == 1 { 0xedb88320 } else { 0 };
            }
        }
        out.extend_from_slice(&(!crc).to_be_bytes());
    }
    let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
    chunk(&mut out, b"IHDR", &[0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0]);
    let raw = [0, red, 0, 0];
    let mut a = 1u32;
    let mut b = 0u32;
    for x in raw {
        a = (a + u32::from(x)) % 65521;
        b = (b + a) % 65521;
    }
    let mut data = vec![0x78, 0x01, 0x01, 4, 0, 0xfb, 0xff];
    data.extend(raw);
    data.extend_from_slice(&((b << 16) | a).to_be_bytes());
    chunk(&mut out, b"IDAT", &data);
    chunk(&mut out, b"IEND", &[]);
    out
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clipboard_two_clients_paste_sources_shims_detach_and_restart() {
    let f = Fixture::new();
    let a_image = image(32);
    let b_image = image(224);
    let codex = f.call(&["open", "codex", "--mode", "native", "--headless"])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let a = f.provider("A", &a_image).await;
    let b = f.provider("B", &b_image).await;
    for (harness, id) in [
        ("codex", codex),
        (
            "claude",
            f.call(&["open", "claude", "--headless"])["id"]
                .as_str()
                .unwrap()
                .to_owned(),
        ),
    ] {
        if harness == "claude" && tower_pal::operating_system() != "linux" {
            f.call(&["kill", &id]);
            continue;
        }
        let mut ca = f.attach(&id, a.identity()).await;
        output(&mut ca, "READY").await;
        let mut cb = f.attach(&id, b.identity()).await;
        output(&mut cb, "READY").await;
        assert_eq!(
            f.call(&["clipboard", "status", &id])["source"]["client_id"],
            b.identity().client_id
        );
        wire::data(&mut ca, b"draft\x16Z").await.unwrap();
        let seen = output(&mut ca, "KEY:Z").await;
        assert!(
            seen.contains(&format!(
                "IMAGE:{}",
                Sha256::digest(&a_image)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )),
            "{harness}: {seen}"
        );
        assert!(seen.find("IMAGE:").unwrap() < seen.find("KEY:Z").unwrap());
        assert_eq!(
            f.call(&["clipboard", "status", &id])["source"]["client_id"],
            b.identity().client_id
        );
        let status = f.call(&["clipboard", "status", &id]);
        let img: ClipboardImage = tower_clipboard::client::call(
            &f.socket(),
            "clipboard.read",
            json!({"id":id,"generation":status["generation"],"format":"image/png"}),
            &CallerContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            tower_pal::read_private(std::path::Path::new(&img.path)).unwrap(),
            b_image
        );
        wire::write(&mut cb, &Request::Detach).await.unwrap();
        cb.shutdown().await.unwrap();
        drop(cb);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if f.call(&["clipboard", "status", &id])["source"]["client_id"]
                    == a.identity().client_id
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let img: ClipboardImage = tower_clipboard::client::call(
            &f.socket(),
            "clipboard.read",
            json!({"id":id,"generation":status["generation"],"format":"image/png"}),
            &CallerContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            tower_pal::read_private(std::path::Path::new(&img.path)).unwrap(),
            a_image
        );
        f.call(&["kill", &id]);
    }
    let codex = f.call(&["open", "codex", "--mode", "native", "--headless"])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut attached = f.attach(&codex, a.identity()).await;
    output(&mut attached, "READY").await;
    f.call(&["daemon", "stop"]);
    f.call(&["ls"]);
    assert!(f.call(&["clipboard", "status", &codex])["source"].is_null());
    wire::data(&mut attached, b"Q").await.unwrap();
    output(&mut attached, "KEY:Q").await;
    assert!(tower_clipboard::client::call::<Value>(&f.socket(),"session.attach_info",json!({"id":codex,"client_id":a.identity().client_id,"client_token":a.identity().client_token}),&CallerContext::default()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Codex; verifies image paste with an isolated loopback API"]
async fn real_codex_clipboard_reaches_local_model_only_after_submit() {
    real_clipboard("codex").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Claude Code; verifies image paste with an isolated loopback API"]
async fn real_claude_clipboard_reaches_local_model_only_after_submit() {
    real_clipboard("claude").await;
}

async fn real_clipboard(harness: &str) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    let received = Arc::new(AtomicBool::new(false));
    let delivered = received.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let api = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let delivered = delivered.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let codex = line.starts_with("POST /v1/responses ");
                let claude = line.starts_with("POST /v1/messages ")
                    || line.starts_with("POST /v1/messages?");
                let mut length = 0;
                loop {
                    line.clear();
                    stream.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                        if key.eq_ignore_ascii_case("authorization") {
                            assert_eq!(value.trim(), "Bearer fixture-key");
                        }
                        if key.eq_ignore_ascii_case("x-api-key") {
                            assert_eq!(value.trim(), "fixture-key");
                        }
                    }
                }
                assert!(length <= 4 * 1024 * 1024);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                if codex || claude {
                    let value: Value = serde_json::from_slice(&body).unwrap();
                    fn has_image(v: &Value) -> bool {
                        match v {
                            Value::Array(a) => a.iter().any(has_image),
                            Value::Object(o) => {
                                matches!(
                                    o.get("type").and_then(Value::as_str),
                                    Some("input_image" | "image")
                                ) || o.values().any(has_image)
                            }
                            _ => false,
                        }
                    }
                    if has_image(&value) {
                        delivered.store(true, Ordering::SeqCst);
                    }
                }
                let (kind, body) = if codex {
                    let events = [
                        json!({"type":"response.created","response":{"id":"response_clipboard"}}),
                        json!({"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":"message_clipboard","content":[{"type":"output_text","text":"TOWER-CLIPBOARD-REPLY"}]}}),
                        json!({"type":"response.completed","response":{"id":"response_clipboard","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
                    ];
                    (
                        "text/event-stream",
                        events
                            .iter()
                            .map(|e| {
                                format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap())
                            })
                            .collect::<String>(),
                    )
                } else if claude {
                    let events = [
                        json!({"type":"message_start","message":{"id":"msg_clipboard","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}),
                        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"TOWER-CLIPBOARD-REPLY"}}),
                        json!({"type":"content_block_stop","index":0}),
                        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}),
                        json!({"type":"message_stop"}),
                    ];
                    (
                        "text/event-stream",
                        events
                            .iter()
                            .map(|e| {
                                format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap())
                            })
                            .collect::<String>(),
                    )
                } else {
                    ("application/json", r#"{"input_tokens":1}"#.into())
                };
                let _=stream.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
            });
        }
    });
    let mut f = Fixture::new();
    f.env.insert("PATH".into(), std::env::var("PATH").unwrap());
    let path = f.root.path();
    let home = path.join(harness);
    std::fs::create_dir(&home).unwrap();
    if harness == "codex" {
        f.env
            .insert("CODEX_HOME".into(), home.display().to_string());
        std::fs::write(
            home.join("config.toml"),
            format!(
                r#"model="test-model"
model_provider="fixture"
approval_policy="never"
sandbox_mode="read-only"
check_for_update_on_startup=false
[model_providers.fixture]
name="Clipboard fixture"
base_url="http://127.0.0.1:{port}/v1"
wire_api="responses"
request_max_retries=0
stream_max_retries=0
[projects.{}]
trust_level="trusted"
"#,
                serde_json::to_string(path.to_str().unwrap()).unwrap()
            ),
        )
        .unwrap();
    } else {
        f.env.extend([
            ("CLAUDE_CONFIG_DIR".into(), home.display().to_string()),
            (
                "ANTHROPIC_BASE_URL".into(),
                format!("http://127.0.0.1:{port}"),
            ),
            ("ANTHROPIC_API_KEY".into(), "fixture-key".into()),
            (
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC".into(),
                "1".into(),
            ),
        ]);
        std::fs::write(home.join(".claude.json"),serde_json::to_vec(&json!({"hasCompletedOnboarding":true,"theme":"dark","customApiKeyResponses":{"approved":["fixture-key"]},"projects":{path.to_str().unwrap():{"hasTrustDialogAccepted":true,"hasCompletedProjectOnboarding":true}}})).unwrap()).unwrap();
    }
    let args = if harness == "codex" {
        vec![
            "open",
            "codex",
            "--mode",
            "app-server",
            "--headless",
            "--",
            "Reply with ready.",
        ]
    } else {
        vec![
            "open",
            "claude",
            "--headless",
            "--",
            "--model",
            "sonnet",
            "Reply with ready.",
        ]
    };
    let id = f.call(&args)["id"].as_str().unwrap().to_owned();
    let provider = f.provider("native-image", &image(128)).await;
    let mut attached = f.attach(&id, provider.identity()).await;
    let ready = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let screen = f.call(&["snapshot", &id]);
            if screen["buffer"]
                .as_str()
                .unwrap()
                .contains("TOWER-CLIPBOARD-REPLY")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if ready.is_err() {
        api.abort();
        panic!("native startup: {}", f.call(&["snapshot", &id]));
    }
    wire::data(&mut attached, b"\x16").await.unwrap();
    let submitted = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            if let Frame::Json(bytes) = wire::read(&mut attached).await.unwrap() {
                match serde_json::from_slice::<Reply>(&bytes).unwrap() {
                    Reply::Clipboard { state, code } => {
                        assert_eq!(
                            state,
                            "submitted",
                            "{code:?}; {}",
                            f.call(&["snapshot", &id])
                        );
                        break;
                    }
                    Reply::Error { code, message } => panic!("{code}: {message}"),
                    _ => {}
                }
            }
        }
    })
    .await;
    assert!(
        submitted.is_ok(),
        "native paste timed out: {}",
        f.call(&["snapshot", &id])
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !received.load(Ordering::SeqCst),
        "paste submitted a prompt automatically"
    );
    wire::data(&mut attached, b"\x1b[200~Describe this image.\x1b[201~")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    wire::data(&mut attached, b"\r").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        while !received.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    api.abort();
    assert!(
        result.is_ok(),
        "image did not reach the local API: {}",
        f.call(&["snapshot", &id])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_binds_stdin_credentials_without_echo_and_rejects_agent_providers() {
    let f = Fixture::new();
    let id = f.call(&["open", "codex", "--mode", "native", "--headless"])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let provider = f.provider("stdin-client", &image(72)).await;
    let mut terminal = tower_pal::PtyProcess::spawn(
        &[
            env!("CARGO_BIN_EXE_tower").into(),
            "attach".into(),
            id.clone(),
            "--clipboard-stdin".into(),
        ],
        &f.env,
        f.root.path(),
        24,
        80,
    )
    .unwrap();
    async fn read_until(terminal: &tower_pal::PtyProcess, needle: &str) -> String {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut all = String::new();
            let mut buf = [0; 8192];
            loop {
                let n = terminal.read(&mut buf).await.unwrap();
                assert!(n > 0);
                all.push_str(&String::from_utf8_lossy(&buf[..n]));
                if all.contains(needle) {
                    return all;
                }
            }
        })
        .await
        .unwrap()
    }
    read_until(&terminal, "ready for clipboard credential").await;
    let mut credential = serde_json::to_vec(provider.identity()).unwrap();
    credential.push(b'\n');
    let mut remaining = credential.as_slice();
    while !remaining.is_empty() {
        let n = terminal.write(remaining).await.unwrap();
        remaining = &remaining[n..];
    }
    let restored = read_until(&terminal, "READY").await;
    assert!(!restored.contains(&provider.identity().client_token.0));
    assert_eq!(
        f.call(&["clipboard", "status", &id])["source"]["client_id"],
        provider.identity().client_id
    );
    terminal.write(b"\x16").await.unwrap();
    read_until(&terminal, "IMAGE:").await;
    terminal.write(b"\0\x11").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if terminal.poll_exit().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let token = tower_pal::read_private(&f.root.path().join("s/tower/tokens").join(&id)).unwrap();
    let context = CallerContext {
        agent_token: Some(tower_protocol::Secret(String::from_utf8(token).unwrap())),
        ..Default::default()
    };
    let result = tower_clipboard::client::call::<Value>(
        &f.socket(),
        "clipboard.offer",
        json!({"name":"agent-attempt","formats":["image/png"]}),
        &context,
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_provider_does_not_block_other_clients_and_revocation_discards_queued_submit() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let f = Fixture::new();
    let id = f.call(&["open", "codex", "--mode", "native", "--headless"])["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut provider = BufReader::new(tower_pal::connect(&f.socket()).await.unwrap());
    provider.get_mut().write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"clipboard.offer\",\"params\":{\"name\":\"stalled\",\"formats\":[\"image/png\"]}}\n").await.unwrap();
    let mut line = String::new();
    provider.read_line(&mut line).await.unwrap();
    let response: Value = serde_json::from_str(&line).unwrap();
    let identity: ClipboardClient = serde_json::from_value(response["result"].clone()).unwrap();
    let other = f.provider("responsive", &image(64)).await;
    let mut a = f.attach(&id, &identity).await;
    output(&mut a, "READY").await;
    let mut b = f.attach(&id, other.identity()).await;
    output(&mut b, "READY").await;
    wire::data(&mut a, b"draft\x16\r").await.unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(5), provider.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let read: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(read["method"], "clipboard.read");
    wire::data(&mut b, b"Q").await.unwrap();
    output(&mut b, "KEY:Q").await;
    provider
        .get_mut()
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"clipboard.revoke\",\"params\":{}}\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Frame::Json(bytes) = wire::read(&mut a).await.unwrap()
                && let Reply::Clipboard { state, code } = serde_json::from_slice(&bytes).unwrap()
            {
                assert_eq!(state, "failed");
                assert_eq!(code.as_deref(), Some("clipboard_unavailable"));
                break;
            }
        }
    })
    .await
    .unwrap();
    let screen = f.call(&["snapshot", &id]);
    let text = screen["buffer"].as_str().unwrap();
    assert!(text.contains("KEY:d"));
    assert!(!text.contains("IMAGE:"));
    assert_eq!(
        f.call(&["clipboard", "status", &id])["source"]["client_id"],
        other.identity().client_id
    );
    wire::data(&mut a, b"R").await.unwrap();
    output(&mut a, "KEY:R").await;
}
