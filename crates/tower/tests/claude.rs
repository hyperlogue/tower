use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

struct Fixture {
    root: tempfile::TempDir,
    env: BTreeMap<String, String>,
}
impl Fixture {
    fn command(&self, args: &[&str]) -> Command {
        self.program(env!("CARGO_BIN_EXE_tower-cli"), args)
    }
    fn program(&self, binary: impl AsRef<std::ffi::OsStr>, args: &[&str]) -> Command {
        let mut command = Command::new(binary);
        command
            .args(args)
            .env_clear()
            .envs(&self.env)
            .current_dir(self.root.path());
        command
    }
    fn call(&self, args: &[&str]) -> Value {
        let output = self.command(&["--json"]).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for (binary, args) in [
            (env!("CARGO_BIN_EXE_tower-terminal-pool"), &["stop"][..]),
            (env!("CARGO_BIN_EXE_tower-cli"), &["daemon", "stop"][..]),
        ] {
            let _ = self
                .program(binary, args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Claude Code; uses an isolated home and local fake Anthropic API"]
async fn real_claude_hook_identity_and_output_use_only_local_fixture_api() {
    exercise_claude(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires installed Claude Code; sends through its real inbox with only a local API"]
async fn real_claude_native_inbox_delivers_to_the_model() {
    exercise_claude(true).await;
}

async fn exercise_claude(native_delivery: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let arrived = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let state = (arrived.clone(), delivered.clone(), release.clone());
    let api = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (arrived, delivered, release) = state.clone();
            tokio::spawn(async move {
                let mut read = BufReader::new(stream);
                let mut line = String::new();
                read.read_line(&mut line).await.unwrap();
                let messages = line.starts_with("POST /v1/messages ")
                    || line.starts_with("POST /v1/messages?");
                let mut length = 0;
                loop {
                    line.clear();
                    read.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                        if key.eq_ignore_ascii_case("x-api-key") {
                            assert_eq!(value.trim(), "fixture-key");
                        }
                    }
                }
                assert!(length <= 2 * 1024 * 1024);
                let mut body = vec![0; length];
                read.read_exact(&mut body).await.unwrap();
                if messages && native_delivery {
                    let body = String::from_utf8(body).unwrap();
                    if body.contains("TOWER-INBOX-MARKER") {
                        delivered.store(true, Ordering::Release);
                    }
                    if body.contains("Return the fixture greeting.")
                        && !arrived.swap(true, Ordering::AcqRel)
                    {
                        release.notified().await;
                    }
                }
                let (kind, body) = if messages {
                    let events = [
                        json!({"type":"message_start","message":{"id":"msg_fixture","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}),
                        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"TOWER-CLAUDE-FIXTURE"}}),
                        json!({"type":"content_block_stop","index":0}),
                        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4}}),
                        json!({"type":"message_stop"}),
                    ];
                    (
                        "text/event-stream",
                        events
                            .iter()
                            .map(|v| {
                                format!("event: {}\ndata: {v}\n\n", v["type"].as_str().unwrap())
                            })
                            .collect::<String>(),
                    )
                } else {
                    ("application/json", r#"{"input_tokens":1}"#.into())
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                read.get_mut().write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    let root = tempfile::Builder::new()
        .prefix("tw-claude-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = root.path();
    let env = [
        ("HOME".into(), path.to_string_lossy().into_owned()),
        ("PATH".into(), std::env::var("PATH").unwrap()),
        (
            "CLAUDE_CONFIG_DIR".into(),
            path.join("claude").to_string_lossy().into_owned(),
        ),
        (
            "XDG_STATE_HOME".into(),
            path.join("s").to_string_lossy().into_owned(),
        ),
        (
            "XDG_CONFIG_HOME".into(),
            path.join("c").to_string_lossy().into_owned(),
        ),
        (
            "ANTHROPIC_BASE_URL".into(),
            format!("http://127.0.0.1:{port}"),
        ),
        ("ANTHROPIC_API_KEY".into(), "fixture-key".into()),
        (
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC".into(),
            "1".into(),
        ),
        ("TOKIO_WORKER_THREADS".into(), "2".into()),
        ("TERM".into(), "xterm-256color".into()),
    ]
    .into();
    let f = Fixture { root, env };
    if native_delivery {
        std::fs::create_dir(f.root.path().join("claude")).unwrap();
        std::fs::write(
            f.root.path().join("claude/settings.json"),
            r#"{"crossSessionInbound":"accept"}"#,
        )
        .unwrap();
    }
    let opened = f.call(&[
        "launch",
        "claude",
        "--headless",
        "--",
        "--print",
        "--model",
        "sonnet",
        "Return the fixture greeting.",
    ]);
    let id = opened["id"].as_str().unwrap();
    if native_delivery {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let row = f.call(&["get", id]);
                if arrived.load(Ordering::Acquire) && row["caps"]["message"] == "native" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Claude inbox was not ready: {} {}",
                f.call(&["get", id]),
                f.call(&["snapshot", id])
            )
        });
        let receipt = f.call(&[
            "send",
            id,
            "TOWER-INBOX-MARKER: Return the fixture greeting.",
        ]);
        assert_eq!(receipt["method"], "native");
        assert_eq!(receipt["acknowledgement"], "transport");
        release.notify_one();
    }
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = f.call(&["get", id]);
            let snapshot = f.call(&["snapshot", id]);
            if row["status"] == "exited" {
                assert!(
                    snapshot["buffer"]
                        .as_str()
                        .unwrap()
                        .contains("TOWER-CLAUDE-FIXTURE"),
                    "{row} {snapshot}"
                );
                assert!(row["native_session_id"].is_string(), "{row}");
                assert_eq!(row["caps"]["resume"], true);
                if native_delivery {
                    assert!(
                        delivered.load(Ordering::Acquire),
                        "Native inbox text did not reach the model"
                    );
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    api.abort();
    result.unwrap_or_else(|_| {
        panic!(
            "Claude fixture did not finish: {} {}",
            f.call(&["get", id]),
            f.call(&["snapshot", id])
        )
    });
}
