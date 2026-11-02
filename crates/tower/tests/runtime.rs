//! Exercise the installed CLI boundary with isolated state and real daemons.
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

struct Fixture {
    dir: tempfile::TempDir,
    child: Option<Child>,
    binary: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        Self {
            // Keep the socket below the macOS Unix path limit.
            dir: tempfile::Builder::new()
                .prefix("tw-")
                .tempdir_in("/tmp")
                .unwrap(),
            child: None,
            binary: env!("CARGO_BIN_EXE_tower").into(),
        }
    }
    fn standalone() -> Self {
        let mut f = Self::new();
        let install = f.dir.path().join("install");
        std::fs::create_dir(&install).unwrap();
        let binary = install.join("tower");
        std::fs::copy(&f.binary, &binary).unwrap();
        tower_pal::make_executable(&binary).unwrap();
        f.binary = binary;
        assert_eq!(std::fs::read_dir(install).unwrap().count(), 1);
        f
    }
    fn socket(&self) -> PathBuf {
        self.dir.path().join("state/tower/run/towerd.sock")
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(&self.binary);
        c.args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("TERM", "xterm-256color")
            .env("HOME", self.dir.path())
            .env("XDG_STATE_HOME", self.dir.path().join("state"))
            .env("XDG_CONFIG_HOME", self.dir.path().join("config"))
            .env("TOKIO_WORKER_THREADS", "2");
        for key in [
            "TOWER_SOCKET",
            "TOWER_AGENT_ID",
            "TOWER_AGENT_TOKEN",
            "TOWER_BEARER_TOKEN",
            "CLAUDE_CODE_SESSION_ID",
            "CODEX_THREAD_ID",
            "CODEX_SESSION_ID",
        ] {
            c.env_remove(key);
        }
        c
    }
    fn run(&self, args: &[&str]) -> Value {
        decode(self.command(args).arg("--json").output().unwrap())
    }
    fn stop(&self) {
        let output = self.command(&["daemon", "stop"]).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        wait_for(|| !self.socket().exists());
        let _lock =
            tower_pal::SingletonLock::acquire(&self.dir.path().join("state/tower/run/towerd.lock"))
                .expect("daemon stop must wait for the lifetime lock to be released");
    }
    fn foreground(&mut self) {
        self.child = Some(
            self.command(&["daemon", "run"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        wait_for(|| self.socket().exists());
        self.run(&["--no-start", "daemon", "status"]);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self
            .command(&["daemon", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.socket().exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if let Ok(mut stream) =
            tower_pal::connect_blocking(&self.dir.path().join("state/tower/run/pool.sock"))
        {
            let _ = tower_pool_proto::hello_sync(&mut stream);
            let _ = tower_pool_proto::write_sync(&mut stream, &tower_pool_proto::Request::Shutdown);
            let _ = tower_pool_proto::read_sync(&mut stream);
        }
    }
}
fn wait_for(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "daemon did not change state");
        thread::sleep(Duration::from_millis(10));
    }
}
fn decode(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn failure(output: Output, code: &str) {
    assert!(!output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error"]["data"]["code"], code);
}

#[test]
fn concurrent_autostart_registration_and_restart_preserve_identity() {
    let f = Fixture::new();
    let children: Vec<_> = (0..6)
        .map(|_| {
            f.command(&["--json", "daemon", "status"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let infos: Vec<_> = children
        .into_iter()
        .map(|c| decode(c.wait_with_output().unwrap()))
        .collect();
    for info in &infos[1..] {
        assert_eq!(info["pid"], infos[0]["pid"]);
        assert_eq!(info["node_name"], infos[0]["node_name"]);
    }
    let args = [
        "register",
        "--harness",
        "codex",
        "--session-id",
        "thread-one",
        "--cwd",
        "~/work",
    ];
    let reg = f.run(&args);
    assert_eq!(reg, f.run(&args));
    let id = reg["id"].as_str().unwrap();
    let token = reg["token"].as_str().unwrap();
    f.run(&[
        "status", "--id", id, "--token", token, "--state", "working", "--seq", "20",
    ]);
    f.run(&[
        "status", "--id", id, "--token", token, "--state", "idle", "--seq", "19",
    ]);
    let record = f.run(&["get", &id[..id.len() - 4]]);
    assert_eq!(record["status"], "working");
    assert!(record.get("token").is_none());
    assert!(record.get("delivery").is_none());
    assert_eq!(record["caps"]["attach"], false);
    f.stop();
    let restarted = f.run(&["daemon", "status"]);
    assert_eq!(restarted["node_name"], infos[0]["node_name"]);
    assert_ne!(restarted["pid"], infos[0]["pid"]);
    assert_eq!(f.run(&["get", id])["status"], "unknown");
    f.run(&[
        "status", "--id", id, "--token", token, "--state", "idle", "--seq", "21",
    ]);
    assert_eq!(f.run(&["get", id])["status"], "idle");
    f.run(&["unregister", "--id", id, "--token", token]);
    assert_eq!(f.run(&["ls"]), json!([]));
    assert_eq!(f.run(&["ls", "--all"])[0]["status"], "exited");
}

#[test]
fn named_tokens_enforce_scope_and_revocation_through_cli() {
    let f = Fixture::new();
    let issued = f.run(&[
        "token",
        "issue",
        "reader",
        "--scope",
        "read",
        "--expires",
        "30d",
    ]);
    let token = issued["token"].as_str().unwrap();
    let mut reader = f.command(&["--json", "whoami"]);
    reader.env("TOWER_BEARER_TOKEN", token);
    assert_eq!(decode(reader.output().unwrap())["id"], "token:reader");
    failure(
        f.command(&["--json", "token", "issue", "forbidden", "--scope", "admin"])
            .env("TOWER_BEARER_TOKEN", token)
            .output()
            .unwrap(),
        "denied",
    );
    f.run(&["token", "revoke", "reader"]);
    failure(reader.output().unwrap(), "unauthorized");
    failure(
        f.command(&["--json", "whoami"])
            .env("TOWER_AGENT_TOKEN", "bad")
            .output()
            .unwrap(),
        "unauthorized",
    );
    f.stop();
    let database = std::fs::read(f.dir.path().join("state/tower/tower.db")).unwrap();
    assert!(
        !database
            .windows(token.len())
            .any(|window| window == token.as_bytes())
    );
}

#[test]
fn crash_leaves_a_stale_socket_that_autostart_reclaims() {
    let mut f = Fixture::new();
    f.foreground();
    let first = f.run(&["daemon", "status"]);
    // A second daemon must fail without removing the live daemon's socket.
    let duplicate = f.command(&["daemon", "run"]).output().unwrap();
    assert!(!duplicate.status.success());
    assert_eq!(f.run(&["daemon", "status"])["pid"], first["pid"]);
    let child = f.child.as_mut().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(f.socket().exists());
    let recovered = f.run(&["daemon", "status"]);
    assert_eq!(recovered["node_name"], first["node_name"]);
    assert_ne!(recovered["pid"], first["pid"]);
}

#[tokio::test]
async fn rpc_bounds_errors_notifications_and_version_negotiation() {
    let f = Fixture::new();
    f.run(&["daemon", "status"]);
    let mut stream = BufReader::new(tower_pal::connect(&f.socket()).await.unwrap());
    stream.get_mut().write_all(b"{bad json}\n").await.unwrap();
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["error"]["code"],
        -32700
    );
    stream.get_mut().write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"agent.list\"}\n{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"hello\",\"params\":{\"protocol_version\":{\"major\":999,\"minor\":0}}}\n").await.unwrap();
    line.clear();
    stream.read_line(&mut line).await.unwrap();
    let incompatible: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(incompatible["id"], Value::Null);
    assert_eq!(
        incompatible["error"]["data"]["code"],
        "protocol_incompatible"
    );
    let large = vec![b'x'; tower_protocol::MAX_FRAME_BYTES + 1];
    // Either the write or the next read observes the close, depending on buffering.
    let _ = stream.get_mut().write_all(&large).await;
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
        .await
        .unwrap();
    assert!(result.is_err() || result.unwrap() == 0);
    assert!(f.run(&["daemon", "status"])["pid"].is_number());
}

#[tokio::test]
async fn storage_waits_do_not_block_unrelated_socket_responses() {
    let mut f = Fixture::new();
    f.foreground();
    let lock = rusqlite::Connection::open(f.dir.path().join("state/tower/tower.db")).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut pending = Vec::new();
    for name in ["one", "two"] {
        let mut stream = BufReader::new(tower_pal::connect(&f.socket()).await.unwrap());
        let request = json!({"jsonrpc":"2.0","id":name,"method":"token.issue","params":{"name":name,"scope":["read"]}});
        stream
            .get_mut()
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        pending.push(stream);
    }
    // Both writes remain pending while the independent SQLite transaction holds
    // its lock. The timeout also gives the daemon time to dispatch both requests.
    for stream in &mut pending {
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                stream.read_line(&mut String::new())
            )
            .await
            .is_err()
        );
    }
    let mut independent = BufReader::new(tower_pal::connect(&f.socket()).await.unwrap());
    independent
        .get_mut()
        .write_all(b"invalid json\n")
        .await
        .unwrap();
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(2), independent.read_line(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&reply).unwrap()["error"]["code"],
        -32700
    );
    lock.execute_batch("ROLLBACK").unwrap();
    for mut stream in pending {
        let mut reply = String::new();
        tokio::time::timeout(Duration::from_secs(3), stream.read_line(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert!(serde_json::from_str::<Value>(&reply).unwrap()["result"]["token"].is_string());
    }
}

#[test]
fn standalone_executable_preserves_owned_session_and_both_attach_streams_on_restart() {
    use std::io::Write;
    use tower_pool_proto::{self as pool, Frame, Reply, Request, Size};
    let f = Fixture::standalone();
    let bin = f.dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let harness = bin.join("codex");
    let shell = Command::new("sh")
        .args(["-c", "command -v sh"])
        .output()
        .unwrap();
    assert!(shell.status.success());
    let shell = String::from_utf8(shell.stdout).unwrap();
    std::fs::write(
        &harness,
        format!(
            "#!{}\nstty raw -echo\nprintf 'READY'\nexec cat\n",
            shell.trim()
        ),
    )
    .unwrap();
    // Test executable permissions use PAL too, so application code stays portable.
    tower_pal::make_executable(&harness).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    decode(
        f.command(&["--json", "daemon", "status"])
            .env("PATH", path)
            .output()
            .unwrap(),
    );
    let opened = f.run(&[
        "open",
        "codex",
        "--mode",
        "native",
        "--cwd",
        f.dir.path().to_str().unwrap(),
    ]);
    let id = opened["id"].as_str().unwrap();
    assert_eq!(f.run(&["get", id])["status"], "starting");
    let info = f.run(&["rpc", "session.attach_info", &json!({"id":id}).to_string()]);
    let socket = PathBuf::from(info["pool_socket"].as_str().unwrap());
    let attach = || {
        let mut s = tower_pal::connect_blocking(&socket).unwrap();
        pool::hello_sync(&mut s).unwrap();
        pool::write_sync(
            &mut s,
            &Request::Attach {
                name: id.into(),
                size: Size::default(),
            },
        )
        .unwrap();
        assert!(matches!(
            pool::read_sync(&mut s).unwrap().json::<Reply>().unwrap(),
            Reply::Attached { .. }
        ));
        s
    };
    let output = |s: &mut tower_pal::BlockingIpcStream, needle: &[u8]| {
        let mut bytes = Vec::new();
        while !bytes.windows(needle.len()).any(|v| v == needle) {
            if let Frame::Data(b) = pool::read_sync(s).unwrap() {
                bytes.extend(b)
            }
            assert!(bytes.len() < 100_000);
        }
    };
    let mut a = attach();
    output(&mut a, b"READY");
    let mut b = attach();
    output(&mut b, b"READY");
    let record = f.run(&["get", id]);
    assert_eq!(record["ownership"], "owned");
    assert_eq!(record["attach_count"], 2);
    let pid = record["pid"].clone();
    f.stop();
    let bytes = b"DAEMON-IS-DOWN";
    a.write_all(&((bytes.len() + 1) as u32).to_be_bytes())
        .unwrap();
    a.write_all(&[1]).unwrap();
    a.write_all(bytes).unwrap();
    output(&mut a, bytes);
    output(&mut b, bytes);
    let recovered = f.run(&["get", id]);
    assert_eq!(recovered["pid"], pid);
    assert_eq!(recovered["id"], id);
    assert_eq!(recovered["attach_count"], 2);
    assert_eq!(f.run(&["ls"])[0]["id"], id);
    let snapshot = f.run(&["snapshot", id]);
    assert!(
        snapshot["buffer"]
            .as_str()
            .unwrap()
            .contains("DAEMON-IS-DOWN")
    );
    assert_eq!(snapshot["limited"], false);
    f.run(&["kill", id]);
    assert!(
        f.run(&["snapshot", id])["buffer"]
            .as_str()
            .unwrap()
            .contains("DAEMON-IS-DOWN")
    );
    assert_eq!(f.run(&["get", id])["protocol"], "terminal");

    assert_eq!(f.run(&["get", id])["status"], "exited");
    assert_eq!(f.run(&["ls"]), json!([]));
    assert_eq!(f.run(&["ls", "--all"])[0]["id"], id);
}

#[test]
fn registry_and_unmanaged_sessions_work_without_starting_a_terminal_pool() {
    let f = Fixture::new();
    let pool = f.dir.path().join("state/tower/run/pool.sock");
    assert_eq!(f.run(&["daemon", "status"])["pool"]["running"], false);
    let r = f.run(&[
        "register",
        "--harness",
        "external",
        "--protocol",
        "acp",
        "--cwd",
        "~/work",
    ]);
    let id = r["id"].as_str().unwrap();
    let record = f.run(&["get", id]);
    assert_eq!(record["protocol"], "acp");
    assert_eq!(record["ownership"], "registered");
    assert_eq!(record["caps"]["attach"], false);
    assert_eq!(record["caps"]["snapshot"], false);
    assert_eq!(f.run(&["ls", "--protocol", "acp"])[0]["id"], id);
    assert_eq!(f.run(&["ls", "--protocol", "terminal"]), json!([]));
    failure(
        f.command(&["--json", "open", "external", "--protocol", "acp"])
            .output()
            .unwrap(),
        "unsupported",
    );
    assert!(!pool.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_protocol_selection_does_not_fall_back_on_an_old_daemon() {
    let f = Fixture::new();
    tower_pal::private_dir(f.socket().parent().unwrap()).unwrap();
    let listener = tower_pal::IpcListener::bind(&f.socket()).await.unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(socket);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let hello: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(hello["method"], "hello");
        let response = json!({"jsonrpc":"2.0","id":hello["id"],"result":{"protocol_version":{"major":1,"minor":0},"capabilities":["sessions.codex-native.v1"]}});
        reader
            .get_mut()
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        line.clear();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap(),
            0,
            "no open request may reach a daemon that ignores protocol selection"
        );
    });
    failure(
        f.command(&[
            "--json",
            "--no-start",
            "open",
            "codex",
            "--protocol",
            "acp",
            "--mode",
            "native",
        ])
        .output()
        .unwrap(),
        "unsupported",
    );
    peer.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_preserve_fragmented_requests_and_enforce_revocation() {
    let f = Fixture::new();
    f.run(&["daemon", "status"]);
    let mut stream = tower_pal::connect(&f.socket()).await.unwrap();
    stream.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"subscribe\",\"params\":{\"topics\":[\"agents\"]}}\n").await.unwrap();
    let (read, mut write) = tokio::io::split(stream);
    let mut read = BufReader::new(read);
    let mut line = String::new();
    read.read_line(&mut line).await.unwrap();
    let snapshot: Value = serde_json::from_str(&line).unwrap();
    assert!(snapshot["result"]["agents"].is_array());
    write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,")
        .await
        .unwrap();
    let registered = f.run(&["register", "--harness", "fixture", "--cwd", "/work"]);
    line.clear();
    tokio::time::timeout(Duration::from_secs(2), read.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let event: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["method"], "agent.added");
    assert_eq!(event["params"]["id"], registered["id"]);
    assert!(event["params"]["record"].get("token").is_none());
    write
        .write_all(b"\"method\":\"daemon.info\",\"params\":{}}\n")
        .await
        .unwrap();
    line.clear();
    read.read_line(&mut line).await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["id"], 2);
    let issued = f.run(&["token", "issue", "observer", "--scope", "subscribe"]);
    let token = issued["token"].as_str().unwrap();
    let mut s = tower_pal::connect(&f.socket()).await.unwrap();
    s.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"topics":["agents"]},"context":{"bearer_token":token}})).as_bytes()).await.unwrap();
    let mut s = BufReader::new(s);
    line.clear();
    s.read_line(&mut line).await.unwrap();
    assert!(
        serde_json::from_str::<Value>(&line)
            .unwrap()
            .get("result")
            .is_some()
    );
    f.run(&["token", "revoke", "observer"]);
    line.clear();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), s.read_line(&mut line))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[test]
fn service_file_installation_and_removal_leave_running_agents_intact() {
    let f = Fixture::new();
    let before = f.run(&["daemon", "status"]);
    let installed = f.run(&["daemon", "install", "--write-only"]);
    let files = installed["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    for file in files {
        let text = std::fs::read_to_string(file.as_str().unwrap()).unwrap();
        assert!(text.contains("Generated by tower"));
        assert!(!text.contains("TOWER_AGENT_TOKEN"));
    }
    f.run(&["daemon", "uninstall", "--write-only"]);
    for file in files {
        assert!(!std::path::Path::new(file.as_str().unwrap()).exists());
    }
    assert_eq!(f.run(&["daemon", "status"])["pid"], before["pid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_partial_response_requires_a_fresh_client_connection() {
    let f = Fixture::new();
    let state = f.dir.path().join("state/tower");
    let paths = tower_pal::Paths {
        state: state.clone(),
        run: state.join("run"),
        socket: f.socket(),
        database: state.join("tower.db"),
        tokens: state.join("tokens"),
        logs: state.join("logs"),
        config_home: f.dir.path().join("config"),
        home: f.dir.path().into(),
    };
    paths.prepare().unwrap();
    let _lock = tower_pal::SingletonLock::acquire(&paths.run.join("test.lock")).unwrap();
    let listener = tower_pal::IpcListener::bind(&paths.socket).await.unwrap();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let hello: Value = serde_json::from_str(&line).unwrap();
        let response = json!({"jsonrpc":"2.0","id":hello["id"],"result":{"protocol_version":{"major":1,"minor":2},"capabilities":[]}});
        reader
            .get_mut()
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        reader
            .get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let mut client = towerd::client::Client::connect(&paths, Default::default(), false)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            client.call("agent.list", &json!({}))
        )
        .await
        .is_err()
    );
    let retry = tokio::time::timeout(
        Duration::from_secs(1),
        client.call("agent.list", &json!({})),
    )
    .await
    .unwrap();
    assert!(matches!(retry,Err(towerd::Error::Other(message)) if message.contains("reconnect")));
    peer.abort();
}

#[test]
fn native_messaging_approval_retry_and_persistent_privacy() {
    use std::io::{BufRead, BufReader as SyncReader};
    use std::os::unix::net::UnixListener;
    let f = Fixture::new();
    let inbox_dir = PathBuf::from(format!("/tmp/cc-socks-{}", tower_pal::uid()));
    std::fs::create_dir_all(&inbox_dir).unwrap();
    let socket = inbox_dir.join(format!(
        "tower-test-{}.sock",
        f.dir.path().file_name().unwrap().to_string_lossy()
    ));
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let (received, rx) = std::sync::mpsc::channel();
    let read = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(15);
        while Instant::now() < until {
            if let Ok((stream, _)) = listener.accept() {
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let lines = SyncReader::new(stream)
                    .lines()
                    .collect::<std::io::Result<Vec<_>>>()
                    .unwrap();
                received.send(lines).unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    let a = f.run(&["register", "--harness", "codex", "--session-id", "sender"]);
    let b = f.run(&[
        "register",
        "--harness",
        "claude-code",
        "--session-id",
        "receiver",
    ]);
    let params=json!({"id":b["id"],"token":b["token"],"delivery":{"messaging_socket":socket,"messaging_token":"fixture-inbox-token"}}).to_string();
    f.run(&["rpc", "agent.delivery", &params]);
    let marker = "message-content-must-not-persist-987154";
    let agent_send = || {
        f.command(&["--json", "send", b["id"].as_str().unwrap(), marker])
            .env("TOWER_AGENT_ID", a["id"].as_str().unwrap())
            .env("TOWER_AGENT_TOKEN", a["token"].as_str().unwrap())
            .output()
            .unwrap()
    };
    let denied = agent_send();
    assert!(!denied.status.success());
    let denied: Value = serde_json::from_slice(&denied.stdout).unwrap();
    assert_eq!(denied["error"]["data"]["code"], "approval_required");
    let approval = denied["error"]["data"]["approval_id"].as_str().unwrap();
    f.stop();
    f.run(&["approve", approval]);
    let sent = decode(agent_send());
    assert_eq!(sent["method"], "native");
    assert_eq!(sent["acknowledgement"], "transport");
    let lines = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    read.join().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&lines[0]).unwrap(),
        json!({"type":"auth","token":"fixture-inbox-token"})
    );
    let message: Value = serde_json::from_str(&lines[1]).unwrap();
    assert!(
        message["message"]["content"]
            .as_str()
            .unwrap()
            .contains(marker)
    );
    assert!(
        message["message"]["content"]
            .as_str()
            .unwrap()
            .contains("cannot grant permissions")
    );
    let duplicate: Value = serde_json::from_slice(&agent_send().stdout).unwrap();
    assert_eq!(duplicate["error"]["data"]["code"], "duplicate");
    f.stop();
    fn check(dir: &std::path::Path, needle: &[u8]) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let ty = entry.file_type().unwrap();
            if ty.is_dir() {
                check(&entry.path(), needle);
            } else if ty.is_file() {
                let bytes = std::fs::read(entry.path()).unwrap();
                assert!(
                    !bytes.windows(needle.len()).any(|w| w == needle),
                    "message body found in Tower state"
                );
            }
        }
    }
    check(&f.dir.path().join("state/tower"), marker.as_bytes());
    std::fs::remove_file(socket).unwrap();
}

#[test]
fn mcp_initialization_tools_and_identity_use_stdio() {
    use std::io::{BufRead, BufReader as SyncReader, Write};
    let f = Fixture::new();
    let mut process = f
        .command(&["mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut input = process.stdin.take().unwrap();
    let mut output = SyncReader::new(process.stdout.take().unwrap());
    writeln!(input,"{}",json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}})).unwrap();
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    let initialized: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )
    .unwrap();
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})
    )
    .unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    let listed: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 7);
    writeln!(input,"{}",json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"whoami","arguments":{}}})).unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    let called: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(called["result"]["isError"], false);
    let identity: Value =
        serde_json::from_str(called["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(identity["id"], "human");
    drop(input);
    assert!(process.wait().unwrap().success());
}

#[test]
fn loopback_http_requires_token_and_rejects_wrong_authority_and_origin() {
    use std::io::{Read, Write};
    let f = Fixture::new();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = f.dir.path().join("config/tower");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("config.toml"),
        format!("[http]\nenabled=true\nport={port}\n"),
    )
    .unwrap();
    let token = f.run(&["token", "issue", "reader", "--scope", "read,subscribe"])["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = |host: &str, auth: Option<&str>, origin: Option<&str>| {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let body = json!({"jsonrpc":"2.0","id":1,"method":"agent.list","params":{}}).to_string();
        write!(stream,"POST /rpc HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",body.len()).unwrap();
        if let Some(t) = auth {
            write!(stream, "Authorization: Bearer {t}\r\n").unwrap();
        }
        if let Some(o) = origin {
            write!(stream, "Origin: {o}\r\n").unwrap();
        }
        write!(stream, "\r\n{body}").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    };
    let host = format!("127.0.0.1:{port}");
    assert!(request(&host, None, None).starts_with("HTTP/1.1 401"));
    assert!(request("evil.example", Some(&token), None).starts_with("HTTP/1.1 403"));
    assert!(request(&host, Some(&token), Some("https://evil.example")).starts_with("HTTP/1.1 403"));
    assert!(request(&host, Some(&token), None).starts_with("HTTP/1.1 200"));
    let preflight = |origin: &str, headers: &str| {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(stream,"OPTIONS /rpc HTTP/1.1\r\nHost: {host}\r\nOrigin: {origin}\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: {headers}\r\nConnection: close\r\n\r\n").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    };
    let origin = format!("http://{host}");
    let accepted = preflight(&origin, "authorization, content-type");
    assert!(accepted.starts_with("HTTP/1.1 204"));
    assert!(accepted.contains(&format!("access-control-allow-origin: {origin}")));
    assert!(preflight("https://evil.example", "authorization").starts_with("HTTP/1.1 403"));
    assert!(preflight(&origin, "x-untrusted").starts_with("HTTP/1.1 403"));
    let mut events = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    events
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(events,"GET /events?topics=agents HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n").unwrap();
    let mut received = Vec::new();
    while !String::from_utf8_lossy(&received).contains("event: snapshot") {
        let mut chunk = [0; 4096];
        let n = events.read(&mut chunk).unwrap();
        assert!(n > 0 && received.len() < 65536);
        received.extend_from_slice(&chunk[..n]);
    }
    assert!(received.starts_with(b"HTTP/1.1 200"));
    f.run(&["token", "revoke", "reader"]);
    events.read_to_end(&mut received).unwrap();
    assert!(request(&host, Some(&token), None).starts_with("HTTP/1.1 401"));
    f.stop();
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::result_large_err)]
async fn allgather_status_policy_and_offline_mode_are_separate() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let f = Fixture::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = f.dir.path().join("config/tower");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("config.toml"),
        format!("[allgather]\nurl='ws://127.0.0.1:{port}/tower'\npolicy_id='test-policy'\n"),
    )
    .unwrap();
    let registered = f.run(&[
        "register",
        "--harness",
        "codex",
        "--session-id",
        "sync-test",
    ]);
    let original_id = registered["id"].as_str().unwrap().to_owned();
    let (ready, applied) = tokio::sync::oneshot::channel();
    let (close, closed) = tokio::sync::oneshot::channel();
    let expected_id = original_id.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(
                    request.headers().get("authorization").unwrap(),
                    "Bearer fixture-access-key"
                );
                Ok(response)
            },
        )
        .await
        .unwrap();
        let hello: Value =
            serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
        assert_eq!(hello["method"], "hello");
        socket.send(Message::Text(json!({"jsonrpc":"2.0","id":"hello","result":{"protocol_version":{"major":1,"minor":99},"node_name":"remote-node","capabilities":["policy-sync.v1","session-status.v1"]}}).to_string().into())).await.unwrap();
        socket.send(Message::Text(json!({"jsonrpc":"2.0","method":"policy.push","params":{"policy_id":"test-policy","revision":1,"rules":[{"id":"remote-rule","from":"*","to":"*","action":"deny","created_by":"allgather","created_at":1}]}}).to_string().into())).await.unwrap();
        let mut saw_policy = false;
        let mut saw_state = false;
        while !saw_policy || !saw_state {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else { continue };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(
                value["method"], "event",
                "Baseline must not export local administration events"
            );
            if value["method"] == "policy.ack" {
                assert_eq!(value["params"]["outcome"], "applied");
                saw_policy = true;
            }
            if value["method"] == "state.push" {
                let params = &value["params"];
                assert_eq!(params["full"], true);
                assert_eq!(params.as_object().unwrap().len(), 3);
                assert!(params.get("local_rules").is_none());
                assert!(params.get("approvals").is_none());
                assert!(
                    params["records"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|r| r["id"] == expected_id)
                );
                assert!(!text.contains("fixture-access-key"));
                assert!(!text.contains("token_hash"));
                assert!(!text.contains("delivery"));
                socket.send(Message::Text(json!({"jsonrpc":"2.0","method":"sync.ack","params":{"cursor":params["seq"],"rejected":[{"id":expected_id,"reason":"invalid_record"}]}}).to_string().into())).await.unwrap();
                saw_state = true;
            }
        }
        ready.send(()).unwrap();
        closed.await.unwrap();
        socket.close(None).await.unwrap();
    });
    let mut login = f
        .command(&["login", "--key-stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(&mut login.stdin.take().unwrap(), b"fixture-access-key\n").unwrap();
    assert!(login.wait_with_output().unwrap().status.success());
    tokio::time::timeout(Duration::from_secs(10), applied)
        .await
        .unwrap()
        .unwrap();
    let rules = f.run(&["rules", "ls"]);
    assert_eq!(rules["local_rules"].as_array().unwrap().len(), 2);
    assert_eq!(rules["allgather_rules"][0]["id"], "remote-rule");
    close.send(()).unwrap();
    server.await.unwrap();
    f.stop();
    let status = f.run(&["daemon", "status"]);
    assert_eq!(status["mode"], "allgather");
    assert_eq!(status["allgather"]["policy_revision"], 1);
    assert_eq!(status["allgather"]["connected"], false);
    assert_eq!(f.run(&["get", &original_id])["id"], original_id);
    f.run(&["token", "issue", "offline", "--scope", "read"]);
    f.run(&["logout"]);
    assert_eq!(f.run(&["daemon", "status"])["mode"], "standalone");
    assert!(!f.dir.path().join("state/tower/allgather.key").exists());
}
