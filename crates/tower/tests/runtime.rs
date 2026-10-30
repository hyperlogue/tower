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
        }
    }
    fn socket(&self) -> PathBuf {
        self.dir.path().join("state/tower/run/towerd.sock")
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower"));
        c.args(args)
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
    assert_eq!(record["caps"]["message"], "none");
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
