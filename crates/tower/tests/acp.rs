#[path = "support/cleanup.rs"]
mod cleanup;
use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
        let agent = root.path().join("agent.mjs");
        std::fs::write(&agent, include_str!("fixtures/acp-agent.mjs")).unwrap();
        let node = tower_pal::find_program("node", &tower_pal::environment())
            .expect("Node fixture runtime");
        let argv =
            serde_json::to_string(&[node.to_string_lossy(), agent.to_string_lossy()]).unwrap();
        std::fs::write(
            config.join("config.toml"),
            format!("[scan]\nenabled=false\n[acp.agents]\nfixture={argv}\n"),
        )
        .unwrap();
        Self { root }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower-cli"));
        c.args(args)
            .env_clear()
            .env("HOME", self.root.path())
            .env("PATH", std::env::var("PATH").unwrap())
            .env("XDG_STATE_HOME", self.root.path().join("state"))
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("XDG_CACHE_HOME", self.root.path().join("cache"))
            .env("TOKIO_WORKER_THREADS", "2")
            .current_dir(self.root.path());
        c
    }
    fn rpc_result(&self, method: &str, params: Value) -> std::process::Output {
        self.command(&["--json", "rpc", method, &params.to_string()])
            .output()
            .unwrap()
    }
    fn rpc(&self, method: &str, params: Value) -> Value {
        let out = self.rpc_result(method, params);
        assert!(
            out.status.success(),
            "{method}: {} {}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }
    async fn status(&self, id: &str, wanted: &str) {
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let rows = self.rpc("agent.list", json!({}));
                if rows.as_array().is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row["id"] == id && row["status"] == wanted)
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("awaiting ACP observed status {wanted}"));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self
            .command(&["daemon", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        cleanup::finish(&mut self.root, "state", None);
    }
}
async fn line(reader: &mut BufReader<tokio::process::ChildStdout>) -> String {
    let mut line = String::new();
    let n = tokio::time::timeout(Duration::from_secs(45), reader.read_line(&mut line))
        .await
        .expect("awaiting forwarded ACP output")
        .unwrap();
    assert!(n > 0, "ACP stream ended");
    assert_eq!(line.pop(), Some('\n'));
    line
}
#[tokio::test]
async fn native_stdio_is_transparent_in_both_directions_and_status_is_observational() {
    let f = Fixture::new();
    let mut command = f.command(&["acp", "fixture"]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = tokio::process::Command::from(command)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    input.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{\"protocolVersion\":1,\"clientCapabilities\":{\"future\":true}}}\n").await.unwrap();
    let initialized: Value = serde_json::from_str(&line(&mut output).await).unwrap();
    assert_eq!(initialized["result"]["vendorCapabilities"]["future"], true);
    let opaque = r#"{ "jsonrpc":"2.0","method":"vendor/future","params":{"large":123456789012345678901234567890,"shape":[null,true,{"custom":"preserved"}]}}"#;
    input
        .write_all(format!("{opaque}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(line(&mut output).await, opaque);
    let large =
        json!({"jsonrpc":"2.0","method":"vendor/future","params":{"text":"\\".repeat(300_000)}})
            .to_string();
    input
        .write_all(format!("{large}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(line(&mut output).await, large);

    input.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\",\"params\":{\"cwd\":\"/work\",\"mcpServers\":[]}}\n").await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line(&mut output).await).unwrap()["result"]["sessionId"],
        "fixture-session"
    );
    input.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"session/prompt\",\"params\":{\"sessionId\":\"fixture-session\",\"prompt\":[{\"type\":\"text\",\"text\":\"question\"}]}}\n").await.unwrap();
    let question: Value = serde_json::from_str(&line(&mut output).await).unwrap();
    assert_eq!(
        question["params"]["requestedSchema"]["$ref"],
        "vendor:future-schema"
    );
    assert_eq!(question["params"]["vendorField"]["keep"], true);
    let rows = f.rpc("agent.list", json!({}));
    let id = rows[0]["id"].as_str().unwrap();
    f.status(id, "waiting_input").await;
    let answer = json!({"jsonrpc":"2.0","id":"agent-question","result":{"vendorReply":[1,"arbitrary",null]}});
    input
        .write_all(format!("{answer}\n").as_bytes())
        .await
        .unwrap();
    let ended: Value = serde_json::from_str(&line(&mut output).await).unwrap();
    assert_eq!(ended["result"]["responseSeen"], answer["result"]);
    f.status(id, "idle").await;
    let db = std::fs::read(f.root.path().join("state/tower/tower.db")).unwrap();
    assert!(
        !db.windows(b"PRIVATE-FRAME-MARKER".len())
            .any(|v| v == b"PRIVATE-FRAME-MARKER")
    );
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(45), child.wait())
            .await
            .expect("awaiting ACP proxy close")
            .unwrap()
            .success()
    );
}
#[tokio::test]
async fn proxy_sequences_are_idempotent_and_stale_generations_are_refused() {
    let f = Fixture::new();
    let opened = f.rpc("acp.open", json!({"harness":"fixture","cwd":f.root.path()}));
    let mut frame = json!({"runtime_id":opened["runtime_id"],"generation":opened["generation"],"sequence":1,"frame":r#"{"jsonrpc":"2.0","method":"vendor/future","params":{"unchanged":true}}"#});
    f.rpc("acp.send", frame.clone());
    f.rpc("acp.send", frame.clone());
    frame["frame"] = json!("{}");
    assert!(!f.rpc_result("acp.send", frame.clone()).status.success());
    frame["generation"] = json!("stale");
    assert!(!f.rpc_result("acp.send", frame).status.success());
    let poll = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let poll = f.rpc("acp.poll", opened.clone());
            if poll["frames"].as_array().is_some_and(|v| !v.is_empty()) {
                break poll;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("awaiting ACP output readiness");
    assert_eq!(poll["frames"][0]["sequence"], 1);
    assert_eq!(f.rpc("acp.poll", opened.clone())["frames"], poll["frames"]);
    let mut ack = opened.clone();
    ack["sequence"] = json!(1);
    f.rpc("acp.ack", ack);
    assert!(
        f.rpc("acp.poll", opened.clone())["frames"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    f.rpc("acp.close", opened.clone());
    let mut old = opened;
    old["sequence"] = json!(2);
    old["frame"] = json!("{}");
    assert!(!f.rpc_result("acp.send", old).status.success());
}

#[tokio::test]
async fn final_native_output_is_forwarded_before_process_exit_closes_the_proxy() {
    let f = Fixture::new();
    let mut command = f.command(&["acp", "fixture"]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = tokio::process::Command::from(command)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let frame = r#"{"jsonrpc":"2.0","method":"vendor/exit","params":{"final":true}}"#;
    input
        .write_all(format!("{frame}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(line(&mut output).await, frame);
    assert!(
        tokio::time::timeout(Duration::from_secs(45), child.wait())
            .await
            .expect("awaiting native EOF close")
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn an_open_connection_can_be_closed_without_initializing_an_agent_session() {
    let f = Fixture::new();
    let opened = f.rpc("acp.open", json!({"harness":"fixture","cwd":f.root.path()}));
    assert_eq!(f.rpc("acp.close", opened.clone())["closed"], true);
    assert_eq!(f.rpc("acp.close", opened)["closed"], true);
}
