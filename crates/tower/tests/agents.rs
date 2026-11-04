use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Fixture {
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-ag-")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::create_dir(root.path().join("bin")).unwrap();
        for bin in [
            "claude", "codex", "reasonix", "kimi", "grok", "opencode", "pi", "agy", "omp",
        ] {
            let path = root.path().join("bin").join(bin);
            let script = if bin == "codex" {
                let node = tower_pal::find_program("node", &tower_pal::environment())
                    .expect("Node is required for Codex fixtures");
                include_str!("fixtures/codex-host.mjs").replacen(
                    "#!/usr/bin/env node",
                    &format!("#!{}", node.display()),
                    1,
                )
            } else {
                FAKE.into()
            };
            std::fs::write(&path, script).unwrap();
            tower_pal::make_executable(&path).unwrap();
        }
        Self { root }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower"));
        c.args(args)
            .env_clear()
            .env("HOME", self.root.path())
            .env("XDG_STATE_HOME", self.root.path().join("s"))
            .env("XDG_CONFIG_HOME", self.root.path().join("c"))
            .env("XDG_CACHE_HOME", self.root.path().join("cache"))
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.root.path().join("bin").display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("TOWER_TEST_BIN", env!("CARGO_BIN_EXE_tower"))
            .env("TOKIO_WORKER_THREADS", "2")
            .env("TERM", "xterm-256color")
            .current_dir(self.root.path());
        c
    }
    fn call(&self, args: &[&str]) -> Value {
        let out = self.command(&["--json"]).args(args).output().unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }
    fn wait(&self, id: &str, status: &str) -> Value {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let row = self.call(&["get", id]);
            if row["status"] == status {
                return row;
            }
            assert!(
                Instant::now() < until,
                "expected {status}: {row}; snapshot {}",
                self.call(&["snapshot", id])
            );
            thread::sleep(Duration::from_millis(40));
        }
    }
    fn phase(&self, phase: &str) {
        std::fs::write(self.root.path().join("phase"), phase).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for args in [["__pool", "stop"], ["__codex", "stop"], ["daemon", "stop"]] {
            let _ = self
                .command(&["--no-start"])
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}
const FAKE: &str = r#"#!/bin/sh
if [ "$1" = queue ] && [ "$2" = --help ]; then printf '%s\n' '--thread --message'; exit 0; fi
name=${0##*/}
case $name in claude) harness=claude-code;; agy) harness=antigravity;; *) harness=$name;; esac
printf '%s\n' "$@" > "$name.args"
printf '%s\n' "$TOWER_AGENT_ID" >> "$name.starts"
case $name in
  grok) payload='{"sessionId":"native-grok","modelId":"fixture-model"}';;
  agy) payload='{"conversationId":"native-antigravity","modelName":"fixture-model"}';;
  *) payload=$(printf '{"session_id":"native-%s","model":"fixture-model","root_confirmed":true}' "$harness");;
esac
report() { printf '%s' "$payload" | "$TOWER_TEST_BIN" hook "$harness" "$1"; }
report SessionStart
last=
while :; do
  phase=$(cat phase 2>/dev/null || :)
  if [ "$phase" != "$last" ]; then
    case $phase in work) report UserPromptSubmit;; approval) report PermissionRequest;; idle) report Stop;; quit) exit 0;; esac
    last=$phase
  fi
  sleep 0.03
done
"#;

#[test]
fn all_nine_harnesses_report_state_survive_daemon_restart_and_resume() {
    let f = Fixture::new();
    let mut ids = Vec::new();
    for h in [
        "claude", "codex", "reasonix", "kimi", "grok", "opencode", "pi", "agy", "omp",
    ] {
        let args = vec!["open", h, "--headless"];
        let opened = f.call(&args);
        let id = opened["id"].as_str().unwrap().to_owned();
        let row = f.wait(&id, "idle");
        assert_eq!(row["status_detail"]["model"], "fixture-model");
        assert!(
            row["native_session_id"]
                .as_str()
                .unwrap()
                .starts_with("native-")
        );
        ids.push((id, row["pid"].clone(), row["native_session_id"].clone()));
    }
    f.phase("work");
    for (id, _, _) in &ids {
        f.wait(id, "working");
    }
    f.call(&["daemon", "stop"]);
    f.phase("approval");
    thread::sleep(Duration::from_millis(200));
    for (id, pid, native) in &ids {
        let row = f.wait(id, "waiting_approval");
        assert_eq!(&row["pid"], pid);
        assert_eq!(&row["native_session_id"], native);
    }
    f.phase("idle");
    for (id, _, _) in &ids {
        f.wait(id, "idle");
    }
    for (id, pid, native) in &ids {
        f.call(&["restart", id]);
        let row = f.wait(id, "idle");
        if row["harness"] == "codex" {
            assert_eq!(&row["pid"], pid);
        } else {
            assert_ne!(&row["pid"], pid);
        }
        assert_eq!(&row["native_session_id"], native);
        f.call(&["kill", id]);
        f.wait(id, "exited");
        f.call(&["resume", id]);
        let row = f.wait(id, "idle");
        assert_eq!(&row["native_session_id"], native);
        f.call(&["kill", id]);
    }
    f.call(&["daemon", "stop"]);
    let all = f.call(&["ls", "--all"]);
    assert_eq!(all.as_array().unwrap().len(), 9);
    assert!(
        all.as_array()
            .unwrap()
            .iter()
            .all(|r| r["status"] == "exited")
    );
}

#[test]
fn bad_hook_credentials_and_child_reports_do_not_change_the_root() {
    let f = Fixture::new();
    let opened = f.call(&["open", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    f.wait(id, "idle");
    let root = f.root.path().join("s/tower/sessions").join(id);
    let generation = std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let plan: Value =
        serde_json::from_slice(&std::fs::read(generation.join("plan.json")).unwrap()).unwrap();
    let token = std::fs::read_to_string(f.root.path().join("s/tower/tokens").join(id)).unwrap();
    for (credential, payload) in [
        ("wrong", json!({"session_id":"native-claude-code"})),
        (
            token.as_str(),
            json!({"session_id":"child","subagent_type":"Explore"}),
        ),
    ] {
        let mut child = f
            .command(&["hook", "claude", "UserPromptSubmit"])
            .env(
                "TOWER_LAUNCHER_SOCKET",
                plan["control_socket"].as_str().unwrap(),
            )
            .env("TOWER_AGENT_TOKEN", credential)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.to_string().as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(f.call(&["get", id])["status"], "idle");
    }
    assert!(
        !std::fs::read_to_string(generation.join("plan.json"))
            .unwrap()
            .contains(&token)
    );
    f.call(&["kill", id]);
}

#[test]
fn an_unapproved_project_environment_cannot_run_the_agent() {
    let f = Fixture::new();
    let direnv = f.root.path().join("bin/direnv");
    std::fs::write(
        &direnv,
        "#!/bin/sh\nprintf '%s' '{\"state\":{\"foundRC\":{\"allowed\":1}}}'\n",
    )
    .unwrap();
    tower_pal::make_executable(&direnv).unwrap();
    std::fs::write(f.root.path().join(".envrc"), "export SOMETHING=anything\n").unwrap();
    let opened = f.call(&["open", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let row = f.wait(id, "exited");
    assert_eq!(row["status_detail"]["error"], "environment_not_approved");
    assert!(!f.root.path().join("claude.starts").exists());
}

#[test]
fn pool_crash_recovers_native_conversations_but_never_reopens_killed_sessions() {
    let f = Fixture::new();
    let opened = f.call(&["open", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let before = f.wait(id, "idle");
    let root = f.root.path().join("s/tower/sessions").join(id);
    let generation = std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let state: Value =
        serde_json::from_slice(&std::fs::read(generation.join("state.json")).unwrap()).unwrap();
    let launcher = state["launcher_pid"].as_u64().unwrap() as u32;
    let pool = tower_pal::process_identity(launcher).unwrap().parent;
    f.call(&["daemon", "stop"]);
    assert!(
        Command::new("kill")
            .args(["-KILL", &pool.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let until = Instant::now() + Duration::from_secs(5);
    while tower_pal::process_identity(launcher).is_ok() {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(20));
    }
    let recovered = f.wait(id, "idle");
    assert_ne!(recovered["pid"], before["pid"]);
    assert_eq!(recovered["native_session_id"], before["native_session_id"]);
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("claude.starts"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert!(
        std::fs::read_to_string(f.root.path().join("claude.args"))
            .unwrap()
            .contains("--resume\nnative-claude-code")
    );
    f.call(&["kill", id]);
    assert!(f.command(&["__pool", "stop"]).status().unwrap().success());
    f.call(&["daemon", "stop"]);
    assert_eq!(f.call(&["get", id])["status"], "exited");
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("claude.starts"))
            .unwrap()
            .lines()
            .count(),
        2
    );
}

#[test]
fn missing_native_identity_fails_recovery_without_creating_a_new_conversation() {
    let f = Fixture::new();
    let program = f.root.path().join("bin/claude");
    std::fs::write(
        &program,
        "#!/bin/sh\nprintf started >> starts\nwhile :; do sleep 0.1; done\n",
    )
    .unwrap();
    let opened = f.call(&["open", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    f.wait(id, "unknown");
    let root = f.root.path().join("s/tower/sessions").join(id);
    let generation = std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let until = Instant::now() + Duration::from_secs(5);
    while !generation.join("state.json").exists() {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(20));
    }
    let state: Value =
        serde_json::from_slice(&std::fs::read(generation.join("state.json")).unwrap()).unwrap();
    let launcher = state["launcher_pid"].as_u64().unwrap() as u32;
    let pool = tower_pal::process_identity(launcher).unwrap().parent;
    f.call(&["daemon", "stop"]);
    assert!(
        Command::new("kill")
            .args(["-KILL", &pool.to_string()])
            .status()
            .unwrap()
            .success()
    );
    while tower_pal::process_identity(launcher).is_ok() {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(20));
    }
    let ended = f.wait(id, "exited");
    assert_eq!(ended["status_detail"]["error"], "session_resume_failed");
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("starts")).unwrap(),
        "started"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_hook_receiver_cannot_hold_the_harness() {
    let f = Fixture::new();
    tower_pal::private_dir(f.root.path()).unwrap();
    let path = f.root.path().join("hook.sock");
    let _lock = tower_pal::SingletonLock::acquire(&f.root.path().join("hook.lock")).unwrap();
    let listener = tower_pal::IpcListener::bind(&path).await.unwrap();
    let stalled = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let before = Instant::now();
    let mut child = f
        .command(&["hook", "claude", "Stop"])
        .env("TOWER_LAUNCHER_SOCKET", &path)
        .env("TOWER_AGENT_TOKEN", "fixture-token")
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write;
    child.stdin.take().unwrap().write_all(b"{}").unwrap();
    assert!(child.wait().unwrap().success());
    assert!(
        before.elapsed() < Duration::from_secs(2),
        "hook must not wait for the native harness timeout"
    );
    stalled.abort();
}

#[test]
fn native_discovery_reserves_identity_before_start_and_keeps_history_external() {
    let f = Fixture::new();
    let path = f
        .root
        .path()
        .join(".claude/projects/work/native-claude-code.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let history = format!(
        "{{\"type\":\"user\",\"cwd\":{},\"message\":\"PRIVATE-HISTORY\"}}\n",
        serde_json::to_string(f.root.path()).unwrap()
    );
    std::fs::write(&path, &history).unwrap();
    let inventory = f.call(&["agents", "conversations", "--harness", "claude"]);
    assert_eq!(inventory["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(
        f.call(&["ls"]),
        json!([]),
        "discovery must not allocate registry IDs"
    );
    let opened = f.call(&["open", "claude", "--resume", "native-claude-code"]);
    let id = opened["id"].as_str().unwrap();
    assert_eq!(
        f.call(&["get", id])["native_session_id"],
        "native-claude-code"
    );
    let again = f
        .command(&["--json", "open", "claude", "--resume", "native-claude-code"])
        .output()
        .unwrap();
    assert!(!again.status.success());
    let error: Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(error["error"]["data"]["code"], "already_managed");
    assert_eq!(error["error"]["data"]["id"], id);
    let inventory = f.call(&["agents", "conversations", "--harness", "claude"]);
    assert_eq!(inventory["conversations"][0]["tower_id"], id);
    assert!(!inventory.to_string().contains("PRIVATE-HISTORY"));
    f.call(&["kill", id]);
    assert_eq!(std::fs::read_to_string(path).unwrap(), history);
    f.call(&["resume", id]);
    assert_eq!(
        f.wait(id, "idle")["native_session_id"],
        "native-claude-code"
    );
    f.call(&["kill", id]);
}

#[test]
fn native_inventory_does_not_block_registry_requests() {
    let f = Fixture::new();
    let path = f.root.path().join("bin/opencode");
    std::fs::write(
        &path,
        "#!/bin/sh\nprintf ready > \"$HOME/inventory.started\"\nsleep 2\nprintf '[]'\n",
    )
    .unwrap();
    f.call(&["daemon", "status"]);
    let child = f
        .command(&["agents", "conversations", "--harness", "opencode"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !f.root.path().join("inventory.started").exists() {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(10));
    }
    let start = Instant::now();
    f.call(&["daemon", "status"]);
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "inventory must not occupy the mutation worker"
    );
    assert!(child.wait_with_output().unwrap().status.success());
}

#[test]
fn a_project_cannot_redirect_resume_into_another_native_store() {
    let f = Fixture::new();
    let history = f
        .root
        .path()
        .join(".claude/projects/work/native-claude-code.jsonl");
    std::fs::create_dir_all(history.parent().unwrap()).unwrap();
    std::fs::write(
        history,
        format!(
            "{{\"cwd\":{}}}\n",
            serde_json::to_string(f.root.path()).unwrap()
        ),
    )
    .unwrap();
    let direnv = f.root.path().join("bin/direnv");
    std::fs::write(&direnv,"#!/bin/sh\ncase $1 in\nstatus) printf '%s' '{\"state\":{\"foundRC\":{\"allowed\":0}}}';;\nexec) shift 2; export CLAUDE_CONFIG_DIR=\"$HOME/other\"; exec \"$@\";;\nesac\n").unwrap();
    tower_pal::make_executable(&direnv).unwrap();
    std::fs::write(
        f.root.path().join(".envrc"),
        "export CLAUDE_CONFIG_DIR=other\n",
    )
    .unwrap();
    let opened = f.call(&[
        "open",
        "claude",
        "--resume",
        "native-claude-code",
        "--headless",
    ]);
    let id = opened["id"].as_str().unwrap();
    assert_eq!(
        f.wait(id, "exited")["status_detail"]["error"],
        "native_home_changed"
    );
    f.call(&["resume", id]);
    assert_eq!(
        f.wait(id, "exited")["status_detail"]["error"],
        "native_home_changed"
    );
    assert!(!f.root.path().join("claude.starts").exists());
}

#[test]
fn a_fork_gets_a_new_identity_and_cannot_adopt_its_parent() {
    let f = Fixture::new();
    let history = f
        .root
        .path()
        .join(".claude/projects/work/native-claude-code.jsonl");
    std::fs::create_dir_all(history.parent().unwrap()).unwrap();
    std::fs::write(
        history,
        format!(
            "{{\"cwd\":{}}}\n",
            serde_json::to_string(f.root.path()).unwrap()
        ),
    )
    .unwrap();
    let parent = f.call(&["open", "claude", "--resume", "native-claude-code"]);
    let program=FAKE.replace("report SessionStart\nlast=","report SessionStart\npayload='{\"session_id\":\"forked\",\"root_confirmed\":true}'\nreport SessionStart\nlast=");
    std::fs::write(f.root.path().join("bin/claude"), program).unwrap();
    let fork = f.call(&[
        "open",
        "claude",
        "--resume",
        "native-claude-code",
        "--fork",
        "--headless",
    ]);
    assert_ne!(parent["id"], fork["id"]);
    let id = fork["id"].as_str().unwrap();
    let row = f.wait(id, "idle");
    assert_eq!(row["native_session_id"], "forked");
    assert_eq!(
        f.call(&["get", parent["id"].as_str().unwrap()])["native_session_id"],
        "native-claude-code"
    );
    let args = std::fs::read_to_string(f.root.path().join("claude.args")).unwrap();
    assert!(args.contains("--resume\nnative-claude-code\n--fork-session"));
    f.call(&["kill", id]);
    f.call(&["kill", parent["id"].as_str().unwrap()]);
}

#[test]
fn restart_never_silently_discards_an_unknown_policy_option() {
    let f = Fixture::new();
    let opened = f.call(&[
        "open",
        "claude",
        "--headless",
        "--",
        "--future-policy",
        "restrictive",
    ]);
    let id = opened["id"].as_str().unwrap();
    let before = f.wait(id, "idle");
    let output = f.command(&["--json", "restart", id]).output().unwrap();
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["data"]["code"],
        "resume_options_required"
    );
    assert_eq!(f.call(&["get", id])["pid"], before["pid"]);
    f.call(&["restart", id, "--", "--permission-mode", "plan"]);
    let after = f.wait(id, "idle");
    assert_ne!(after["pid"], before["pid"]);
    let args = std::fs::read_to_string(f.root.path().join("claude.args")).unwrap();
    assert!(args.contains("--permission-mode\nplan"));
    assert!(!args.contains("--future-policy"));
    f.call(&["kill", id]);
}

#[test]
fn foreground_wrapper_registers_and_unregisters_without_a_pool() {
    let f = Fixture::new();
    let script = f.root.path().join("bin/claude");
    std::fs::write(&script,"#!/bin/sh\nprintf '{\"session_id\":\"wrapper-native\",\"cwd\":\"%s\"}' \"$PWD\" | \"$TOWER_TEST_BIN\" hook claude SessionStart\nsleep 0.3\nprintf '{\"session_id\":\"wrapper-native\",\"cwd\":\"%s\"}' \"$PWD\" | \"$TOWER_TEST_BIN\" hook claude Stop\nsleep 0.3\n").unwrap();
    let output = f.command(&["run", "claude"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = f.call(&["ls", "--all"]);
    let records = records.as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["ownership"], "registered");
    assert_eq!(records[0]["native_session_id"], "wrapper-native");
    assert_eq!(records[0]["status"], "exited");
    assert_eq!(records[0]["caps"]["attach"], false);
    assert!(!f.root.path().join("s/tower/run/pool.sock").exists());
}

#[test]
fn scanner_does_not_allocate_an_id_for_a_managed_process() {
    let f = Fixture::new();
    let config = f.root.path().join("c/tower");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("config.toml"), "[scan]\ninterval_s=1\n").unwrap();
    let opened = f.call(&["open", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    f.wait(id, "idle");
    thread::sleep(Duration::from_millis(2200));
    let rows = f.call(&["ls"]);
    let records = rows.as_array().unwrap();
    let owned = records
        .iter()
        .filter(|r| r["ownership"] == "owned")
        .collect::<Vec<_>>();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0]["id"], id);
    assert!(!records.iter().any(|r| {
        r["sources"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("scan")))
            && r["pid"] == owned[0]["pid"]
    }));
    f.call(&["kill", id]);
}

#[test]
fn configured_pty_delivery_uses_the_supervisor_gate_and_rejects_busy_targets() {
    let f = Fixture::new();
    let config = f.root.path().join("c/tower");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("config.toml"), "[inject.opencode]\npty=true\n").unwrap();
    let script = f.root.path().join("bin/opencode");
    std::fs::write(&script,r#"#!/bin/sh
stty raw -echo
printf '\033[?2004h'
printf '{"session_id":"pty-native","root_confirmed":true}' | "$TOWER_TEST_BIN" hook opencode SessionStart
printf READY
cat
"#).unwrap();
    let opened = f.call(&["open", "opencode", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let row = f.wait(id, "idle");
    assert_eq!(row["caps"]["message"], "pty");
    let receipt = f.call(&["send", id, "PTY-DELIVERY-MARKER"]);
    assert_eq!(receipt["method"], "pty");
    assert_eq!(receipt["acknowledgement"], "pty_submission");
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        if f.call(&["snapshot", id])["buffer"]
            .as_str()
            .unwrap()
            .contains("PTY-DELIVERY-MARKER")
        {
            break;
        }
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(10));
    }
    let second = f
        .command(&["--json", "send", id, "SECOND-PTY-MESSAGE"])
        .output()
        .unwrap();
    assert!(!second.status.success());
    let error: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(error["error"]["data"]["reason"], "target_busy_pty");
    std::fs::write(config.join("config.toml"), "[inject.opencode]\npty=false\n").unwrap();
    assert_eq!(f.call(&["get", id])["caps"]["message"], "none");
    f.call(&["kill", id]);
}
