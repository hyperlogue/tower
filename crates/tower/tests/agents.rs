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
        let fixture = Self { root };
        fixture.config("");
        fixture
    }
    /// Writes daemon config. The process scan is off unless a test enables it:
    /// it would register agent processes from other tests and the host.
    fn config(&self, extra: &str) {
        let dir = self.root.path().join("c/tower");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("config.toml"),
            format!("[scan]\nenabled=false\n{extra}"),
        )
        .unwrap();
    }
    fn command(&self, args: &[&str]) -> Command {
        self.program(env!("CARGO_BIN_EXE_tower-cli"), args)
    }
    fn program(&self, binary: impl AsRef<std::ffi::OsStr>, args: &[&str]) -> Command {
        let mut c = Command::new(binary);
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
            .env("TOWER_TEST_BIN", env!("CARGO_BIN_EXE_tower-cli"))
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
        for (binary, args) in [
            (env!("CARGO_BIN_EXE_tower-terminal-pool"), &["stop"][..]),
            ("codex", &["app-server", "daemon", "stop"][..]),
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
        let args = vec!["launch", h, "--headless"];
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
    assert_eq!(all.as_array().unwrap().len(), 9, "{all}");
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
    let opened = f.call(&["launch", "claude", "--headless"]);
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
    let opened = f.call(&["launch", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let row = f.wait(id, "exited");
    assert_eq!(row["status_detail"]["error"], "environment_not_approved");
    assert!(!f.root.path().join("claude.starts").exists());
}

#[test]
fn pool_crash_recovers_native_conversations_but_never_reopens_killed_sessions() {
    let f = Fixture::new();
    let opened = f.call(&["launch", "claude", "--headless"]);
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
    assert!(
        f.program(env!("CARGO_BIN_EXE_tower-terminal-pool"), &["stop"])
            .status()
            .unwrap()
            .success()
    );
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
fn pool_upgrade_replaces_the_pool_and_resumes_its_sessions() {
    let f = Fixture::new();
    let opened = f.call(&["launch", "claude", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let before = f.wait(id, "idle");
    let launcher = |row: &Value| {
        tower_pal::process_identity(row["pid"].as_u64().unwrap() as u32)
            .unwrap()
            .parent
    };
    let old_pool = tower_pal::process_identity(launcher(&before))
        .unwrap()
        .parent;
    let info = f.call(&["pool", "upgrade", "--wait-idle"]);
    assert_eq!(info["running"], true);
    let recovered = f.wait(id, "idle");
    assert_ne!(recovered["pid"], before["pid"]);
    assert_eq!(recovered["native_session_id"], before["native_session_id"]);
    assert!(tower_pal::process_identity(old_pool).is_err());
    assert!(
        std::fs::read_to_string(f.root.path().join("claude.args"))
            .unwrap()
            .contains("--resume\nnative-claude-code")
    );
    f.call(&["kill", id]);
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
    let opened = f.call(&["launch", "claude", "--headless"]);
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
    let opened = f.call(&["launch", "claude", "--resume", "native-claude-code"]);
    let id = opened["id"].as_str().unwrap();
    assert_eq!(
        f.call(&["get", id])["native_session_id"],
        "native-claude-code"
    );
    let again = f
        .command(&[
            "--json",
            "launch",
            "claude",
            "--resume",
            "native-claude-code",
        ])
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
        "launch",
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
    let parent = f.call(&[
        "launch",
        "claude",
        "--resume",
        "native-claude-code",
        "--defer",
    ]);
    let program=FAKE.replace("report SessionStart\nlast=","report SessionStart\npayload='{\"session_id\":\"forked\",\"root_confirmed\":true}'\nreport SessionStart\nlast=");
    std::fs::write(f.root.path().join("bin/claude"), program).unwrap();
    let fork = f.call(&[
        "launch",
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
        "launch",
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
    std::fs::write(
        config.join("config.toml"),
        "[scan]\nenabled=true\ninterval_s=1\n",
    )
    .unwrap();
    let opened = f.call(&["launch", "claude", "--headless"]);
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
    f.config("[inject.opencode]\npty=true\n");
    let script = f.root.path().join("bin/opencode");
    std::fs::write(&script,r#"#!/bin/sh
stty raw -echo
printf '\033[?2004h'
printf '{"session_id":"pty-native","root_confirmed":true}' | "$TOWER_TEST_BIN" hook opencode SessionStart
printf READY
cat
"#).unwrap();
    let opened = f.call(&["launch", "opencode", "--headless"]);
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
    f.config("[inject.opencode]\npty=false\n");
    assert_eq!(f.call(&["get", id])["caps"]["message"], "none");
    f.call(&["kill", id]);
}

#[test]
fn pool_upgrade_nudges_an_agent_that_was_working() {
    let f = Fixture::new();
    let config = f.root.path().join("c/tower");
    std::fs::create_dir_all(&config).unwrap();
    f.config("[inject.opencode]\npty=true\n");
    let script = f.root.path().join("bin/opencode");
    std::fs::write(&script,r#"#!/bin/sh
stty raw -echo
printf '\033[?2004h'
printf '{"session_id":"pty-native","root_confirmed":true}' | "$TOWER_TEST_BIN" hook opencode SessionStart
case " $* " in
*" pty-native "*) ;;
*) printf '{"session_id":"pty-native"}' | "$TOWER_TEST_BIN" hook opencode UserPromptSubmit ;;
esac
printf READY
cat
"#).unwrap();
    let opened = f.call(&["launch", "opencode", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let before = f.wait(id, "working");
    f.call(&["pool", "upgrade"]);
    let after = f.wait(id, "idle");
    assert_ne!(after["pid"], before["pid"]);
    let until = Instant::now() + Duration::from_secs(5);
    while !f.call(&["snapshot", id])["buffer"]
        .as_str()
        .unwrap()
        .contains("Tower restarted and interrupted")
    {
        assert!(Instant::now() < until, "the resumed agent was not nudged");
        thread::sleep(Duration::from_millis(20));
    }
    f.call(&["kill", id]);
}

#[test]
fn codex_launch_explains_a_daemon_that_cannot_start() {
    let f = Fixture::new();
    let codex = f.root.path().join("bin/codex");
    std::fs::write(
        &codex,
        "#!/bin/sh\ncase \"$1\" in --version|-V) echo 'codex-cli 0.166.0';; *) exit 1;; esac\n",
    )
    .unwrap();
    tower_pal::make_executable(&codex).unwrap();
    let opened = f.call(&["launch", "codex", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        let snapshot = f.call(&["snapshot", id]).to_string();
        if snapshot.contains("Run `codex app-server daemon start` to see the cause") {
            break;
        }
        assert!(Instant::now() < until, "no daemon start error: {snapshot}");
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn terminal_launch_starts_by_default_and_defer_is_explicit() {
    let f = Fixture::new();
    let opened = f.call(&[
        "launch", "pi", "--term", "xterm", "--rows", "31", "--cols", "101",
    ]);
    let id = opened["id"].as_str().unwrap();
    f.wait(id, "idle");
    let snapshot = f.call(&["snapshot", id]);
    assert_eq!(snapshot["rows"], 31);
    assert_eq!(snapshot["cols"], 101);
    let deferred = f.call(&["launch", "opencode", "--defer"]);
    let id = deferred["id"].as_str().unwrap();
    let row = f.call(&["get", id]);
    assert_eq!(row["status"], "starting");
    assert!(row["pid"].is_null());
    assert!(!f.root.path().join("opencode.starts").exists());
    let unsupported = f
        .command(&[
            "launch",
            "codex",
            "--protocol",
            "codex-app-server",
            "--term",
            "xterm",
        ])
        .output()
        .unwrap();
    assert!(!unsupported.status.success());
    assert!(
        String::from_utf8_lossy(&unsupported.stderr)
            .contains("Terminal options require a terminal session")
    );
    for args in [
        vec!["launch", "pi", "--term", "bad/term"],
        vec!["launch", "pi", "--rows", "0", "--cols", "80"],
    ] {
        assert!(!f.command(&args).output().unwrap().status.success());
    }
}

#[test]
fn non_codex_launch_does_not_load_codex_adapter_settings() {
    let f = Fixture::new();
    f.config("[codex]\nendpoint=42\n");
    let opened = f.call(&["launch", "pi", "--headless"]);
    f.wait(opened["id"].as_str().unwrap(), "idle");
}

#[test]
fn failed_state_writes_do_not_drop_the_native_process_or_replay_its_launch() {
    let f = Fixture::new();
    let opened = f.call(&["launch", "pi", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let before = f.wait(id, "idle");
    let root = f.root.path().join("s/tower/sessions").join(id);
    let generation = std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let path = generation.join("state.json");
    let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let pid = saved["agent_pid"].as_u64().unwrap() as u32;
    let identity = tower_pal::process_identity(pid).unwrap();
    std::fs::rename(&path, generation.join("state.backup")).unwrap();
    std::fs::create_dir(&path).unwrap();
    f.phase("work");
    thread::sleep(Duration::from_millis(700));
    assert_eq!(
        tower_pal::process_identity(pid).unwrap().start_time,
        identity.start_time
    );
    std::fs::remove_dir(&path).unwrap();
    let after = f.wait(id, "working");
    assert_eq!(after["pid"], before["pid"]);
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("pi.starts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    f.call(&["kill", id]);
    f.wait(id, "exited");
}

#[test]
fn omp_dialogs_and_child_prompts_reach_the_registry_without_replacing_root_facts() {
    let f = Fixture::new();
    std::fs::write(f.root.path().join("bin/omp"),r#"#!/bin/sh
report() { printf '%s' "$2" | "$TOWER_TEST_BIN" hook omp "$1"; }
report SessionStart '{"session_id":"native-omp","model":"parent-model"}'
last=
while :; do
  phase=$(cat phase 2>/dev/null || :)
  if [ "$phase" != "$last" ]; then
    case $phase in
      question) report status-update '{"session_id":"native-omp","blocking_prompt":"decision"}';;
      child) report Stop '{"session_id":"child","session_is_child":true,"blocking_prompt":"approval","model":"child-model","cwd":"/child"}';;
      finish) report Stop '{"session_id":"native-omp"}';;
      child-release) report status-update '{"session_id":"child","session_is_child":true,"blocking_prompt":"none"}';;
      release) report status-update '{"session_id":"native-omp","blocking_prompt":"none"}';;
    esac
    printf '%s' "$phase" > observed-phase
    last=$phase
  fi
  sleep 0.03
done
"#).unwrap();
    let launched = f.call(&["launch", "omp", "--headless"]);
    let id = launched["id"].as_str().unwrap();
    f.wait(id, "idle");
    f.phase("question");
    f.wait(id, "waiting_input");
    f.phase("child");
    let row = f.wait(id, "waiting_approval");
    assert_eq!(row["native_session_id"], "native-omp");
    assert_eq!(row["status_detail"]["model"], "parent-model");
    assert_ne!(row["cwd"], "/child");
    f.phase("finish");
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::read_to_string(f.root.path().join("observed-phase")).unwrap_or_default()
        != "finish"
    {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(f.call(&["get", id])["status"], "waiting_approval");
    f.phase("child-release");
    f.wait(id, "waiting_input");
    f.phase("release");
    f.wait(id, "idle");
    f.call(&["kill", id]);
    f.wait(id, "exited");
}

fn native_codex(f: &Fixture) {
    std::fs::rename(
        f.root.path().join("bin/codex"),
        f.root.path().join("bin/codex-host"),
    )
    .unwrap();
    let bin = f.root.path().join("bin/codex");
    std::fs::write(&bin,r#"#!/bin/sh
if [ "$1" = queue ]; then
  if [ "$2" = --help ]; then printf '%s\n' '--thread --message'; else printf '%s\n' "$3" > queued-thread; fi
  exit 0
fi
if [ "$1" != --profile ]; then exec "$HOME/bin/codex-host" "$@"; fi
printf '%s\n' "$@" > native.args
id="native-$TOWER_AGENT_ID"
previous=
for arg in "$@"; do
  if [ "$previous" = resume ]; then id=$arg; fi
  previous=$arg
done
mkdir -p "$CODEX_HOME/sessions/2026/01/01"
printf '{"type":"session_meta","payload":{"id":"%s","cwd":"%s","source":"cli"}}\n' "$id" "$PWD" > "$CODEX_HOME/sessions/2026/01/01/$id.jsonl"
payload=$(printf '{"session_id":"%s","model":"native-model","cwd":"%s"}' "$id" "$PWD")
report() { printf '%s' "$payload" | "$TOWER_TEST_BIN" hook codex "$1"; }
report SessionStart
last=
while :; do
  phase=$(cat phase 2>/dev/null || :)
  if [ "$phase" != "$last" ]; then
    case $phase in work) report UserPromptSubmit;; approval) report PermissionRequest;; idle) report Stop;; esac
    last=$phase
  fi
  sleep 0.03
done
"#).unwrap();
    tower_pal::make_executable(&bin).unwrap();
}

#[test]
fn codex_native_mode_uses_hooks_queue_and_its_own_process_without_a_host() {
    let f = Fixture::new();
    native_codex(&f);
    f.config("[codex]\nendpoint=42\n");
    let opened = f.call(&["launch", "codex", "--mode", "native", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    let row = f.wait(id, "idle");
    assert_eq!(row["mode"], "native");
    assert_eq!(row["shared_process"], false);
    assert!(row["pid"].is_number());
    assert_eq!(row["caps"]["message"], "queue");
    assert!(
        !f.root
            .path()
            .join(".codex/app-server-control/app-server-control.sock")
            .exists()
    );
    let native = row["native_session_id"].as_str().unwrap();
    let args = std::fs::read_to_string(f.root.path().join("native.args")).unwrap();
    assert!(args.starts_with("--profile\ntower\n--enable\nhooks\n"));
    f.phase("work");
    f.wait(id, "working");
    let receipt = f.call(&["send", id, "native fixture message"]);
    assert_eq!(receipt["method"], "queue");
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("queued-thread"))
            .unwrap()
            .trim(),
        native
    );
    f.call(&["daemon", "stop"]);
    let recovered = f.wait(id, "working");
    assert_eq!(recovered["pid"], row["pid"]);
    f.phase("idle");
    f.wait(id, "idle");
    f.call(&["kill", id]);
    f.wait(id, "exited");
    f.call(&["resume", id]);
    let resumed = f.wait(id, "idle");
    assert_eq!(resumed["mode"], "native");
    assert_eq!(resumed["native_session_id"], native);
    assert_ne!(resumed["pid"], row["pid"]);
    f.call(&["kill", id]);
}

#[test]
fn codex_mode_switch_validates_history_and_preserves_the_shared_host() {
    let f = Fixture::new();
    native_codex(&f);
    let opened = f.call(&["launch", "codex", "--headless"]);
    let id = opened["id"].as_str().unwrap();
    f.wait(id, "idle");
    f.phase("work");
    f.wait(id, "working");
    f.phase("idle");
    let original = f.wait(id, "idle");
    assert_eq!(original["mode"], "app-server");
    let native = original["native_session_id"].as_str().unwrap();
    let rejected = f
        .command(&["restart", id, "--mode", "native"])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert_eq!(f.wait(id, "idle")["pid"], original["pid"]);
    let sessions = f.root.path().join(".codex/sessions/2026/01/01");
    std::fs::create_dir_all(&sessions).unwrap();
    std::fs::write(sessions.join(format!("{native}.jsonl")),format!("{}\n",json!({"type":"session_meta","payload":{"id":native,"cwd":f.root.path(),"source":"cli"}}))).unwrap();
    f.call(&["restart", id, "--mode", "native"]);
    let switched = f.wait(id, "idle");
    assert_eq!(switched["mode"], "native");
    assert_eq!(switched["native_session_id"], native);
    assert_eq!(switched["shared_process"], false);
    assert!(tower_pal::process_identity(original["pid"].as_u64().unwrap() as u32).is_ok());
    f.call(&["restart", id, "--mode", "app-server"]);
    let back = f.wait(id, "idle");
    assert_eq!(back["mode"], "app-server");
    assert_eq!(back["native_session_id"], native);
    assert_eq!(back["pid"], original["pid"]);
    assert_eq!(back["shared_process"], true);
    f.call(&["kill", id]);
}

#[test]
fn unmanaged_claude_sessions_are_discovered_from_session_files() {
    let f = Fixture::new();
    f.call(&["ls"]);
    let sessions = f.root.path().join(".claude/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let mut child = Command::new("sleep").arg("30").spawn().unwrap();
    let pid = child.id();
    let start = tower_pal::process_identity(pid).unwrap().start_time;
    let native = "0e9d6f8e-1111-4222-8333-444455556666";
    let write_session = |native: &str, status: &str| {
        let staged = sessions.join("staged");
        let file = serde_json::json!({"pid":pid,"procStart":start.to_string(),"sessionId":native,"cwd":f.root.path(),"status":status,"name":"Fixture","nameSource":"user"});
        std::fs::write(&staged, file.to_string()).unwrap();
        std::fs::rename(&staged, sessions.join(format!("{pid}.json"))).unwrap();
    };
    let find_session = |native: &str, status: &str| {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let all = f.call(&["ls", "--all"]);
            if let Some(row) = all
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["native_session_id"] == native && r["status"] == status)
            {
                return row.clone();
            }
            assert!(Instant::now() < until, "expected {status}: {all}");
            thread::sleep(Duration::from_millis(40));
        }
    };
    write_session(native, "busy");
    let row = find_session(native, "working");
    assert_eq!(row["ownership"], "registered");
    assert_eq!(row["pid"], pid);
    write_session(native, "idle");
    let first = find_session(native, "idle");
    // A cleared conversation keeps the process and its record.
    let cleared = "1f9d6f8e-1111-4222-8333-444455556666";
    write_session(cleared, "busy");
    assert_eq!(find_session(cleared, "working")["id"], first["id"]);
    child.kill().unwrap();
    child.wait().unwrap();
    find_session(cleared, "lost");
}

#[test]
fn unmanaged_codex_rollouts_are_discovered_from_their_writer() {
    let f = Fixture::new();
    let day = f.root.path().join(".codex/sessions/2026/11/10");
    std::fs::create_dir_all(&day).unwrap();
    let native = "0e9d6f8e-2222-4222-8333-444455556666";
    let rollout = day.join(format!("rollout-2026-11-10T00-00-00-{native}.jsonl"));
    let append = |line: Value| {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&rollout)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    };
    append(
        json!({"type":"session_meta","payload":{"id":native,"cwd":f.root.path(),"source":"cli"}}),
    );
    let standalone = f.root.path().join("standalone");
    std::fs::create_dir(&standalone).unwrap();
    let bin = standalone.join("codex");
    std::fs::write(&bin, "#!/bin/sh\nexec 3>>\"$1\"\nread _\n").unwrap();
    tower_pal::make_executable(&bin).unwrap();
    let mut child = Command::new(&bin)
        .arg(&rollout)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let until = Instant::now() + Duration::from_secs(5);
    while !tower_pal::open_files(pid).is_ok_and(|files| files.contains(&rollout)) {
        assert!(Instant::now() < until, "fixture never opened its rollout");
        thread::sleep(Duration::from_millis(20));
    }
    let find = |status: &str| {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let all = f.call(&["ls", "--all"]);
            if let Some(row) = all
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["native_session_id"] == native && r["status"] == status)
            {
                return row.clone();
            }
            assert!(Instant::now() < until, "expected {status}: {all}");
            thread::sleep(Duration::from_millis(40));
        }
    };
    let row = find("idle");
    assert_eq!(row["ownership"], "registered");
    assert_eq!(row["harness"], "codex");
    assert_eq!(row["pid"], pid);
    append(json!({"type":"event_msg","payload":{"type":"task_started"}}));
    find("working");
    append(json!({"type":"event_msg","payload":{"type":"task_complete"}}));
    find("idle");
    child.kill().unwrap();
    child.wait().unwrap();
    find("lost");

    // A rollout started while the daemon runs is found from its first write.
    let native = "0e9d6f8e-3333-4222-8333-444455556666";
    let rollout = day.join(format!("rollout-2026-11-10T00-00-01-{native}.jsonl"));
    let mut child = Command::new(&bin)
        .arg(&rollout)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !tower_pal::open_files(child.id()).is_ok_and(|files| files.contains(&rollout)) {
        assert!(Instant::now() < until, "fixture never opened its rollout");
        thread::sleep(Duration::from_millis(20));
    }
    let line =
        json!({"type":"session_meta","payload":{"id":native,"cwd":f.root.path(),"source":"cli"}});
    let started = json!({"type":"event_msg","payload":{"type":"task_started"}});
    std::fs::write(&rollout, format!("{line}\n{started}\n")).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let all = f.call(&["ls", "--all"]);
        if all.as_array().unwrap().iter().any(|r| {
            r["native_session_id"] == native && r["status"] == "working" && r["pid"] == child.id()
        }) {
            break;
        }
        assert!(Instant::now() < until, "expected the new rollout: {all}");
        thread::sleep(Duration::from_millis(40));
    }
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn unmanaged_pi_grok_reasonix_and_opencode_sessions_are_discovered_from_their_files() {
    let f = Fixture::new();
    let home = f.root.path();
    let work = home.join("work");
    std::fs::create_dir(&work).unwrap();
    let standalone = home.join("standalone");
    std::fs::create_dir(&standalone).unwrap();
    // Each fake harness holds its first argument open (if any) until stdin closes.
    let spawn = |name: &str, held: Option<&std::path::Path>| {
        let bin = standalone.join(name);
        std::fs::write(&bin, "#!/bin/sh\n[ -n \"$1\" ] && exec 3>>\"$1\"\nread _\n").unwrap();
        tower_pal::make_executable(&bin).unwrap();
        let mut command = Command::new(&bin);
        command.current_dir(&work).stdin(Stdio::piped());
        if let Some(held) = held {
            std::fs::create_dir_all(held.parent().unwrap()).unwrap();
            command.arg(held);
        }
        let child = command.spawn().unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        while held.is_some_and(|held| {
            !tower_pal::open_files(child.id()).is_ok_and(|files| files.iter().any(|f| f == held))
        }) {
            assert!(Instant::now() < until, "{name} never opened its file");
            thread::sleep(Duration::from_millis(20));
        }
        child
    };
    let find = |harness: &str, native: &str, status: &str, pid: u32| {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let all = f.call(&["ls", "--all"]);
            if all.as_array().unwrap().iter().any(|r| {
                r["harness"] == harness
                    && r["native_session_id"] == native
                    && r["status"] == status
                    && r["pid"] == pid
            }) {
                return;
            }
            assert!(Instant::now() < until, "expected {harness} {status}: {all}");
            thread::sleep(Duration::from_millis(40));
        }
    };
    let append = |path: &std::path::Path, line: Value| {
        use std::io::Write;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    };

    // Reasonix holds its lease from launch, so the daemon finds it at startup.
    let lease = home.join(".reasonix/projects/-work/sessions/20261112-1.5-model.jsonl.lease.lock");
    let mut reasonix = spawn("reasonix", Some(&lease));
    f.call(&["ls"]);
    find("reasonix", "20261112-1.5-model", "unknown", reasonix.id());

    // Pi keeps no file open; its session is matched by working directory.
    let mut pi = spawn("pi", None);
    let session = home.join(".pi/agent/sessions/--work--/2026-11-12T00-00-00-000Z_p1.jsonl");
    append(
        &session,
        json!({"type":"session","version":3,"id":"p1","cwd":work}),
    );
    append(
        &session,
        json!({"type":"message","message":{"role":"user"}}),
    );
    find("pi", "p1", "working", pi.id());
    append(
        &session,
        json!({"type":"message","message":{"role":"assistant","stopReason":"stop"}}),
    );
    find("pi", "p1", "idle", pi.id());

    // Grok holds lock files in its session directory.
    let dir = home
        .join(".grok/sessions")
        .join(work.to_str().unwrap().replace('/', "%2F"))
        .join("01ab");
    let mut grok = spawn("grok", Some(&dir.join("chat_history.jsonl.lock")));
    append(&dir.join("events.jsonl"), json!({"type":"turn_started"}));
    find("grok", "01ab", "working", grok.id());
    append(
        &dir.join("events.jsonl"),
        json!({"type":"permission_requested"}),
    );
    find("grok", "01ab", "waiting_approval", grok.id());
    append(&dir.join("events.jsonl"), json!({"type":"turn_ended"}));
    find("grok", "01ab", "idle", grok.id());

    // OpenCode records busy and idle times in its session database.
    let mut opencode = spawn("opencode", None);
    let data = home.join(".local/share/opencode");
    std::fs::create_dir_all(&data).unwrap();
    let db = rusqlite::Connection::open(data.join("opencode.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE session_v2 (id text PRIMARY KEY, parent_id text, directory text NOT NULL,
           title text, time_updated integer NOT NULL, time_archived integer,
           time_busy integer, time_idle integer);",
    )
    .unwrap();
    db.execute(
        "INSERT INTO session_v2 VALUES ('ses_1', NULL, ?1, 'Fix it', 10, NULL, 11, NULL)",
        [work.to_str().unwrap()],
    )
    .unwrap();
    find("opencode", "ses_1", "working", opencode.id());
    db.execute("UPDATE session_v2 SET time_idle = 12", [])
        .unwrap();
    find("opencode", "ses_1", "idle", opencode.id());

    for child in [&mut reasonix, &mut pi, &mut grok, &mut opencode] {
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
