use std::{
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tower_agents::runtime::Embedding;
use tower_launcher::local::{Local, Session};

// Stands in for Claude Code: reports through the helper the way its hooks do.
const AGENT: &str = r#"#!/bin/sh
printf '%s\n' "$@" > claude.args
env > claude.env
payload='{"session_id":"native-claude","model":"fixture-model","root_confirmed":true}'
report() { printf '%s' "$payload" | "$EMBEDDER" hook claude-code "$1"; }
report SessionStart
last=
while :; do
  phase=$(cat phase 2>/dev/null || :)
  if [ "$phase" != "$last" ]; then
    case $phase in work) report UserPromptSubmit;; idle) report Stop;; esac
    last=$phase
  fi
  sleep 0.03
done
"#;

fn embedder() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    let path = path.join("examples/embedder");
    // Build the executable with cargo build --workspace --all-targets first.
    assert!(
        path.exists(),
        "build the fixture first: cargo build -p tower-launcher --example embedder"
    );
    path
}
fn fixture_root() -> tempfile::TempDir {
    // Canonical macOS TMPDIR paths exceed sockaddr_un once the control
    // socket's state/run/agents suffix is appended.
    tempfile::Builder::new()
        .prefix("tw-embed-")
        .tempdir_in("/tmp")
        .unwrap()
}

#[tokio::test]
async fn embedded_fixture_control_socket_can_bind() {
    use tower_agents::{
        Harness,
        runtime::{AgentSpec, LauncherPlan},
    };
    let root = fixture_root();
    let root = root.path().canonicalize().unwrap();
    let local = Local::new(local_embedding(&embedder(), &root.join("state"))).unwrap();
    let plan = local
        .prepare(
            "one",
            AgentSpec {
                harness: Harness::ClaudeCode,
                cwd: root.to_string_lossy().into_owned(),
                args: Vec::new(),
                mode: None,
                resume: None,
                fork: false,
                native_home: None,
            },
        )
        .unwrap();
    let plan: LauncherPlan = serde_json::from_slice(&std::fs::read(plan).unwrap()).unwrap();
    let _listener = tower_pal::IpcListener::bind(Path::new(&plan.control_socket))
        .await
        .unwrap();
}

struct Launcher {
    child: Child,
    local: Local,
    reaped: bool,
}
impl Launcher {
    fn new(child: Child, embedding: Embedding) -> Self {
        Self {
            child,
            local: Local::new(embedding).unwrap(),
            reaped: false,
        }
    }
    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.reaped = true;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "embedded launcher did not stop",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Launcher {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        let _ = self.local.stop("one");
        if self.wait().is_ok() {
            return;
        }
        eprintln!("Embedded launcher shutdown failed; force-killing fixture process group");
        let killed =
            tower_pal::process_identity(self.child.id()).and_then(tower_pal::kill_protocol_group);
        let reaped = self.wait();
        let message =
            format!("embedded launcher required forced cleanup: kill={killed:?}, reap={reaped:?}");
        if std::thread::panicking() {
            eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

#[test]
fn embedded_launcher_cleanup_reports_forced_shutdown_and_reaps() {
    let root = fixture_root();
    let child = Command::new("/bin/sh")
        .args(["-c", "exec sleep 60"])
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id();
    let launcher = Launcher::new(
        child,
        local_embedding(&embedder(), &root.path().join("state")),
    );
    let result = std::panic::catch_unwind(|| drop(launcher));
    assert!(result.is_err(), "forced cleanup must fail the test");
    assert!(tower_pal::process_identity(pid).is_err());
}

async fn await_session(
    local: &Local,
    watch: &mut tower_pal::TreeWatch,
    phase: &str,
    ready: impl Fn(&Session) -> bool,
) -> Session {
    // Longer than any launcher step; a miss names the state being awaited.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let sessions = local.sessions().unwrap();
            for session in sessions {
                if ready(&session) {
                    return session;
                }
                assert!(
                    session.state.exit_code.is_none() || session.state.stop_requested,
                    "session exited awaiting {phase}: code {:?}, error {:?}",
                    session.state.exit_code,
                    session.state.detail.error,
                );
            }
            watch.changed().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out awaiting {phase}: {:?}", local.sessions()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedder_runs_watches_and_stops_a_session_without_daemon_or_pool() {
    let root = fixture_root();
    let root = std::fs::canonicalize(root.path()).unwrap();
    let (state, bin, work) = (root.join("state"), root.join("bin"), root.join("work"));
    for dir in [&bin, &work] {
        std::fs::create_dir(dir).unwrap();
    }
    let agent = bin.join("claude");
    std::fs::write(&agent, AGENT).unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let helper = embedder();
    let local = Local::new(Embedding {
        helper: helper.to_string_lossy().into_owned(),
        state: state.to_string_lossy().into_owned(),
        codex_endpoint: None,
    })
    .unwrap();
    let mut watch = local.watch().unwrap();

    let child = Command::new(&helper)
        .args(["launch", "one", "claude-code"])
        .arg(&work)
        .env_clear()
        .env("HOME", &root)
        .env("EMBEDDER", &helper)
        .env("EMBEDDER_STATE", &state)
        // A leftover daemon address must not reach an embedded agent.
        .env("TOWER_SOCKET", root.join("absent.sock"))
        .env(
            "PATH",
            format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
        )
        .stdin(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();
    let mut launcher = Launcher::new(child, local_embedding(&helper, &state));

    let started = await_session(&local, &mut watch, "agent start", |s| {
        s.live
            && s.state
                .native
                .as_ref()
                .is_some_and(|n| n.id == "native-claude")
    })
    .await;
    assert_eq!(started.plan.id, "one");
    std::fs::write(work.join("phase"), "work").unwrap();
    await_session(&local, &mut watch, "working", |s| {
        s.state.status == "working"
    })
    .await;
    std::fs::write(work.join("phase"), "idle").unwrap();
    await_session(&local, &mut watch, "idle", |s| s.state.status == "idle").await;

    // The dashboard's view: one record per session, in the daemon's shape.
    let record = local.records().unwrap().pop().unwrap();
    assert_eq!(
        (
            record.id.as_str(),
            record.status.as_str(),
            record.harness.as_str()
        ),
        ("one", "idle", "claude-code")
    );
    assert_eq!(record.native_session_id.as_deref(), Some("native-claude"));
    let detail = serde_json::to_value(&record.status_detail).unwrap();
    assert_eq!(detail["model"], "fixture-model");
    assert!(record.caps.kill && record.caps.resume && !record.caps.attach);
    assert!(record.needs_attention, "a finished turn awaits its user");
    let pinned = local
        .patch_tags("one", &["pinned".into()], &[], true)
        .unwrap();
    assert!(pinned.tags.contains("pinned") && !pinned.needs_attention);
    watch.changed().await;
    std::fs::write(work.join("phase"), "work").unwrap();
    await_session(&local, &mut watch, "working again", |s| {
        s.state.status == "working"
    })
    .await;
    std::fs::write(work.join("phase"), "idle").unwrap();
    await_session(&local, &mut watch, "idle again", |s| {
        s.state.status == "idle"
    })
    .await;
    let record = local.records().unwrap().pop().unwrap();
    assert!(record.tags.contains("pinned") && record.needs_attention);
    assert!(local.patch_tags("absent", &[], &[], true).is_err());

    let args = std::fs::read_to_string(work.join("claude.args")).unwrap();
    assert!(args.contains("--settings"), "{args}");
    assert!(!args.contains("--mcp-config"), "{args}");
    let settings = Path::new(&started.plan.state_dir).join("config/claude-settings.json");
    let settings = std::fs::read_to_string(settings).unwrap();
    assert!(settings.contains(&*helper.to_string_lossy()), "{settings}");
    let env = std::fs::read_to_string(work.join("claude.env")).unwrap();
    assert!(env.contains("TOWER_LAUNCHER_SOCKET="), "{env}");
    assert!(!env.contains("TOWER_SOCKET="), "{env}");
    assert!(
        !Path::new(&started.plan.state_dir)
            .join("clipboard-bin")
            .exists()
    );
    assert!(local.forget("one").is_err());

    let stopper = Local::new(started.plan.embedding.clone().unwrap()).unwrap();
    tokio::task::spawn_blocking(move || stopper.stop("one"))
        .await
        .unwrap()
        .unwrap();
    let status = tokio::task::spawn_blocking(move || launcher.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
    let ended = await_session(&local, &mut watch, "exit record", |s| !s.live).await;
    assert_eq!(ended.state.status, "exited");
    assert!(ended.state.stop_requested);

    // A restart keeps the id and continues the conversation the agent named.
    let plan = local.prepare_restart("one").unwrap();
    let plan: tower_agents::runtime::LauncherPlan =
        serde_json::from_slice(&std::fs::read(plan).unwrap()).unwrap();
    assert_ne!(plan.generation, ended.plan.generation);
    assert_eq!(plan.spec.resume.unwrap().id, "native-claude");
    assert_eq!(plan.spec.cwd, work.to_string_lossy());
    assert!(local.prepare_restart("absent").is_err());
    assert_eq!(
        local
            .conversation(tower_agents::Harness::ClaudeCode, None, "not/an/id")
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    local.forget("one").unwrap();
    assert!(local.sessions().unwrap().is_empty());
}

// Stands in for native Codex: its hooks run the helper the session names.
const CODEX: &str = r#"#!/bin/sh
if [ "$1" = queue ]; then exit 0; fi
printf '%s\n' "$@" > codex.args
env > codex.env
printf '{"session_id":"native-codex"}' | "$TOWER_HELPER" hook codex SessionStart
while :; do sleep 0.03; done
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_native_codex_shares_the_profile_without_the_mcp_server() {
    let root = fixture_root();
    let root = std::fs::canonicalize(root.path()).unwrap();
    let (state, bin, work) = (root.join("state"), root.join("bin"), root.join("work"));
    for dir in [&bin, &work] {
        std::fs::create_dir(dir).unwrap();
    }
    let agent = bin.join("codex");
    std::fs::write(&agent, CODEX).unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let helper = embedder();
    let local = Local::new(local_embedding(&helper, &state)).unwrap();
    let mut watch = local.watch().unwrap();
    let child = Command::new(&helper)
        .args(["launch", "one", "codex"])
        .arg(&work)
        .env_clear()
        .env("HOME", &root)
        .env("EMBEDDER_STATE", &state)
        .env("EMBEDDER_MODE", "native")
        .env(
            "PATH",
            format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
        )
        .stdin(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();
    let mut launcher = Launcher::new(child, local_embedding(&helper, &state));
    await_session(&local, &mut watch, "agent start", |s| {
        s.live
            && s.state
                .native
                .as_ref()
                .is_some_and(|n| n.id == "native-codex")
    })
    .await;
    let args = std::fs::read_to_string(work.join("codex.args")).unwrap();
    assert_eq!(args, "--profile\ntower\n--enable\nhooks\n");
    let env = std::fs::read_to_string(work.join("codex.env")).unwrap();
    assert!(
        env.contains(&format!("TOWER_HELPER={}\n", helper.display())),
        "{env}"
    );
    let profile = std::fs::read_to_string(root.join(".codex/tower.config.toml")).unwrap();
    assert!(profile.contains("TOWER_HELPER"), "{profile}");
    assert!(!profile.contains("mcp_servers"), "{profile}");

    let stopper = Local::new(local_embedding(&helper, &state)).unwrap();
    tokio::task::spawn_blocking(move || stopper.stop("one"))
        .await
        .unwrap()
        .unwrap();
    tokio::task::spawn_blocking(move || launcher.wait())
        .await
        .unwrap()
        .unwrap();
    await_session(&local, &mut watch, "exit record", |s| !s.live).await;
}
fn local_embedding(helper: &Path, state: &Path) -> Embedding {
    Embedding {
        helper: helper.to_string_lossy().into_owned(),
        state: state.to_string_lossy().into_owned(),
        codex_endpoint: None,
    }
}
