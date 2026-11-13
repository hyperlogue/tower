use std::{
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use tower_agents::runtime::Embedding;
use tower_terminal::local::{Local, Session};

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
    // Cargo builds examples for a package's whole test run, not for `--test`.
    assert!(
        path.exists(),
        "build the fixture first: cargo build -p tower-terminal --example embedder"
    );
    path
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
            if let Some(session) = local.sessions().unwrap().into_iter().find(&ready) {
                return session;
            }
            watch.changed().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out awaiting {phase}: {:?}", local.sessions()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedder_runs_watches_and_stops_a_session_without_daemon_or_pool() {
    let root = tempfile::tempdir().unwrap();
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

    let mut launcher = Command::new(&helper)
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
    let root = tempfile::tempdir().unwrap();
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
    let mut launcher = Command::new(&helper)
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
