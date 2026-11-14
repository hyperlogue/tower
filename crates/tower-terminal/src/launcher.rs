use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tower_agents::{
    Harness, events,
    runtime::{AgentDetail, LauncherPlan, RuntimeState},
};
use tower_pal::SingletonLock;

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn file(plan: &LauncherPlan) -> PathBuf {
    Path::new(&plan.state_dir).join("state.json")
}
fn save(plan: &LauncherPlan, state: &RuntimeState) -> io::Result<()> {
    tower_pal::write_private(&file(plan), &serde_json::to_vec(state)?)
}
/// What a resume after a crash or power loss depends on. The rest of the
/// state describes the running agent and is reported again as it changes.
fn lasting(state: &RuntimeState) -> impl PartialEq + use<> {
    (
        state.native.as_ref().map(|n| n.id.clone()),
        state.cwd.clone(),
        state.agent_pid,
        state.agent_start_time,
        state.stop_requested,
        state.exit_code,
        state.private.clone(),
    )
}

pub fn hook(harness: &str, event: &str) {
    let _ = send_hook(harness, event);
}
fn send_hook(harness: &str, event: &str) -> io::Result<()> {
    let Some(harness) = Harness::parse(harness) else {
        return Ok(());
    };
    let (Ok(socket), Ok(token)) = (
        std::env::var("TOWER_LAUNCHER_SOCKET"),
        std::env::var("TOWER_AGENT_TOKEN"),
    ) else {
        return Ok(());
    };
    let bytes = tower_pal::read_stdin_bounded(1024 * 1024, Duration::from_millis(500))?;
    let event = events::parse(
        harness,
        event,
        if bytes.is_empty() { b"{}" } else { &bytes },
    )
    .map_err(io::Error::other)?;
    tower_agents::runtime::report(
        Path::new(&socket),
        &tower_agents::runtime::Report {
            token,
            harness,
            event: Some(event),
            stop: false,
            delivery: (harness == Harness::ClaudeCode).then(|| tower_protocol::DeliveryEndpoint {
                messaging_socket: std::env::var("CLAUDE_CODE_MESSAGING_SOCKET").ok(),
                messaging_token: std::env::var("CLAUDE_CODE_MESSAGING_TOKEN")
                    .ok()
                    .map(tower_protocol::Secret),
                ..Default::default()
            }),
            message: None,
            pty_only: false,
        },
    )
}
pub use tower_agents::environment::project_environment;
pub async fn run(path: &Path) -> io::Result<i32> {
    if !tower_pal::is_group_leader() {
        return Err(io::Error::other(
            "agent supervisor must run in an owned session",
        ));
    }
    let plan: LauncherPlan = serde_json::from_slice(&tower_pal::read_private(path)?)?;
    let token = match &plan.embedding {
        Some(embedding) => String::from_utf8(tower_pal::read_private(
            &tower_pal::Paths::rooted(embedding.state.clone().into())?.token_path(&plan.id)?,
        )?)
        .map_err(io::Error::other)?,
        None => std::env::var("TOWER_AGENT_TOKEN")
            .map_err(|_| io::Error::other("missing session credential"))?,
    };
    if token.is_empty() {
        return Err(io::Error::other("empty credential"));
    }
    let _lock = SingletonLock::acquire(&Path::new(&plan.state_dir).join("launcher.lock"))?;
    let terminated = tower_pal::Termination::new()?.into_channel();
    let identity = tower_pal::process_identity(tower_pal::pid())?;
    let mut state = RuntimeState {
        id: plan.id.clone(),
        generation: plan.generation.clone(),
        harness: plan.spec.harness,
        status: "starting".into(),
        cwd: plan.spec.cwd.clone(),
        native: if plan.spec.fork {
            None
        } else {
            plan.spec.resume.clone()
        },
        display_name: None,
        detail: AgentDetail::default(),
        launcher_pid: identity.pid,
        launcher_start_time: identity.start_time,
        boot_id: tower_pal::boot_id()?,
        agent_pid: None,
        agent_start_time: None,
        seq: 0,
        updated_at: now(),
        stop_requested: false,
        exit_code: None,
        attention: None,
        version: Some(env!("CARGO_PKG_VERSION").into()),
        private: Default::default(),
    };
    if plan.spec.fork
        && let Some(parent) = &plan.spec.resume
    {
        state
            .private
            .insert("fork_parent".into(), parent.id.clone().into());
    }
    if plan.spec.args.iter().any(|a| {
        matches!(
            a.as_str(),
            "--ephemeral" | "--no-session" | "--no-session-persistence"
        )
    }) {
        state
            .private
            .insert("native_resumable".into(), false.into());
    }
    save(&plan, &state)?;
    let (publisher, updates) = super::session::SessionPublisher::channel();
    let (commands, receiver) = mpsc::channel(1);
    let context = super::session::SessionContext {
        plan: plan.clone(),
        token,
        state: state.clone(),
        publisher,
        commands: super::session::SessionCommands(receiver),
    };
    let mut attention = plan
        .embedding
        .is_some()
        .then(|| super::session::Attention::new(&state));
    // Status changes on every tool call; only a change to what a resume
    // needs waits for the disk.
    let mut saved = Some(lasting(&state));
    super::session::drive(
        super::agent::task(context),
        updates,
        commands,
        state,
        terminated,
        |state| {
            let noted = attention.as_mut().map(|attention| {
                let mut state = state.clone();
                attention.note(&mut state);
                state
            });
            let state = noted.as_ref().unwrap_or(state);
            let now = Some(lasting(state));
            if now == saved {
                return tower_pal::write_private_unsynced(
                    &file(&plan),
                    &serde_json::to_vec(state)?,
                );
            }
            // A failed write is retried in full.
            saved = None;
            save(&plan, state)?;
            saved = now;
            Ok(())
        },
    )
    .await
}
