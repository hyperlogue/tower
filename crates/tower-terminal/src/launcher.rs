use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
use tower_agents::{
    Harness,
    events::{self, AgentEvent},
    runtime::{AgentDetail, LauncherPlan, RuntimeState},
};
use tower_pal::SingletonLock;

#[derive(Serialize, Deserialize)]
pub(crate) struct Report {
    pub(crate) token: String,
    pub(crate) harness: Harness,
    pub(crate) event: Option<AgentEvent>,
    #[serde(default)]
    pub(crate) stop: bool,
    #[serde(default)]
    pub(crate) delivery: Option<tower_protocol::DeliveryEndpoint>,
    #[serde(default)]
    pub(crate) message: Option<String>,
    #[serde(default)]
    pub(crate) pty_only: bool,
}
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
/// A fresh launch of `id` running `spec`, with its directories created below
/// `paths`. An embedded launch has no daemon socket.
pub(crate) fn plan(
    paths: &tower_pal::Paths,
    id: &str,
    spec: tower_agents::runtime::AgentSpec,
    embedding: Option<tower_agents::runtime::Embedding>,
) -> io::Result<LauncherPlan> {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).map_err(|_| io::Error::other("random source unavailable"))?;
    let generation: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let root = paths.state.join("sessions").join(id).join(&generation);
    let run = paths.run.join("agents");
    tower_pal::private_dir(&root)?;
    tower_pal::private_dir(&run)?;
    Ok(LauncherPlan {
        id: id.into(),
        state_dir: root.to_string_lossy().into_owned(),
        tower_socket: if embedding.is_some() {
            String::new()
        } else {
            paths.socket.to_string_lossy().into_owned()
        },
        control_socket: run
            .join(format!("{generation}.sock"))
            .to_string_lossy()
            .into_owned(),
        generation,
        spec,
        continue_turn: false,
        embedding,
    })
}
/// `spec` changed to continue the conversation its last launch recorded in
/// `state`, or why that conversation cannot be continued.
pub(crate) fn continued(
    mut spec: tower_agents::runtime::AgentSpec,
    state: RuntimeState,
) -> Result<tower_agents::runtime::AgentSpec, &'static str> {
    if state.private.get("native_resumable") == Some(&serde_json::Value::Bool(false)) {
        return Err("This native conversation is ephemeral");
    }
    spec.resume = Some(
        state
            .native
            .ok_or("The agent has not reported a native session identity")?,
    );
    spec.native_home = state
        .private
        .get("native_home")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .or(spec.native_home);
    spec.cwd = state.cwd;
    Ok(spec)
}
fn file(plan: &LauncherPlan) -> PathBuf {
    Path::new(&plan.state_dir).join("state.json")
}
pub fn read_state(path: &Path) -> io::Result<RuntimeState> {
    Ok(serde_json::from_slice(&tower_pal::read_private(path)?)?)
}
fn save(plan: &LauncherPlan, state: &RuntimeState) -> io::Result<()> {
    tower_pal::write_private(&file(plan), &serde_json::to_vec(state)?)
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
    report(
        Path::new(&socket),
        &Report {
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
fn report(path: &Path, report: &Report) -> io::Result<()> {
    let bytes = serde_json::to_vec(report)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            tokio::time::timeout(
                if report.stop {
                    Duration::from_secs(40)
                } else {
                    Duration::from_millis(400)
                },
                async {
                    let mut stream = tower_pal::connect(path).await?;
                    stream.write_u32(bytes.len() as u32).await?;
                    stream.write_all(&bytes).await?;
                    if stream.read_u8().await? != 1 {
                        return Err(io::Error::other("launcher rejected report"));
                    }
                    Ok(())
                },
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "launcher report timed out"))?
        })
}
pub fn stop(plan: &LauncherPlan, token: &str) -> io::Result<()> {
    report(
        Path::new(&plan.control_socket),
        &Report {
            token: token.into(),
            harness: plan.spec.harness,
            event: None,
            stop: true,
            delivery: None,
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
    super::session::drive(
        super::agent::task(context),
        updates,
        commands,
        state,
        terminated,
        |state| match &mut attention {
            Some(attention) => {
                let mut state = state.clone();
                attention.note(&mut state);
                save(&plan, &state)
            }
            None => save(&plan, state),
        },
    )
    .await
}

pub fn send_message(
    plan: &LauncherPlan,
    token: &str,
    text: &str,
    pty_only: bool,
) -> tower_sessions::Result<tower_protocol::DeliveryReceipt> {
    let report = Report {
        token: token.into(),
        harness: plan.spec.harness,
        event: None,
        stop: false,
        delivery: None,
        message: Some(text.into()),
        pty_only,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| {
            tower_protocol::RpcError::application("internal_error", "Cannot start delivery worker")
        })?;
    runtime.block_on(async {
        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            tower_pal::connect(Path::new(&plan.control_socket)),
        )
        .await
        .map_err(|_| {
            tower_protocol::RpcError::application(
                "delivery_failed",
                "Launcher connection timed out",
            )
        })?
        .map_err(|_| {
            tower_protocol::RpcError::application("delivery_failed", "Launcher is unavailable")
        })?;
        let result: io::Result<tower_sessions::Result<Option<tower_protocol::DeliveryReceipt>>> =
            tokio::time::timeout(Duration::from_secs(14), async {
                let bytes = serde_json::to_vec(&report)?;
                stream.write_u32(bytes.len() as u32).await?;
                stream.write_all(&bytes).await?;
                let n = stream.read_u32().await? as usize;
                if n > 65536 {
                    return Err(io::Error::other("oversized launcher response"));
                }
                let mut bytes = vec![0; n];
                stream.read_exact(&mut bytes).await?;
                Ok(serde_json::from_slice(&bytes)?)
            })
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "launcher submission timed out",
                ))
            });
        result
            .map_err(|_| {
                tower_protocol::RpcError::application(
                    "delivery_unknown",
                    "Launcher response was lost; do not automatically retry",
                )
            })??
            .ok_or_else(|| {
                tower_protocol::RpcError::application(
                    "delivery_unknown",
                    "Launcher returned no receipt",
                )
            })
    })
}
