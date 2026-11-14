//! What a launcher and the programs that start, watch and stop it agree on:
//! the plan a launch is described by, the state file it keeps, and the
//! messages its control socket takes.

use super::{AgentSpec, Embedding, LauncherPlan, NativeSession, RuntimeState};
use crate::{Harness, events::AgentEvent};
use serde::{Deserialize, Serialize};
use std::{io, path::Path, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower_protocol::RpcError;
use tower_sessions::Result;

fn error(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}
/// A message on a launcher's control socket.
#[derive(Serialize, Deserialize)]
pub struct Report {
    pub token: String,
    pub harness: Harness,
    pub event: Option<AgentEvent>,
    #[serde(default)]
    pub stop: bool,
    #[serde(default)]
    pub delivery: Option<tower_protocol::DeliveryEndpoint>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub pty_only: bool,
}
/// A fresh launch of `id` running `spec`, with its directories created below
/// `paths`. An embedded launch has no daemon socket.
pub fn plan(
    paths: &tower_pal::Paths,
    id: &str,
    spec: AgentSpec,
    embedding: Option<Embedding>,
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
pub fn continued(
    mut spec: AgentSpec,
    state: RuntimeState,
) -> std::result::Result<AgentSpec, &'static str> {
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
pub fn read_state(path: &Path) -> io::Result<RuntimeState> {
    Ok(serde_json::from_slice(&tower_pal::read_private(path)?)?)
}
/// Sends `report` to the launcher listening at `path` and waits for it to be taken.
pub fn report(path: &Path, report: &Report) -> io::Result<()> {
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
pub fn send_message(
    plan: &LauncherPlan,
    token: &str,
    text: &str,
    pty_only: bool,
) -> Result<tower_protocol::DeliveryReceipt> {
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
        let result: io::Result<Result<Option<tower_protocol::DeliveryReceipt>>> =
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

/// Whether the launcher that wrote `state`, or its agent, still runs.
pub fn launcher_alive(state: &RuntimeState) -> Result<bool> {
    if state.boot_id
        != tower_pal::boot_id()
            .map_err(|_| error("backend_unavailable", "Cannot verify boot identity"))?
    {
        return Ok(false);
    }
    for (pid, start) in [
        (Some(state.launcher_pid), Some(state.launcher_start_time)),
        (state.agent_pid, state.agent_start_time),
    ] {
        if let (Some(pid), Some(start)) = (pid, start) {
            match tower_pal::process_identity(pid) {
                Ok(p) if p.start_time == start => return Ok(true),
                Err(e)
                    if !matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                    ) =>
                {
                    return Err(error(
                        "backend_unavailable",
                        "Cannot verify previous process exit",
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(false)
}
/// Whether the user's configuration lets messages for `harness` be typed into its terminal.
pub fn injection_allowed(paths: &tower_pal::Paths, harness: Harness) -> bool {
    if harness == Harness::Codex {
        return false;
    }
    let Ok(config) = tower_pal::read_config(&paths.config_home.join("tower/config.toml")) else {
        return false;
    };
    let config: toml::Table = match config.map(|s| s.parse()).transpose() {
        Ok(c) => c.unwrap_or_default(),
        Err(_) => return false,
    };
    config
        .get("inject")
        .and_then(|v| v.get(harness.id()))
        .and_then(|v| v.get("pty"))
        .map(|v| v.as_bool().unwrap_or(false))
        .unwrap_or(harness == Harness::ClaudeCode)
}
/// How a harness takes an image that its user pastes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClipboardPaste {
    FileUrl,
    NativeKey,
}
pub fn clipboard_paste(harness: Harness) -> Option<ClipboardPaste> {
    match harness {
        Harness::Codex => Some(ClipboardPaste::FileUrl),
        Harness::ClaudeCode if tower_pal::operating_system() == "linux" => {
            Some(ClipboardPaste::NativeKey)
        }
        _ => None,
    }
}
/// Reads the conversation `id` from the Codex app-server at `config`.
pub async fn codex_thread(
    paths: &tower_pal::Paths,
    config: crate::codex_service::Config,
    id: &str,
) -> crate::codex_service::Result<crate::inventory::Candidate> {
    let host = crate::codex_host::ensure_configured(paths, config).await?;
    let mut client = crate::codex_service::Client::connect(&host.config.endpoint).await?;
    let response = client
        .request(
            "thread/read",
            serde_json::json!({"threadId":id,"includeTurns":false}),
        )
        .await?;
    codex_candidate(&response["thread"], &host.config.endpoint)
        .filter(|candidate| candidate.native.id == id)
        .ok_or(crate::codex_service::Error::Protocol)
}
/// Runs a Codex inventory read to completion on the calling thread.
pub fn codex_block<T>(
    operation: impl std::future::Future<Output = crate::codex_service::Result<T>>,
) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| error("backend_unavailable", "Cannot start Codex inventory reader"))?
        .block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(20), operation)
                .await
                .map_err(|_| error("conversation_unavailable", "Host Codex inventory timed out"))?
                .map_err(|_| {
                    error(
                        "conversation_unavailable",
                        "Host Codex inventory is unavailable",
                    )
                })
        })
}
/// The resumable conversation an app-server thread row describes, if it is one.
pub fn codex_candidate(
    row: &serde_json::Value,
    endpoint: &std::path::Path,
) -> Option<crate::inventory::Candidate> {
    if row["ephemeral"] == true
        || !row["parentThreadId"].is_null()
        || row["threadSource"]
            .as_str()
            .is_some_and(|source| source != "user")
    {
        return None;
    }
    let id = row["id"]
        .as_str()
        .filter(|id| crate::events::native_id(id))?;
    let cwd = row["cwd"]
        .as_str()
        .filter(|cwd| cwd.len() <= 4096 && std::path::Path::new(cwd).is_absolute())?;
    let updated_at = row["updatedAt"].as_u64()?.saturating_mul(1000);
    Some(crate::inventory::Candidate {
        harness: Harness::Codex,
        native: NativeSession {
            id: id.into(),
            resume_ref: None,
        },
        cwd: cwd.into(),
        title: row["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .map(|name| name.chars().take(200).collect()),
        updated_at,
        namespace: Some(format!("codex-endpoint:{}", endpoint.display())),
    })
}
