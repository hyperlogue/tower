use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, mpsc},
};
use tower_agents::{
    Harness,
    events::{self, AgentEvent},
    runtime::{AgentDetail, LauncherPlan, RuntimeState},
};
use tower_pal::{IpcListener, SingletonLock};

#[derive(Serialize, Deserialize)]
struct Report {
    token: String,
    harness: Harness,
    event: Option<AgentEvent>,
    #[serde(default)]
    stop: bool,
    #[serde(default)]
    delivery: Option<tower_protocol::DeliveryEndpoint>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    pty_only: bool,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
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

fn drain_observations(observed: &mut mpsc::Receiver<AgentEvent>, state: &mut RuntimeState) {
    while let Ok(event) = observed.try_recv() {
        events::apply(state, event, now());
    }
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
    let _lock = SingletonLock::acquire(&Path::new(&plan.state_dir).join("launcher.lock"))?;
    let terminated = tower_pal::termination_flag()?;
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
    let result = serve(&plan, &mut state, terminated).await;
    let code = match result {
        Ok(code) => code,
        Err(error) => {
            state.detail.error = Some(
                match error.kind() {
                    io::ErrorKind::NotFound => "agent_not_found",
                    io::ErrorKind::PermissionDenied => "environment_not_approved",
                    _ => "launch_failed",
                }
                .into(),
            );
            eprintln!(
                "tower: {}",
                state.detail.error.as_deref().unwrap_or("launch_failed")
            );
            1
        }
    };
    state.status = "exited".into();
    state.exit_code = Some(code);
    state.seq = state.seq.saturating_add(1);
    state.updated_at = now();
    save(&plan, &state)?;
    Ok(code)
}
async fn serve(
    plan: &LauncherPlan,
    state: &mut RuntimeState,
    terminated: Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<i32> {
    let token = std::env::var("TOWER_AGENT_TOKEN")
        .map_err(|_| io::Error::other("missing session credential"))?;
    if token.is_empty() {
        return Err(io::Error::other("empty credential"));
    }
    let exe = tower_pal::Program::Cli.path()?;
    let mut env =
        project_environment(Path::new(&plan.spec.cwd), &exe, tower_pal::environment()).await?;
    let native_home = if plan.spec.harness == Harness::Codex {
        Some(
            tower_agents::codex_host::namespace(&tower_pal::Paths::discover()?)
                .map_err(io::Error::other)?,
        )
    } else {
        tower_agents::inventory::namespace(plan.spec.harness, &env)
    };
    if plan.spec.resume.is_some()
        && plan
            .spec
            .native_home
            .as_ref()
            .is_some_and(|expected| Some(expected) != native_home.as_ref())
    {
        state.detail.error = Some("native_home_changed".into());
        return Ok(1);
    }
    if let Some(home) = native_home {
        state.private.insert("native_home".into(), home.into());
    }
    for key in [
        "TOWER_AGENT_ID",
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_LAUNCHER_SOCKET",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_CHILD_SESSION",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "CLAUDE_PID",
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
    ] {
        env.remove(key);
    }
    env.insert("TOWER_AGENT_ID".into(), plan.id.clone());
    env.insert("TOWER_AGENT_TOKEN".into(), token.clone());
    env.insert("TOWER_SOCKET".into(), plan.tower_socket.clone());
    env.insert("TOWER_LAUNCHER_SOCKET".into(), plan.control_socket.clone());
    let mut invocation = tower_agents::config::prepare(
        &plan.spec,
        &Path::new(&plan.state_dir).join("config"),
        &exe,
        env,
    )?;
    if super::clipboard_mode(plan.spec.harness).is_some() {
        tower_clipboard::shim::install(
            Path::new(&plan.state_dir),
            &exe,
            &mut invocation.env,
            &plan.generation,
        )?;
    }
    let paths = tower_pal::Paths::discover()?;
    state.private.insert(
        "pty_supported".into(),
        super::injection::supported(&paths).await.into(),
    );
    let listener = IpcListener::bind(Path::new(&plan.control_socket)).await?;
    let (observations, mut observed) = mpsc::channel(128);
    let mut server = if plan.spec.mode.as_deref() == Some("app-server") {
        match super::app_server::start(plan, &mut invocation, observations).await {
            Ok(server) => Some(server),
            Err(e) => {
                // The session's terminal is where the person launching it looks.
                eprintln!("tower: {e}");
                state.detail.error = Some("app_server_unavailable".into());
                return Ok(1);
            }
        }
    } else {
        None
    };
    if let Some(identity) = server.as_ref().and_then(|server| server.process) {
        state
            .private
            .insert("server_pid".into(), serde_json::json!(identity.pid));
        state.private.insert(
            "server_start_time".into(),
            serde_json::json!(identity.start_time),
        );
        save(plan, state)?;
    }
    if terminated.load(Ordering::Relaxed) {
        state.stop_requested = true;
        return Ok(143);
    }
    let mut child =
        tower_pal::spawn_inherited(&invocation.argv, &invocation.env, Path::new(&plan.spec.cwd))?;
    let mut activity = tower_agents::activity::Activity::new(plan.spec.harness, &invocation.env);
    state.agent_pid = child.id();
    state.agent_start_time = child
        .id()
        .and_then(|p| tower_pal::process_identity(p).ok())
        .map(|p| p.start_time);
    state.status = "unknown".into();
    state.updated_at = now();
    save(plan, state)?;
    let (tx, mut rx) = mpsc::channel::<(
        Report,
        tokio::sync::oneshot::Sender<
            tower_sessions::Result<Option<tower_protocol::DeliveryReceipt>>,
        >,
    )>(64);
    let permits = Arc::new(Semaphore::new(32));
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut child_exit = None;
    let mut stop_confirmed = false;
    loop {
        tokio::select! {
            result=child.wait(), if child_exit.is_none()=>{
                child_exit = Some(result?.code().unwrap_or(128));
                if stop_confirmed || server.is_none() { return Ok(child_exit.unwrap()); }
                state.stop_requested = true;
                let cleaned = server.as_mut().unwrap().shutdown().await.is_ok();
                drain_observations(&mut observed,state);
                if cleaned {
                    state.private.insert("codex_cleanup_confirmed".into(), true.into());
                    return Ok(child_exit.unwrap());
                }
                state.status = "unknown".into();
                state.detail.error = Some("codex_cleanup_unconfirmed".into());
                state.seq = state.seq.saturating_add(1);
                state.updated_at = now();
                save(plan, state)?;
            }
            accepted=listener.accept()=>{
                if let Ok((mut stream,_))=accepted && let Ok(permit)=permits.clone().try_acquire_owned() {
                    let tx=tx.clone();
                    tokio::spawn(async move {
                        let _permit=permit;
                        let _=tokio::time::timeout(Duration::from_secs(40),async {
                            let n=stream.read_u32().await? as usize;
                            if n>65536 {return Err(io::Error::other("report too large"));}
                            let mut bytes=vec![0;n];stream.read_exact(&mut bytes).await?;
                            let report:Report=serde_json::from_slice(&bytes)?;
                            let (ack,rx)=tokio::sync::oneshot::channel();
                            let message=report.message.is_some();
                            tx.try_send((report,ack)).map_err(io::Error::other)?;
                            let result=rx.await.unwrap_or_else(|_|Err(tower_protocol::RpcError::application("delivery_unknown","Launcher stopped during submission")));
                            if message {
                                let bytes=serde_json::to_vec(&result)?;
                                stream.write_u32(bytes.len() as u32).await?;
                                stream.write_all(&bytes).await
                            }else{stream.write_u8(u8::from(result.is_ok())).await}
                        }).await;
                    });
                }
            }
            Some((report,ack))=rx.recv()=>{
                if report.harness!=plan.spec.harness || !bool::from(report.token.as_bytes().ct_eq(token.as_bytes())) {let _=ack.send(Err(tower_protocol::RpcError::application("unauthorized","Invalid launcher credential")));continue;}
                if let Some(text)=report.message {
                    if text.len()>40*1024 || state.stop_requested {
                        let _=ack.send(Err(tower_protocol::RpcError::application("delivery_failed","Target session is unavailable"))); continue;
                    }
                    if report.pty_only {
                        if !super::injection::allowed(&paths,plan.spec.harness){let _=ack.send(Err(tower_protocol::RpcError::application("unsupported","PTY injection is disabled")));continue;}
                        match super::injection::proof(state) {
                            Ok(proof)=>{let paths=paths.clone();let name=plan.id.clone();tokio::spawn(async move{let _=ack.send(super::injection::send(paths,name,proof,text).await.map(Some));});}
                            Err(e)=>{let _=ack.send(Err(e));}
                        }
                        continue;
                    }
                    let thread=state.native.as_ref().map(|n|n.id.clone());
                    let relay=server.as_ref().map(|s|s.messages.clone());
                    let mut endpoint=state.private.get("delivery").cloned().and_then(|v|serde_json::from_value(v).ok()).unwrap_or_default();
                    let endpoint: &mut tower_protocol::DeliveryEndpoint=&mut endpoint;
                    endpoint.thread_id=thread.clone();
                    let endpoint=endpoint.clone();
                    let env=invocation.env.clone();let cwd=plan.spec.cwd.clone();let harness=plan.spec.harness;
                    tokio::spawn(async move {
                        let result=if let (Some(relay),Some(thread))=(relay,thread) {
                            let (sender,receiver)=tokio::sync::oneshot::channel();
                            if relay.try_send(super::app_server::Submission{thread,text,result:sender}).is_err(){Err(tower_protocol::RpcError::application("delivery_failed","Relay is unavailable or busy"))}else{
                                match tokio::time::timeout(Duration::from_secs(10),receiver).await {Ok(Ok(result))=>result,_=>Err(tower_protocol::RpcError::application("delivery_unknown","Relay submission outcome is unknown; do not automatically retry"))}
                            }
                        }else{tower_agents::delivery::send(harness.id(),&endpoint,&text,&env,Path::new(&cwd)).await};
                        let _=ack.send(result.map(Some));
                    });
                    continue;
                }
                if report.stop {
                    state.stop_requested = true;
                    state.updated_at = now();
                    save(plan,state)?;
                    let cleaned=match &mut server {Some(server)=>server.shutdown().await.is_ok(),None=>true};
                    drain_observations(&mut observed,state);
                    if !cleaned {
                        state.status = "unknown".into();
                        state.detail.error = Some("codex_cleanup_unconfirmed".into());
                        state.seq = state.seq.saturating_add(1);
                        save(plan,state)?;
                        let _=ack.send(Err(tower_protocol::RpcError::application("stop_unconfirmed","Codex thread cleanup is unconfirmed")));
                        continue;
                    }
                    stop_confirmed = true;
                    state.private.insert("codex_cleanup_confirmed".into(), true.into());
                    save(plan,state)?;
                    let _=ack.send(Ok(None));
                    if let Some(code)=child_exit {return Ok(code);}
                    tower_pal::terminate_own_group()?;
                    continue;
                }else if plan.spec.mode.as_deref()!=Some("app-server") && let Some(event)=report.event
                    && !(plan.spec.fork && plan.spec.resume.as_ref().is_some_and(|parent|event.native_id.as_ref()==Some(&parent.id))) {
                    let kind=event.kind;let before=state.seq;let was_idle=state.status=="idle";events::apply(state,event,now());
                    if state.seq!=before {
                        super::injection::observe_idle(state,kind,was_idle);
                        if let Some(endpoint)=report.delivery && tower_agents::delivery::validate(&endpoint).is_ok() && endpoint.messaging_socket.is_some() && endpoint.messaging_token.is_some() {
                            state.private.insert("delivery".into(),serde_json::to_value(endpoint)?);
                        }
                    }
                }
                save(plan,state)?; let _=ack.send(Ok(None));
            }
            Some(event)=observed.recv(), if server.is_some()=>{
                let kind=event.kind;let before=state.seq;let was_idle=state.status=="idle";events::apply(state,event,now());if state.seq!=before{super::injection::observe_idle(state,kind,was_idle);}save(plan,state)?;
            }
            _=tick.tick()=>{}
        }
        if server.is_none() && activity.refresh(state, now()) {
            save(plan, state)?;
        }
        if terminated.swap(false, Ordering::Relaxed) && !stop_confirmed {
            state.stop_requested = true;
            state.updated_at = now();
            save(plan, state)?;
            let cleaned = match &mut server {
                Some(server) => server.shutdown().await.is_ok(),
                None => true,
            };
            drain_observations(&mut observed, state);
            if !cleaned {
                state.status = "unknown".into();
                state.detail.error = Some("codex_cleanup_unconfirmed".into());
                state.seq = state.seq.saturating_add(1);
                save(plan, state)?;
                continue;
            }
            stop_confirmed = true;
            state
                .private
                .insert("codex_cleanup_confirmed".into(), true.into());
            save(plan, state)?;
            if let Some(code) = child_exit {
                return Ok(code);
            }
            tower_pal::terminate_own_group()?;
        }
        if server.as_ref().is_some_and(|server| !server.available()) && !state.stop_requested {
            state.stop_requested = true;
            state.status = "unknown".into();
            state.detail.error = Some("codex_relay_unavailable".into());
            state.seq = state.seq.saturating_add(1);
            state.updated_at = now();
            save(plan, state)?;
        }
    }
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
