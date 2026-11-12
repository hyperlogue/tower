use crate::{
    launcher::{Report, now},
    session::*,
};
use std::{io, path::Path, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, mpsc},
};
use tower_agents::{
    environment::project_environment,
    events::{self, AgentEvent},
    runtime::{LauncherPlan, RuntimeState},
};
use tower_pal::IpcListener;

struct Configured {
    invocation: tower_agents::config::Invocation,
    token: String,
}
struct HookAdapter(Configured);
struct CodexAdapter {
    launch: Configured,
    config: tower_agents::codex_service::Config,
}
impl AgentAdapter for HookAdapter {
    fn run(self: Box<Self>, context: SessionContext) -> SessionTask {
        run(context, self.0, None)
    }
}
impl AgentAdapter for CodexAdapter {
    fn run(self: Box<Self>, context: SessionContext) -> SessionTask {
        run(context, self.launch, Some(self.config))
    }
}
pub(crate) fn task(context: SessionContext) -> SessionTask {
    Box::pin(async move {
        let mut context = context;
        let token = std::mem::take(&mut context.token);
        match configure(&context.plan, &mut context.state, token).await {
            Ok(adapter) => adapter.run(context).await,
            Err(error) => Ok(failed(context.state, error)),
        }
    })
}
/// What hosts this launcher: Tower's own daemon and pool, or an embedding
/// program's terminal window.
struct Host {
    exe: std::path::PathBuf,
    paths: tower_pal::Paths,
    daemon: bool,
}
fn host(plan: &LauncherPlan) -> io::Result<Host> {
    Ok(match &plan.embedding {
        Some(embedding) => Host {
            exe: embedding.helper.clone().into(),
            paths: tower_pal::Paths::rooted(embedding.state.clone().into())?,
            daemon: false,
        },
        None => Host {
            exe: tower_pal::Program::Cli.path()?,
            paths: tower_pal::Paths::discover()?,
            daemon: true,
        },
    })
}
async fn configure(
    plan: &LauncherPlan,
    state: &mut RuntimeState,
    token: String,
) -> io::Result<Box<dyn AgentAdapter>> {
    let Host { exe, paths, daemon } = host(plan)?;
    let mut env =
        project_environment(Path::new(&plan.spec.cwd), &exe, tower_pal::environment()).await?;
    let config = match plan.spec.harness.definition().execution(&plan.spec) {
        tower_agents::definition::Execution::Hooks => None,
        tower_agents::definition::Execution::CodexAppServer => {
            Some(tower_agents::codex_service::Config::load(&paths).map_err(io::Error::other)?)
        }
    };
    let native_home = match &config {
        Some(c) => Some(format!("codex-endpoint:{}", c.endpoint.display())),
        None => tower_agents::inventory::namespace(plan.spec.harness, &env),
    };
    if plan.spec.resume.is_some()
        && plan
            .spec
            .native_home
            .as_ref()
            .is_some_and(|expected| Some(expected) != native_home.as_ref())
    {
        state.detail.error = Some("native_home_changed".into());
        return Err(io::Error::other("native home changed"));
    }
    for key in [
        "TOWER_AGENT_ID",
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_LAUNCHER_SOCKET",
        tower_agents::config::HELPER,
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
    // Integrations that call back into the daemon key off this variable.
    if daemon {
        env.insert("TOWER_SOCKET".into(), plan.tower_socket.clone());
    } else {
        env.remove("TOWER_SOCKET");
    }
    env.insert("TOWER_LAUNCHER_SOCKET".into(), plan.control_socket.clone());
    env.insert(
        tower_agents::config::HELPER.into(),
        exe.to_string_lossy().into_owned(),
    );
    let mut invocation = tower_agents::config::prepare(
        &plan.spec,
        &Path::new(&plan.state_dir).join("config"),
        &exe,
        env,
    )?;
    if plan.spec.harness == tower_agents::Harness::Codex && config.is_none() {
        let queue =
            tower_agents::delivery::queue_supported(&invocation.env, Path::new(&plan.spec.cwd))
                .await;
        state.private.insert("queue_supported".into(), queue.into());
    }
    // The shim reaches the pool's clipboard; a plain terminal has its own.
    if daemon && super::clipboard_mode(plan.spec.harness).is_some() {
        tower_clipboard::shim::install(
            Path::new(&plan.state_dir),
            &exe,
            &mut invocation.env,
            &plan.generation,
        )?;
    }
    if let Some(home) = native_home {
        state.private.insert("native_home".into(), home.into());
    }
    let configured = Configured { invocation, token };
    Ok(match config {
        Some(config) => Box::new(CodexAdapter {
            launch: configured,
            config,
        }),
        None => Box::new(HookAdapter(configured)),
    })
}
fn failed(mut state: RuntimeState, error: io::Error) -> SessionExit {
    if state.detail.error.is_none() {
        state.detail.error = Some(
            match error.kind() {
                io::ErrorKind::NotFound => "agent_not_found",
                io::ErrorKind::PermissionDenied => "environment_not_approved",
                _ => "launch_failed",
            }
            .into(),
        );
    }
    eprintln!("tower: {}", state.detail.error.as_deref().unwrap());
    SessionExit { code: 1, state }
}
fn run(
    mut context: SessionContext,
    configured: Configured,
    codex: Option<tower_agents::codex_service::Config>,
) -> SessionTask {
    Box::pin(async move {
        let result = serve(
            &context.plan,
            &mut context.state,
            configured,
            codex,
            &context.publisher,
            &mut context.commands,
        )
        .await;
        Ok(match result {
            Ok(code) => SessionExit {
                code,
                state: context.state,
            },
            Err(error) => failed(context.state, error),
        })
    })
}
async fn serve(
    plan: &LauncherPlan,
    state: &mut RuntimeState,
    configured: Configured,
    codex: Option<tower_agents::codex_service::Config>,
    publisher: &SessionPublisher,
    commands: &mut SessionCommands,
) -> io::Result<i32> {
    let mut invocation = configured.invocation;
    let token = configured.token;
    let Host { paths, daemon, .. } = host(plan)?;
    state.private.insert(
        "pty_supported".into(),
        (daemon && super::injection::supported(&paths).await).into(),
    );
    let listener = IpcListener::bind(Path::new(&plan.control_socket)).await?;
    let (observations, mut observed) = mpsc::channel(128);
    let mut server = if let Some(config) = codex {
        match super::app_server::start(plan, &paths, &mut invocation, observations, config).await {
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
        publisher.publish(state).await?;
    }
    if commands.requested() {
        state.stop_requested = true;
        return Ok(143);
    }
    let mut child =
        tower_pal::spawn_inherited(&invocation.argv, &invocation.env, Path::new(&plan.spec.cwd))?;
    let mut activity = plan.spec.harness.definition().hook_session(&invocation.env);
    activity.start();
    state.agent_pid = child.id();
    state.agent_start_time = child
        .id()
        .and_then(|p| tower_pal::process_identity(p).ok())
        .map(|p| p.start_time);
    state.status = "unknown".into();
    state.updated_at = now();
    publisher.publish(state).await?;
    let (tx, mut rx) = mpsc::channel::<(
        Report,
        tokio::sync::oneshot::Sender<
            tower_sessions::Result<Option<tower_protocol::DeliveryReceipt>>,
        >,
    )>(64);
    let permits = Arc::new(Semaphore::new(32));
    if server.is_none() && activity.refresh(state, now()) {
        publisher.publish(state).await?;
    }
    let mut accept_after = tokio::time::Instant::now();
    let mut child_exit = None;
    let mut stop_confirmed = false;
    let mut stop_signal = false;
    loop {
        tokio::select! {
            _=commands.stop()=>{stop_signal=true;}
            _=activity.changed()=>{},
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
                publisher.publish(state).await?;
            }
            accepted=async {tokio::time::sleep_until(accept_after).await;listener.accept().await}=>{
                if accepted.is_err() {accept_after=tokio::time::Instant::now()+Duration::from_millis(250);}
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
                        if !daemon || !super::injection::allowed(&paths,plan.spec.harness){let _=ack.send(Err(tower_protocol::RpcError::application("unsupported","PTY injection is disabled")));continue;}
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
                    if publisher.write(state).await.is_err() {
                        let _=ack.send(Err(tower_protocol::RpcError::application("stop_unconfirmed", "Cannot persist stop intent; retry cleanup")));
                        continue;
                    }
                    let cleaned=match &mut server {Some(server)=>server.shutdown().await.is_ok(),None=>true};
                    drain_observations(&mut observed,state);
                    if !cleaned {
                        state.status = "unknown".into();
                        state.detail.error = Some("codex_cleanup_unconfirmed".into());
                        state.seq = state.seq.saturating_add(1);
                        publisher.publish(state).await?;
                        let _=ack.send(Err(tower_protocol::RpcError::application("stop_unconfirmed","Codex thread cleanup is unconfirmed")));
                        continue;
                    }
                    state.private.insert("codex_cleanup_confirmed".into(), true.into());
                    if publisher.write(state).await.is_err() {
                        let _=ack.send(Err(tower_protocol::RpcError::application("stop_unconfirmed", "Cannot persist cleanup confirmation; retry cleanup")));
                        continue;
                    }
                    stop_confirmed = true;
                    let _=ack.send(Ok(None));
                    if let Some(code)=child_exit {return Ok(code);}
                    tower_pal::terminate_own_group()?;
                    continue;
                }else if server.is_none() && let Some(event)=report.event
                    && !(plan.spec.fork && plan.spec.resume.as_ref().is_some_and(|parent|event.native_id.as_ref()==Some(&parent.id))) {
                    let kind=event.kind;let child=event.child;let before=state.seq;let was_idle=state.status=="idle";activity.apply(state,event,now());
                    if state.seq!=before && !child {
                        super::injection::observe_idle(state,kind,was_idle);
                        if daemon { continue_turn(&paths,plan,state); }
                        if let Some(endpoint)=report.delivery && tower_agents::delivery::validate(&endpoint).is_ok() && endpoint.messaging_socket.is_some() && endpoint.messaging_token.is_some() {
                            state.private.insert("delivery".into(),serde_json::to_value(endpoint)?);
                        }
                    }
                }
                publisher.publish(state).await?; let _=ack.send(Ok(None));
            }
            Some(event)=observed.recv(), if server.is_some()=>{
                let kind=event.kind;let before=state.seq;let was_idle=state.status=="idle";activity.apply(state,event,now());if state.seq!=before{super::injection::observe_idle(state,kind,was_idle);}publisher.publish(state).await?;
            }
        }
        if server.is_none() && activity.refresh(state, now()) {
            publisher.publish(state).await?;
        }
        if stop_signal && !stop_confirmed {
            state.stop_requested = true;
            state.updated_at = now();
            if publisher.write(state).await.is_err() {
                continue;
            }
            let cleaned = match &mut server {
                Some(server) => server.shutdown().await.is_ok(),
                None => true,
            };
            drain_observations(&mut observed, state);
            if !cleaned {
                state.status = "unknown".into();
                state.detail.error = Some("codex_cleanup_unconfirmed".into());
                state.seq = state.seq.saturating_add(1);
                publisher.publish(state).await?;
                continue;
            }
            state
                .private
                .insert("codex_cleanup_confirmed".into(), true.into());
            if publisher.write(state).await.is_err() {
                continue;
            }
            stop_confirmed = true;
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
            publisher.publish(state).await?;
        }
    }
}

fn drain_observations(observed: &mut mpsc::Receiver<AgentEvent>, state: &mut RuntimeState) {
    while let Ok(event) = observed.try_recv() {
        events::apply(state, event, now());
    }
}

/// Asks a resumed agent to continue the turn its previous incarnation was
/// running, once, as soon as it accepts input.
fn continue_turn(paths: &tower_pal::Paths, plan: &LauncherPlan, state: &mut RuntimeState) {
    if !plan.continue_turn
        || state.private.contains_key("continued")
        || !super::injection::allowed(paths, plan.spec.harness)
    {
        return;
    }
    let Ok(proof) = super::injection::proof(state) else {
        return;
    };
    state.private.insert("continued".into(), true.into());
    let (paths, name) = (paths.clone(), plan.id.clone());
    tokio::spawn(async move {
        // A just-started agent can report readiness before the pool has seen
        // it enable bracketed paste.
        for _ in 0..40 {
            let mut proof = proof.clone();
            proof.checked_at = tower_pal::monotonic_ms();
            let text = tower_agents::runtime::CONTINUE.to_owned();
            match super::injection::send(paths.clone(), name.clone(), proof, text).await {
                Err(e)
                    if matches!(
                        e.data.details.get("reason"),
                        Some(tower_protocol::Value::String(r)) if r == "target_busy_pty"
                    ) =>
                {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                _ => return,
            }
        }
    });
}
