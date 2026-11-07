use crate::{Binding, Journal, Result, engine, error, internal};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tower_protocol::{
    ExecutionCommand, ExecutionError, ExecutionResult, HarnessControls, Secret, SessionOpenParams,
};
use tower_sessions::{ManagedBackend, ManagedSession, Observation};

pub(crate) fn validate(b: &Binding, c: &ExecutionCommand) -> Result<()> {
    let p = serde_json::to_value(&c.payload).map_err(internal)?;
    match c.kind.as_str() {
        "launch"
            if p.as_object().is_some_and(|p| p.len() == 3)
                && p["protocol"] == "terminal"
                && p["harness"] == b.harness
                && p["cwd"].is_string()
                && b.resume_session_id.is_none() =>
        {
            Ok(())
        }
        "cancel" if p.as_object().is_some_and(|p| p.is_empty()) => Ok(()),
        "launch" | "cancel" => Err(error("invalid_params", "Invalid terminal command payload")),
        _ => Err(error(
            "unsupported_control",
            "Terminal runtimes support only launch and whole-runtime stop",
        )),
    }
}
fn public(v: impl serde::Serialize) -> Result<tower_protocol::Value> {
    serde_json::from_value(serde_json::to_value(v).map_err(internal)?).map_err(internal)
}
fn failure(code: &str, stage: &str, effect: bool) -> ExecutionError {
    ExecutionError {
        code: code.into(),
        message: "Terminal operation did not complete".into(),
        stage: stage.into(),
        may_have_effect: effect,
    }
}
fn result(b: &Binding, outcome: &str) -> ExecutionResult {
    ExecutionResult {
        tower_session_id: Some(b.tower_session_id.clone()),
        owner_id: Some(b.owner_id.clone()),
        outcome: Some(outcome.into()),
        ..Default::default()
    }
}
pub(crate) fn session(journal: &Journal, b: &Binding) -> Result<ManagedSession> {
    let stored = journal.binding(&b.namespace(), &b.runtime_id, &b.generation)?;
    if serde_json::to_value(&stored).map_err(internal)?
        != serde_json::to_value(b).map_err(internal)?
        || b.protocol != "terminal"
    {
        return Err(error("runtime_conflict", "Terminal owner identity changed"));
    }
    serde_json::from_value(journal.metadata(b)?["terminal_session"].clone())
        .map_err(|_| error("not_found", "Terminal preparation is not available"))
}
async fn observe(backend: Arc<dyn ManagedBackend>, session: ManagedSession) -> Result<Observation> {
    tokio::task::spawn_blocking(move || {
        backend
            .observe(std::slice::from_ref(&session))?
            .into_iter()
            .find(|o| o.id == session.id)
            .ok_or_else(|| error("backend_unavailable", "Terminal observation is missing"))
    })
    .await
    .map_err(internal)?
}
async fn stop(backend: Arc<dyn ManagedBackend>, session: ManagedSession) -> Result<Observation> {
    tokio::task::spawn_blocking(move || backend.stop(&session))
        .await
        .map_err(internal)?
}
pub(crate) fn state_payload(b: &Binding, metadata: &Value, state: &str, terminated: bool) -> Value {
    let mut payload = json!({"state":state,"owner_id":b.owner_id,"tower_session_id":b.tower_session_id,"controls":HarnessControls::terminal()});
    if let Some(native) = metadata["native_session_id"]
        .as_str()
        .filter(|v| crate::identifier(v))
    {
        payload["native_session_id"] = json!(native);
    }
    if terminated {
        payload["termination"] = json!("confirmed");
    }
    payload
}
fn record(
    journal: &Journal,
    b: &Binding,
    mut observation: Observation,
    grace: bool,
) -> Result<bool> {
    observation.caps.resume = false;
    observation.recovery_pending = false;
    observation.binding = None;
    let terminal = observation.terminal.as_ref();
    let running = terminal.is_some_and(|t| t.running);
    let terminated = terminal.is_some_and(|t| t.terminated);
    let mut metadata = journal.metadata(b)?;
    let old = metadata.clone();
    metadata["observation"] = serde_json::to_value(&observation).map_err(internal)?;
    if let Some(native) = &observation.metadata.native_session_id {
        if metadata["native_session_id"]
            .as_str()
            .is_some_and(|old| old != native)
        {
            return Err(error(
                "runtime_identity_conflict",
                "Terminal native identity changed",
            ));
        }
        metadata["native_session_id"] = json!(native);
    }
    let (previous, fenced, _) = journal.state(b)?;
    let state = if terminated {
        "ended"
    } else if running {
        "running"
    } else {
        "starting"
    };
    let payload = state_payload(b, &metadata, state, terminated);
    if !fenced && old["lifecycle"] != payload {
        journal.append(b, "runtime_state", public(&payload)?, None, true)?;
        metadata["lifecycle"] = payload;
    }
    if metadata != old || (!fenced && previous != state) {
        if fenced {
            journal.terminal_observation(b, &metadata["observation"])?;
        } else {
            journal.update_metadata(b, state, &metadata)?;
        }
    }
    if terminated {
        journal.fence(b, "terminal_exited", "confirmed")?;
        return Ok(true);
    }
    if !running && !grace && !fenced {
        journal.fence(b, "terminal_unavailable", "unknown")?;
    }
    Ok(false)
}
fn recover_commands(journal: &Journal, b: &Binding, running: bool) -> Result<()> {
    for (kind, receipt) in journal.unsettled(b)? {
        let effect = receipt.dispatch.state != "not_started";
        let confirmed_launch = kind == "launch" && effect && running;
        journal.finish_dispatch(
            b,
            &receipt.command_id,
            confirmed_launch,
            confirmed_launch.then(|| result(b, "completed")),
            (!confirmed_launch).then(|| {
                failure(
                    if effect {
                        "dispatch_unknown"
                    } else {
                        "owner_restarted"
                    },
                    "recovery",
                    effect,
                )
            }),
        )?;
    }
    Ok(())
}

async fn cancel_pending(
    authority: &crate::runtime::Authority,
    journal: &Journal,
    b: &Binding,
    launch_id: &str,
    control: &mut mpsc::Receiver<engine::Control>,
) -> Result<bool> {
    while let Ok(stop) = control.try_recv() {
        let reason = match stop {
            engine::Control::Suspend => continue,
            engine::Control::Command(c) => {
                if let Err(e) = authority.check(b, &c).await {
                    journal.finish_dispatch(
                        b,
                        &c.command_id,
                        false,
                        None,
                        Some(failure(&e.data.code, "dispatch", false)),
                    )?;
                    continue;
                }
                match journal.begin_dispatch(b, &c.command_id) {
                    Ok(true) => {
                        journal.finish_dispatch(
                            b,
                            &c.command_id,
                            false,
                            Some(result(b, "stopped")),
                            None,
                        )?;
                    }
                    Ok(false) => continue,
                    Err(e) => {
                        journal.finish_dispatch(
                            b,
                            &c.command_id,
                            false,
                            None,
                            Some(failure(&e.data.code, "dispatch", false)),
                        )?;
                        continue;
                    }
                }
                "stop_runtime".into()
            }
            engine::Control::Stop(reason) => reason,
        };
        journal.finish_dispatch(
            b,
            launch_id,
            false,
            None,
            Some(failure("runtime_cancelled", "dispatch", false)),
        )?;
        journal.fence(b, &reason, "confirmed")?;
        return Ok(true);
    }
    Ok(false)
}

/// Relaunches a lost terminal session on its native conversation. Returns
/// `None` when the conversation cannot be resumed.
async fn resume(
    paths: &tower_pal::Paths,
    journal: &Journal,
    b: &Binding,
    backend: Arc<dyn ManagedBackend>,
    lost: &ManagedSession,
) -> Result<Option<ManagedSession>> {
    let recover_backend = backend.clone();
    let lost_session = lost.clone();
    let Some(prepared) =
        tokio::task::spawn_blocking(move || recover_backend.recover(&lost_session))
            .await
            .map_err(internal)??
    else {
        return Ok(None);
    };
    let next = ManagedSession {
        id: lost.id.clone(),
        protocol: "terminal".into(),
        binding: prepared.binding,
        launch: Some(prepared.launch),
        auto_resume: true,
    };
    let mut metadata = journal.metadata(b)?;
    metadata["terminal_session"] = serde_json::to_value(&next).map_err(internal)?;
    let (state, _, _) = journal.state(b)?;
    journal.update_metadata(b, &state, &metadata)?;
    let token = paths.token_path(&b.tower_session_id).map_err(internal)?;
    let token = Secret(
        String::from_utf8(tower_pal::read_private(&token).map_err(internal)?).map_err(internal)?,
    );
    let start_session = next.clone();
    tokio::task::spawn_blocking(move || backend.start(&start_session, &token))
        .await
        .map_err(internal)??;
    Ok(Some(next))
}
pub(crate) async fn run(
    ctx: engine::Context,
    b: Binding,
    backend: Arc<dyn ManagedBackend>,
    mut normal: mpsc::Receiver<ExecutionCommand>,
    mut control: mpsc::Receiver<engine::Control>,
    recover: bool,
) {
    let journal = &ctx.journal;
    if let Err(e) = run_inner(
        &ctx,
        &b,
        backend.clone(),
        &mut normal,
        &mut control,
        recover,
    )
    .await
    {
        let _ = journal.fence(&b, &e.data.code, "unknown");
        if let Ok(session) = session(journal, &b)
            && let Ok(observation) = stop(backend, session).await
        {
            let _ = record(journal, &b, observation, false);
        }
    }
}
async fn run_inner(
    ctx: &engine::Context,
    b: &Binding,
    backend: Arc<dyn ManagedBackend>,
    normal: &mut mpsc::Receiver<ExecutionCommand>,
    control: &mut mpsc::Receiver<engine::Control>,
    recover: bool,
) -> Result<()> {
    let (paths, journal) = (&ctx.paths, &*ctx.journal);
    let native = if recover {
        let native = match session(journal, b) {
            Ok(native) => native,
            Err(_) => {
                let uncertain = journal
                    .unsettled(b)?
                    .iter()
                    .any(|(_, r)| r.dispatch.state != "not_started");
                journal.fence(
                    b,
                    "terminal_preparation_lost",
                    if uncertain { "unknown" } else { "confirmed" },
                )?;
                return Ok(());
            }
        };
        if journal
            .unsettled(b)?
            .iter()
            .any(|(kind, r)| kind == "launch" && r.dispatch.state == "not_started")
        {
            recover_commands(journal, b, false)?;
            journal.fence(b, "owner_restarted_before_launch", "confirmed")?;
            return Ok(());
        }
        let observation = observe(backend.clone(), native.clone()).await?;
        recover_commands(
            journal,
            b,
            observation.terminal.as_ref().is_some_and(|t| t.running),
        )?;
        if record(journal, b, observation, false)? {
            return Ok(());
        }
        native
    } else {
        let Some(command) = normal.recv().await else {
            return Ok(());
        };
        if cancel_pending(&ctx.authority, journal, b, &command.command_id, control).await? {
            return Ok(());
        }
        if let Err(e) = ctx.authority.check(b, &command).await {
            journal.finish_dispatch(
                b,
                &command.command_id,
                false,
                None,
                Some(failure(&e.data.code, "dispatch", false)),
            )?;
            journal.fence(b, &e.data.code, "confirmed")?;
            return Ok(());
        }
        let prepare_backend = backend.clone();
        let id = b.tower_session_id.clone();
        let request = SessionOpenParams {
            terminal: None,
            protocol: Some("terminal".into()),
            harness: b.harness.clone(),
            cwd: b.cwd.clone(),
            args: vec![],
            headless: true,
            mode: None,
            checkout_id: None,
            resume: None,
        };
        let prepared = tokio::task::spawn_blocking(move || prepare_backend.prepare(&id, &request))
            .await
            .map_err(internal)?;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                journal.finish_dispatch(
                    b,
                    &command.command_id,
                    false,
                    None,
                    Some(failure(&e.data.code, "dispatch", false)),
                )?;
                journal.fence(b, &e.data.code, "confirmed")?;
                return Ok(());
            }
        };
        let native = ManagedSession {
            id: b.tower_session_id.clone(),
            protocol: "terminal".into(),
            binding: prepared.binding,
            launch: Some(prepared.launch),
            // A lost pool session resumes in the same runtime.
            auto_resume: true,
        };
        let mut metadata = json!({"terminal_session":native,"launch_command_id":command.command_id,"boot_id":tower_pal::boot_id().map_err(internal)?});
        let payload = state_payload(b, &metadata, "starting", false);
        journal.append(b, "runtime_state", public(&payload)?, None, true)?;
        metadata["lifecycle"] = payload;
        journal.update_metadata(b, "starting", &metadata)?;
        if let Err(e) = ctx.authority.check(b, &command).await {
            journal.finish_dispatch(
                b,
                &command.command_id,
                false,
                None,
                Some(failure(&e.data.code, "dispatch", false)),
            )?;
            journal.fence(b, &e.data.code, "confirmed")?;
            return Ok(());
        }
        if cancel_pending(&ctx.authority, journal, b, &command.command_id, control).await? {
            return Ok(());
        }
        if !journal.begin_dispatch(b, &command.command_id)? {
            return Ok(());
        }
        let token = paths.token_path(&b.tower_session_id).map_err(internal)?;
        let token = Secret(
            String::from_utf8(tower_pal::read_private(&token).map_err(internal)?)
                .map_err(internal)?,
        );
        let start_backend = backend.clone();
        let start_session = native.clone();
        match tokio::task::spawn_blocking(move || start_backend.start(&start_session, &token))
            .await
            .map_err(internal)?
        {
            Ok(observation) => {
                journal.finish_dispatch(
                    b,
                    &command.command_id,
                    true,
                    Some(result(b, "completed")),
                    None,
                )?;
                if record(journal, b, observation, true)? {
                    return Ok(());
                }
            }
            Err(_) => {
                journal.finish_dispatch(
                    b,
                    &command.command_id,
                    false,
                    None,
                    Some(failure("dispatch_unknown", "dispatch", true)),
                )?;
            }
        }
        native
    };
    let mut native = native;
    let started = tokio::time::Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! { biased;
            command = control.recv() => {
                let Some(command) = command else { return Ok(()) };
                let (command, reason) = match command {
                    engine::Control::Command(command) => (Some(command), "stop_runtime".to_owned()),
                    engine::Control::Stop(reason) => (None, reason),
                    // Terminal sessions live in the pool across server restarts.
                    engine::Control::Suspend => continue,
                };
                if let Some(c) = &command {
                    if let Err(e) = ctx.authority.check(b, c).await {
                        journal.finish_dispatch(b, &c.command_id, false, None, Some(failure(&e.data.code, "dispatch", false)))?;
                        continue;
                    }
                    if !journal.begin_dispatch(b, &c.command_id)? { continue }
                }
                match stop(backend.clone(), native.clone()).await {
                    Ok(observation) if observation.terminal.as_ref().is_some_and(|t| t.terminated) => {
                        if let Some(c) = &command {
                            journal.finish_dispatch(b, &c.command_id, true, Some(result(b, "stopped")), None)?;
                        }
                        record(journal, b, observation, false)?;
                        journal.fence(b, &reason, "confirmed")?;
                        return Ok(());
                    }
                    _ => {
                        if let Some(c) = &command {
                            journal.finish_dispatch(b, &c.command_id, false, None, Some(failure("dispatch_unknown", "execution", true)))?;
                        }
                        journal.fence(b, "stop_unconfirmed", "unknown")?;
                    }
                }
            }
            _ = tick.tick() => {
                match observe(backend.clone(), native.clone()).await {
                    // The pool lost the session, for example when it was
                    // replaced: resume the conversation in the same runtime.
                    Ok(observation) if observation.status == "lost" && !journal.state(b)?.1 => {
                        match resume(paths, journal, b, backend.clone(), &native).await {
                            Ok(Some(next)) => native = next,
                            _ => {
                                if record(journal, b, observation, false)? { return Ok(()) }
                            }
                        }
                    }
                    Ok(observation) => {
                        if !journal.state(b)?.1 && observation.terminal.as_ref().is_some_and(|t| t.running) {
                            // Positive identity evidence can settle a lost create reply without resubmission.
                            let metadata = journal.metadata(b)?;
                            if let Some(id) = metadata["launch_command_id"].as_str() {
                                journal.finish_dispatch(b, id, true, Some(result(b, "completed")), None)?;
                            }
                        }
                        if record(journal, b, observation, !recover && started.elapsed() < Duration::from_secs(5))? { return Ok(()) }
                    }
                    Err(_) => { journal.fence(b, "terminal_unavailable", "unknown")?; }
                }
            }
            command = normal.recv() => {
                if command.is_none() { return Ok(()) }
                return Err(error("runtime_conflict", "A terminal runtime already has its launch"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    fn binding() -> Binding {
        Binding {
            origin: "https://allgather.example".into(),
            machine_id: "machine".into(),
            session_id: "session".into(),
            runtime_id: "runtime".into(),
            generation: "generation".into(),
            tower_session_id: "tower-session".into(),
            owner_id: "owner".into(),
            protocol: "terminal".into(),
            harness: "pi".into(),
            cwd: "/work/project".into(),
            resume_session_id: None,
        }
    }
    #[test]
    fn recovery_uses_positive_launch_evidence_and_preserves_uncertain_stop() {
        let root = tempfile::tempdir().unwrap();
        let journal = Journal::open(&root.path().join("tower.db"), Limits::default()).unwrap();
        let b = binding();
        let launch = crate::command(
            &b,
            "launch",
            json!({"protocol":"terminal","harness":"pi","cwd":b.cwd}),
        )
        .unwrap();
        let cancel = crate::command(&b, "cancel", json!({})).unwrap();
        journal.admit(&b, &launch).unwrap();
        journal.begin_dispatch(&b, &launch.command_id).unwrap();
        journal.admit(&b, &cancel).unwrap();
        journal.begin_dispatch(&b, &cancel.command_id).unwrap();
        recover_commands(&journal, &b, true).unwrap();
        let status = |c: &ExecutionCommand| {
            journal
                .status(
                    &b.namespace(),
                    &tower_protocol::ExecutionStatus {
                        command_id: c.command_id.clone(),
                        runtime_id: b.runtime_id.clone(),
                        generation: b.generation.clone(),
                    },
                )
                .unwrap()
        };
        assert_eq!(status(&launch).dispatch.state, "finished");
        assert_eq!(
            status(&launch).result.unwrap().outcome.as_deref(),
            Some("completed")
        );
        assert_eq!(status(&cancel).dispatch.state, "unknown");
        assert!(status(&cancel).error.unwrap().may_have_effect);
        assert_eq!(journal.admit(&b, &cancel).unwrap(), status(&cancel));
        assert_eq!(journal.cursor(&b).unwrap().coverage, "partial");
        assert!(!journal.state(&b).unwrap().1);
    }
    #[test]
    fn terminal_commands_do_not_accept_structured_or_unbounded_launch_input() {
        let b = binding();
        for kind in ["prompt", "reply", "configure", "resume"] {
            let c = crate::command(&b, kind, json!({})).unwrap();
            assert_eq!(
                validate(&b, &c).unwrap_err().data.code,
                "unsupported_control"
            );
        }
        for payload in [
            json!({"protocol":"terminal","harness":"pi","cwd":b.cwd,"args":["secret"]}),
            json!({"protocol":"terminal","harness":"other","cwd":b.cwd}),
        ] {
            assert_eq!(
                validate(&b, &crate::command(&b, "launch", payload).unwrap())
                    .unwrap_err()
                    .data
                    .code,
                "invalid_params"
            );
        }
        assert!(validate(&b, &crate::command(&b, "cancel", json!({})).unwrap()).is_ok());
    }
    #[tokio::test]
    async fn stop_before_launch_never_crosses_the_dispatch_fence() {
        let root = tempfile::tempdir().unwrap();
        let journal =
            Journal::open(&root.path().join("tower.db"), crate::Limits::default()).unwrap();
        let b = binding();
        let c = crate::command(
            &b,
            "launch",
            json!({"protocol":"terminal","harness":"pi","cwd":b.cwd}),
        )
        .unwrap();
        journal.admit(&b, &c).unwrap();
        let (handle, _normal, mut control) = engine::channels();
        handle
            .control
            .try_send(engine::Control::Stop("local_stop".into()))
            .unwrap();
        let authority = crate::runtime::Authority::default();
        assert!(
            cancel_pending(&authority, &journal, &b, &c.command_id, &mut control)
                .await
                .unwrap()
        );
        let receipt = journal
            .status(
                &b.namespace(),
                &tower_protocol::ExecutionStatus {
                    command_id: c.command_id,
                    runtime_id: b.runtime_id.clone(),
                    generation: b.generation.clone(),
                },
            )
            .unwrap();
        assert_eq!(receipt.dispatch.state, "finished");
        assert_eq!(receipt.dispatch.transport_written, Some(false));
        assert!(!receipt.error.unwrap().may_have_effect);
        assert_eq!(
            journal.state(&b).unwrap(),
            ("ended".into(), true, "confirmed".into())
        );
        assert!(
            journal
                .metadata(&b)
                .unwrap()
                .get("terminal_session")
                .is_none()
        );
    }
}
