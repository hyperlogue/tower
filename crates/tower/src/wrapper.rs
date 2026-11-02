use crate::{Client, Result};
use std::sync::atomic::Ordering;
use tower_agents::{Harness, runtime::AgentSpec};
use tower_protocol::{AgentCredential, RegisterResult};

pub async fn run(
    paths: tower_pal::Paths,
    client: &mut Client,
    name: String,
    mode: Option<String>,
    args: Vec<String>,
) -> Result<()> {
    client
        .call("daemon.service_access", &serde_json::json!({}))
        .await?;
    let harness = Harness::parse(&name).ok_or("Unknown harness")?;
    if mode.as_deref().is_some_and(|m| m != "native") {
        return Err("The foreground wrapper uses native mode; use tower open for managed app-server sessions".into());
    }
    let cwd = std::env::current_dir()?;
    let exe = tower_pal::executable()?;
    let mut env =
        tower_terminal::launcher::project_environment(&cwd, &exe, tower_pal::environment()).await?;
    for key in [
        "TOWER_AGENT_ID",
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_LAUNCHER_SOCKET",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_MESSAGING_SOCKET",
        "CLAUDE_CODE_MESSAGING_TOKEN",
        "CLAUDE_CODE_CHILD_SESSION",
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
    ] {
        env.remove(key);
    }
    env.insert("TOWER_WRAPPER".into(), "1".into());
    env.insert(
        "TOWER_SOCKET".into(),
        paths.socket.to_string_lossy().into_owned(),
    );
    let root = tower_pal::temporary_dir_in(&paths.state.join("wrappers"))?;
    let spec = AgentSpec {
        harness,
        cwd: cwd.to_string_lossy().into_owned(),
        args,
        mode: (harness == Harness::Codex).then(|| "native".into()),
        resume: None,
        fork: false,
        native_home: None,
    };
    let invocation = tower_agents::config::prepare(&spec, root.path(), &exe, env)?;
    let stop = tower_pal::termination_flag()?;
    let mut child = tower_pal::spawn_inherited(&invocation.argv, &invocation.env, &cwd)?;
    let pid = child.id().ok_or("Agent exited during launch")?;
    let identity = tower_pal::process_identity(pid)?;
    let registered = client
        .call(
            "agent.wrap",
            &tower_protocol::RegisterParams {
                shared_process: false,
                delivery: None,
                protocol: None,
                harness: harness.id().into(),
                native_session_id: None,
                pid: Some(pid),
                cwd: spec.cwd,
                display_name: None,
            },
        )
        .await?;
    let registered: RegisterResult = serde_json::from_value(serde_json::to_value(registered)?)?;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut stopping = None;
    let status = loop {
        tokio::select! {
            result=child.wait()=>break result?,
            _=tick.tick()=>{
                if stop.load(Ordering::Relaxed) && stopping.is_none(){
                    let _=tower_pal::terminate_process(pid,identity.start_time);stopping=Some(std::time::Instant::now());
                }
                if stopping.is_some_and(|t|t.elapsed()>std::time::Duration::from_secs(2)){let _=child.start_kill();}
            }
        }
    };
    let credential = AgentCredential {
        id: registered.id,
        token: registered.token,
    };
    if client.call("agent.unregister", &credential).await.is_err()
        && let Ok(mut next) = Client::connect(&paths, crate::context()?, true).await
    {
        let _ = next.call("agent.unregister", &credential).await;
    }
    if !status.success() {
        return Err(format!("Agent exited with status {}", status.code().unwrap_or(128)).into());
    }
    Ok(())
}
