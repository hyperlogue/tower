use crate::{Result, context};
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tower_agents::{
    Harness,
    events::{self, EventKind},
};
use tower_protocol::DeliveryEndpoint;

pub fn report(harness: &str, event: &str) -> Result<()> {
    let h = Harness::parse(harness).ok_or("unknown harness")?;
    let bytes = tower_pal::read_stdin_bounded(1024 * 1024, Duration::from_millis(300))?;
    let parsed = events::parse(h, event, if bytes.is_empty() { b"{}" } else { &bytes })
        .map_err(str::to_owned)?;
    if parsed.child {
        return Ok(());
    }
    let native = parsed
        .native_id
        .or_else(|| {
            std::env::var(if h == Harness::Codex {
                "CODEX_THREAD_ID"
            } else {
                "CLAUDE_CODE_SESSION_ID"
            })
            .ok()
        })
        .ok_or("native identity unavailable")?;
    let paths = tower_pal::Paths::discover()?;
    let mut caller = context()?;
    caller.harness = Some(h.id().into());
    caller.native_session_id = Some(native.clone());
    let mut pid = tower_pal::process_identity(tower_pal::pid())
        .ok()
        .map(|p| p.parent);
    let mut found = None;
    for _ in 0..32 {
        let Some(current) = pid.filter(|p| *p > 1) else {
            break;
        };
        if tower_pal::process_name(current)
            .is_ok_and(|name| name == h.program() || (h == Harness::Codex && name == "codex-tui"))
        {
            found = Some(current);
            break;
        }
        pid = tower_pal::process_identity(current).ok().map(|p| p.parent);
    }
    let cwd = parsed
        .cwd
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let status = parsed.authoritative_status.unwrap_or_else(|| {
        match parsed.kind {
            EventKind::Ready | EventKind::Idle
                if parsed.continuing || parsed.background_jobs.is_some_and(|n| n > 0) =>
            {
                "working"
            }
            EventKind::Ready | EventKind::Idle | EventKind::Compacted => "idle",
            EventKind::ToolStart if parsed.tool.as_deref() == Some("AskUserQuestion") => {
                "waiting_input"
            }
            EventKind::Working
            | EventKind::Compacting
            | EventKind::ToolStart
            | EventKind::ToolEnd
            | EventKind::ApprovalResolved => "working",
            EventKind::Approval => "waiting_approval",
            EventKind::End => "exited",
            EventKind::Failure => "idle",
            EventKind::Metadata => "unknown",
        }
        .into()
    });
    let endpoint = DeliveryEndpoint {
        app_server: None,
        messaging_socket: std::env::var("CLAUDE_CODE_MESSAGING_SOCKET")
            .ok()
            .filter(|_| h == Harness::ClaudeCode),
        messaging_token: std::env::var("CLAUDE_CODE_MESSAGING_TOKEN")
            .ok()
            .filter(|_| h == Harness::ClaudeCode)
            .map(tower_protocol::Secret),
        thread_id: (h == Harness::Codex).then_some(native.clone()),
        codex_home: std::env::var("CODEX_HOME")
            .ok()
            .filter(|_| h == Harness::Codex),
    };
    let phase = match parsed.kind {
        EventKind::Compacting => Some("compacting"),
        EventKind::Compacted => Some("compacted"),
        _ => None,
    };
    let mut report = json!({"harness":h.id(),"native_session_id":native,"pid":found,"cwd":cwd,"display_name":parsed.title,"kind":parsed.kind,"status":status,"detail":{"last_tool":parsed.tool,"model":parsed.model,"effort":parsed.effort,"phase":phase,"context_tokens":parsed.context_tokens},"delivery":endpoint});
    let detail = report["detail"].as_object_mut().unwrap();
    if let Some(prompt) = &parsed.prompt {
        detail.insert("last_prompt".into(), json!(prompt));
    }
    if let Some(commands) = &parsed.background_commands {
        detail.insert("background_commands".into(), json!(commands));
    } else if parsed.kind == EventKind::Working {
        detail.insert("background_commands".into(), json!([]));
    }
    if parsed.effort.is_none() && !parsed.clear_effort {
        report["detail"].as_object_mut().unwrap().remove("effort");
    }
    if phase.is_none()
        && !matches!(
            parsed.kind,
            EventKind::Working | EventKind::Idle | EventKind::Failure | EventKind::ApprovalResolved
        )
    {
        report["detail"].as_object_mut().unwrap().remove("phase");
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                // Asking who the caller is doubles as the handshake: a hook
                // runs on every tool call and reports in a fixed format.
                let (mut client, identity) = tower_server::client::Client::connect_calling(
                    &paths,
                    caller,
                    true,
                    "identity.whoami",
                    &json!({}),
                )
                .await?;
                let identity = serde_json::to_value(identity)?;
                if let Some(id) = identity["id"].as_str()
                    && id != "human"
                    && let Ok(bytes) = tower_pal::read_private(&paths.token_path(id)?)
                    && let Ok(token) = String::from_utf8(bytes)
                {
                    report["token"] = token.into();
                }
                client.call("agent.hook", &report).await
            })
            .await??;
            Ok(())
        })
}
