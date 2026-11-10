use crate::{Harness, definition::*, runtime::AgentSpec};
use std::{io, path::Path};
use tower_pal::Environment;
use tower_pal::write_private;
mod prompts;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Omp
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "omp",
            program: "omp",
            label: "oh-my-pi",
            aliases: &["omp", "oh-my-pi"],
        }
    }
    fn execution(&self) -> Execution {
        Execution::Hooks
    }
    fn status_behavior(&self) -> StatusBehavior {
        StatusBehavior {
            session_start_is_root: true,
            reset_on_session_change: true,
        }
    }
    fn normalize_hook(
        &self,
        _name: &str,
        value: &serde_json::Value,
        event: &mut crate::events::AgentEvent,
    ) {
        event.blocking_prompt = value
            .get("blocking_prompt")
            .and_then(serde_json::Value::as_str)
            .filter(|s| matches!(*s, "approval" | "decision" | "none"))
            .map(str::to_owned);
        if let Some(jobs) = value
            .get("async_jobs")
            .and_then(serde_json::Value::as_array)
        {
            event.background_jobs = Some(jobs.len().min(u32::MAX as usize) as u32);
        }
    }
    fn hook_session(&self, _env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(prompts::Hooks::default())
    }
    fn supports_fork(&self) -> bool {
        true
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            Some(vec!["--fork".into(), id.into()])
        } else {
            Some(vec!["--resume".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &[]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &[
                "--provider",
                "--thinking",
                "--thinking-level",
                "--session-dir",
            ],
            switches_to_remove: &["-c", "-p"],
            optional_values_to_remove: &["--fork"],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &["--resume", "--continue"]
    }
    fn configure_launch(
        &self,
        _spec: &AgentSpec,
        dir: &Path,
        exe: &Path,
        _env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let path = dir.join("tower-extension.ts");
        write_private(&path, extension(exe).as_bytes())?;
        argv.extend(["-e".into(), path.to_string_lossy().into_owned()]);
        Ok(())
    }
}

pub(crate) const EVENTS: &[(&str, &str)] = &[
    ("session_start", "SessionStart"),
    ("before_agent_start", "UserPromptSubmit"),
    ("tool_execution_start", "PreToolUse"),
    ("tool_execution_end", "PostToolUse"),
    ("session_before_compact", "PreCompact"),
    ("session_compact", "PostCompact"),
    ("session_switch", "SessionStart"),
    ("tool_approval_requested", "PermissionRequest"),
    ("tool_approval_resolved", "PermissionResult"),
    ("plan_review_requested", "status-update"),
    ("plan_review_resolved", "status-update"),
    ("agent_end", "Stop"),
];

pub(crate) fn extension(exe: &Path) -> String {
    crate::extensions::observed_extension(
        exe,
        Harness::Omp,
        EVENTS,
        Some(include_str!("omp/prompts.mjs")),
    )
}
