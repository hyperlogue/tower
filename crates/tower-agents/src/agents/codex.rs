use crate::{Harness, definition::*, runtime::AgentSpec};
use std::{io, path::Path};
use tower_pal::Environment;
pub(crate) mod native;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Codex
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "codex",
            program: "codex",
            label: "Codex",
            aliases: &["codex"],
        }
    }
    fn execution(&self, spec: &AgentSpec) -> Execution {
        if spec.mode.as_deref() == Some("native") {
            Execution::Hooks
        } else {
            Execution::CodexAppServer
        }
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
        if value.get("effort").is_some_and(serde_json::Value::is_null)
            || value
                .get("reasoningEffort")
                .is_some_and(serde_json::Value::is_null)
        {
            event.clear_effort = true;
        }
    }
    fn hook_session(&self, _env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(crate::hook::Stateless)
    }
    fn supports_fork(&self) -> bool {
        true
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            Some(vec!["fork".into(), id.into()])
        } else {
            Some(vec!["resume".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &["--remote", "--profile", "-p"]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &[
                "--config",
                "-c",
                "--sandbox",
                "-s",
                "--ask-for-approval",
                "-a",
                "--add-dir",
                "--enable",
                "--disable",
                "--model-provider",
            ],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &[]
    }
    fn configure_launch(
        &self,
        spec: &AgentSpec,
        _dir: &Path,
        exe: &Path,
        env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()> {
        if spec.mode.as_deref() == Some("native") {
            native::configure(exe, env, argv)?;
        }
        Ok(())
    }
}
