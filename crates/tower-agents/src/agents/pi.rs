use crate::{Harness, definition::*, runtime::AgentSpec};
use std::{io, path::Path};
use tower_pal::Environment;
use tower_pal::write_private;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Pi
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "pi",
            program: "pi",
            label: "Pi",
            aliases: &["pi"],
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
        _value: &serde_json::Value,
        _event: &mut crate::events::AgentEvent,
    ) {
    }
    fn hook_session(&self, _env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(crate::hook::Stateless)
    }
    fn supports_fork(&self) -> bool {
        true
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            Some(vec!["--fork".into(), id.into()])
        } else {
            Some(vec!["--session".into(), id.into()])
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
        &["--session", "--continue"]
    }
    fn configure_launch(
        &self,
        _spec: &AgentSpec,
        dir: &Path,
        exe: &Path,
        _env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let h = self.kind();

        let path = dir.join("tower-extension.ts");
        write_private(
            &path,
            crate::extensions::extension(exe, h, EVENTS).as_bytes(),
        )?;
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
    ("session_info_changed", "metadata"),
    ("model_select", "metadata"),
    ("session_tree", "metadata"),
    ("agent_settled", "Stop"),
    ("session_compact_failed", "StopFailure"),
];
