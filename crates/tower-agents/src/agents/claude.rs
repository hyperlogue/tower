use crate::config::{COMMON, nested, save_json};
use crate::{Harness, definition::*, runtime::AgentSpec};
use serde_json::json;
use std::{io, path::Path};
use tower_pal::Environment;
pub(crate) mod activity;
mod observation;
mod transcript;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::ClaudeCode
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "claude-code",
            program: "claude",
            label: "Claude Code",
            aliases: &["claude", "claude-code", "claudecode"],
        }
    }
    fn execution(&self, _spec: &AgentSpec) -> Execution {
        Execution::Hooks
    }
    fn status_behavior(&self) -> StatusBehavior {
        StatusBehavior {
            session_start_is_root: true,
            reset_on_session_change: false,
        }
    }
    fn normalize_hook(
        &self,
        _name: &str,
        value: &serde_json::Value,
        event: &mut crate::events::AgentEvent,
    ) {
        event.transcript_path = crate::events::text(value, &["/transcript_path"], 4096);
        if event.kind != crate::events::EventKind::Ready {
            event.cwd = None;
        }
        if value.get("agent_id").is_some() {
            event.effort = None;
        }
    }
    fn hook_session(&self, env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(observation::Session::new(env))
    }
    fn supports_fork(&self) -> bool {
        true
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            Some(vec!["--resume".into(), id.into(), "--fork-session".into()])
        } else {
            Some(vec!["--resume".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &["--tmux"]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &[
                "--permission-mode",
                "--system-prompt",
                "--append-system-prompt",
                "--settings",
                "--setting-sources",
                "--mcp-config",
                "--permission-prompt-tool",
                "--output-format",
                "--input-format",
                "--max-budget-usd",
                "--max-turns",
            ],
            switches_to_remove: &["-c", "-p"],
            multiple_values: &[
                "--allowedTools",
                "--allowed-tools",
                "--disallowedTools",
                "--disallowed-tools",
                "--tools",
                "--add-dir",
            ],
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
        env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let h = self.kind();

        let mut names = COMMON.to_vec();
        names.extend([
            "Elicitation",
            "ElicitationResult",
            "PostCompact",
            "PostModelSwitch",
        ]);
        let path = dir.join("claude-settings.json");
        save_json(&path, &json!({"hooks":nested(exe,h,&names)}))?;
        argv.extend(["--settings".into(), path.to_string_lossy().into_owned()]);
        if env.contains_key("TOWER_SOCKET") {
            let mcp = dir.join("tower-mcp.json");
            save_json(
                &mcp,
                &json!({"mcpServers":{"tower":{"command":exe,"args":["mcp"]}}}),
            )?;
            argv.extend(["--mcp-config".into(), mcp.to_string_lossy().into_owned()]);
        }
        env.insert(
            "CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR".into(),
            "1".into(),
        );
        Ok(())
    }
}
