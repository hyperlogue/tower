mod observation;
use crate::config::shell_quote;
use crate::config::{home, hook, json_file, mirror, save_json};
use crate::events::{EventKind, count};
use crate::{Harness, definition::*, runtime::AgentSpec};
use serde_json::Value;
use serde_json::json;
use std::{io, path::Path};
use tower_pal::Environment;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Antigravity
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "antigravity",
            program: "agy",
            label: "Antigravity",
            aliases: &["antigravity", "agy"],
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
        name: &str,
        value: &serde_json::Value,
        event: &mut crate::events::AgentEvent,
    ) {
        if name == "status-update" {
            event.coarse_status = true;
            event.authoritative_status = if value
                .get("tool_confirmation_pending")
                .and_then(Value::as_bool)
                == Some(true)
            {
                Some("waiting_approval".into())
            } else {
                match value.get("agent_state").and_then(Value::as_str) {
                    Some("idle") => Some("idle".into()),
                    Some("thinking" | "working" | "running" | "streaming" | "tool_use") => {
                        Some("working".into())
                    }
                    _ => None,
                }
            };
            if count(value, &["/context_window/context_window_size"]).is_some_and(|n| n > 0) {
                event.context_tokens =
                    count(value, &["/context_window/total_input_tokens"]).map(|n| {
                        n.saturating_add(
                            count(value, &["/context_window/total_output_tokens"]).unwrap_or(0),
                        )
                    });
            }
        }
        if matches!(event.kind, EventKind::Idle | EventKind::Failure) {
            event.background_active = value.get("fullyIdle").and_then(Value::as_bool).map(|v| !v);
        }
        if name == "Stop"
            && value
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        {
            event.kind = EventKind::Failure;
        }
    }
    fn hook_session(&self, env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(observation::Session::new(env))
    }
    fn supports_fork(&self) -> bool {
        false
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            None
        } else {
            Some(vec!["--conversation".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &["--gemini_dir"]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &[],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &["--conversation"]
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

        let real = home(env, "GEMINI_DIR", ".gemini")?;
        let overlay = dir.join("gemini");
        mirror(&real, &overlay, &["config", "antigravity-cli"], &[], &[])?;
        mirror(
            &real.join("config"),
            &overlay.join("config"),
            &["hooks.json"],
            &[],
            &[],
        )?;
        let cli = overlay.join("antigravity-cli");
        mirror(
            &real.join("antigravity-cli"),
            &cli,
            &["settings.json"],
            &[
                "annotations",
                "bin",
                "brain",
                "builtin",
                "cache",
                "conversations",
                "crashes",
                "implicit",
                "log",
                "presence",
                "updater",
            ],
            &[
                "conversation_summaries.db",
                "conversation_summaries.db-wal",
                "conversation_summaries.db-shm",
                "history.jsonl",
                "installation_id",
                "jetski_state.pbtxt",
                "jetbox_summaries_proto.pb",
                "last_check.timestamp",
                "cli.log",
            ],
        )?;
        let mut hooks = json_file(&real.join("config/hooks.json"))?;
        let mut events = serde_json::Map::new();
        for name in ["SessionStart", "PreInvocation", "PostToolUse", "Stop"] {
            let handler = json!({"type":"command","command":format!("{} >/dev/null; printf '{{}}'",hook(exe,h,name)),"timeout":5});
            events.insert(
                name.into(),
                if name == "PostToolUse" {
                    json!([{"matcher":"*","hooks":[handler]}])
                } else {
                    json!([handler])
                },
            );
        }
        hooks["tower"] = Value::Object(events);
        save_json(&overlay.join("config/hooks.json"), &hooks)?;
        let mut settings = json_file(&real.join("antigravity-cli/settings.json"))?;
        let original = settings
            .get("statusLine")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let tail = original
            .get("command")
            .and_then(Value::as_str)
            .filter(|_| original["enabled"] != false)
            .map(|c| {
                format!(
                    "printf '%s' \"$tower_status\" | /bin/sh -c {}",
                    shell_quote(c)
                )
            })
            .unwrap_or_else(|| ":".into());
        let mut line = original.as_object().cloned().unwrap_or_default();
        line.insert("enabled".into(), true.into());
        line.insert("type".into(), "command".into());
        if original.get("command").is_none() {
            line.insert("stack_with_default".into(), true.into());
        }
        line.insert(
            "command".into(),
            format!(
                "tower_status=$(cat); printf '%s' \"$tower_status\" | {} >/dev/null; {tail}",
                hook(exe, h, "status-update")
            )
            .into(),
        );
        settings["statusLine"] = Value::Object(line);
        save_json(&cli.join("settings.json"), &settings)?;
        argv.extend([
            "--gemini_dir".into(),
            overlay.to_string_lossy().into_owned(),
        ]);
        Ok(())
    }
}
