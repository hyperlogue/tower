use crate::config::{COMMON, home, hook, invalid, json_file, nested};
use crate::{Harness, definition::*, runtime::AgentSpec};
use serde_json::Value;
use serde_json::json;
use std::{io, path::Path};
use tower_pal::Environment;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Grok
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "grok",
            program: "grok",
            label: "Grok Build",
            aliases: &["grok", "grok-build"],
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
        let tasks = value.get("backgroundTasks").and_then(Value::as_array);
        let crons = value.get("sessionCrons").and_then(Value::as_array);
        if tasks.is_none() && crons.is_none() {
            return;
        }
        let text = |value: &Value, keys: &[&str]| {
            keys.iter()
                .find_map(|key| value.get(key).and_then(Value::as_str))
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned)
        };
        let tasks = tasks.into_iter().flatten().filter(|t| {
            !matches!(
                t.get("status").and_then(Value::as_str),
                Some("completed" | "failed" | "cancelled")
            )
        });
        let named: Vec<_> = tasks
            .map(|t| {
                text(t, &["command", "description", "agentType", "type"])
                    .unwrap_or_else(|| "task".into())
            })
            .chain(
                crons
                    .into_iter()
                    .flatten()
                    .map(|c| text(c, &["prompt", "schedule"]).unwrap_or_else(|| "loop".into())),
            )
            .collect();
        event.background_jobs = Some(named.len().min(u32::MAX as usize) as u32);
        event.background_commands = Some(named);
    }
    fn hook_session(&self, _env: &Environment) -> Box<dyn crate::hook::HookSession> {
        Box::new(crate::hook::Stateless)
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
        &[]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &[],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &["--resume", "--continue"]
    }
    fn configure_launch(
        &self,
        _spec: &AgentSpec,
        _dir: &Path,
        exe: &Path,
        env: &mut Environment,
        _argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let h = self.kind();

        let real = home(env, "GROK_HOME", ".grok")?;
        let hooks_dir = real.join("hooks");
        tower_pal::ensure_user_dir(&hooks_dir)?;
        let mut names = COMMON.to_vec();
        names.extend(["StopCancelled", "PostCompact"]);
        let mut hooks = nested(exe, h, &names);
        hooks["Notification"] = json!([
            {"matcher":"permission_prompt","hooks":[{"type":"command","command":hook(exe,h,"PermissionRequest"),"timeout":5}]},
            {"matcher":"idle_prompt","hooks":[{"type":"command","command":hook(exe,h,"Stop"),"timeout":5}]}
        ]);
        let path = hooks_dir.join("tower.json");
        if path.exists() && json_file(&path)?.get("_tower_generated") != Some(&Value::Bool(true)) {
            return Err(invalid("Grok tower.json exists and is not owned by Tower"));
        }
        tower_pal::write_owned_config(
            &path,
            &serde_json::to_vec_pretty(&json!({"_tower_generated":true,"hooks":hooks}))?,
        )?;
        env.insert("GROK_HOME".into(), real.to_string_lossy().into_owned());
        Ok(())
    }
}
