use crate::config::{COMMON, extend_events, home, hook, json_file, mirror, save_json};
use crate::{Harness, definition::*, runtime::AgentSpec};
use serde_json::Value;
use serde_json::json;
use std::{io, path::Path};
use tower_pal::Environment;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Reasonix
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "reasonix",
            program: "reasonix",
            label: "Reasonix",
            aliases: &["reasonix"],
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
            Some(vec!["-r".into(), id.into(), "--copy".into()])
        } else {
            Some(vec!["-r".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &[]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &["--provider", "--config", "--approval-mode"],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &["-r", "--resume"]
    }
    fn configure_launch(
        &self,
        spec: &AgentSpec,
        dir: &Path,
        exe: &Path,
        env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let h = self.kind();

        let real = home(env, "REASONIX_HOME", ".reasonix")?;
        let overlay = dir.join("reasonix");
        mirror(
            &real,
            &overlay,
            &["settings.json"],
            &[],
            &[".env", "config.toml"],
        )?;
        let mut settings = json_file(&real.join("settings.json"))?;
        let events = COMMON
            .iter()
            .map(|e| {
                (
                    (*e).to_owned(),
                    json!([{"match":"*","command":hook(exe,h,e)}]),
                )
            })
            .collect();
        extend_events(&mut settings, Value::Object(events))?;
        save_json(&overlay.join("settings.json"), &settings)?;
        env.entry("REASONIX_STATE_HOME".into())
            .or_insert_with(|| real.to_string_lossy().into_owned());
        let cache = if env.contains_key("REASONIX_HOME") {
            real.join("cache")
        } else {
            tower_pal::cache_home(env)?.join("reasonix")
        };
        env.entry("REASONIX_CACHE_HOME".into())
            .or_insert_with(|| cache.to_string_lossy().into_owned());
        env.insert(
            "REASONIX_HOME".into(),
            overlay.to_string_lossy().into_owned(),
        );
        argv.extend(["--dir".into(), spec.cwd.clone()]);
        Ok(())
    }
}
