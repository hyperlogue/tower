use crate::config::{COMMON, home, hook, invalid, mirror};
use crate::{Harness, definition::*, runtime::AgentSpec};
use serde_json::json;
use std::{io, path::Path};
use tower_pal::Environment;
use tower_pal::{read_config, write_private};
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Kimi
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "kimi",
            program: "kimi",
            label: "Kimi Code",
            aliases: &["kimi", "kimi-code"],
        }
    }
    fn execution(&self) -> Execution {
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
        false
    }
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>> {
        if fork {
            None
        } else {
            Some(vec!["--session".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &[]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &["--config", "--config-file"],
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
        env: &mut Environment,
        _argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let h = self.kind();

        let real = home(env, "KIMI_CODE_HOME", ".kimi")?;
        let overlay = dir.join("kimi");
        mirror(
            &real,
            &overlay,
            &["config.toml"],
            &["sessions", "credentials", "logs", "updates", "user-history"],
            &["session_index.jsonl"],
        )?;
        let mut config: toml::Table = read_config(&real.join("config.toml"))?
            .map(|s| s.parse())
            .transpose()
            .map_err(io::Error::other)?
            .unwrap_or_default();
        let hooks = config
            .entry("hooks")
            .or_insert(toml::Value::Array(vec![]))
            .as_array_mut()
            .ok_or_else(|| invalid("Kimi hooks must be an array"))?;
        let mut names = COMMON.to_vec();
        names.extend([
            "TurnStarted",
            "PermissionResult",
            "PostCompact",
            "Interrupt",
        ]);
        for name in names {
            hooks.push(
                toml::Value::try_from(json!({"event":name,"command":hook(exe,h,name),"timeout":5}))
                    .map_err(io::Error::other)?,
            );
        }
        write_private(
            &overlay.join("config.toml"),
            toml::to_string(&config)
                .map_err(io::Error::other)?
                .as_bytes(),
        )?;
        env.insert(
            "KIMI_CODE_HOME".into(),
            overlay.to_string_lossy().into_owned(),
        );
        Ok(())
    }
}
