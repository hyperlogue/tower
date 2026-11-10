use crate::config::{invalid, mirror};
use crate::{Harness, definition::*, runtime::AgentSpec};
use std::path::PathBuf;
use std::{io, path::Path};
use tower_pal::Environment;
use tower_pal::write_private;
pub struct Definition;
impl AgentDefinition for Definition {
    fn kind(&self) -> Harness {
        Harness::Opencode
    }
    fn info(&self) -> AgentInfo {
        AgentInfo {
            id: "opencode",
            program: "opencode",
            label: "opencode",
            aliases: &["opencode"],
        }
    }
    fn execution(&self, _spec: &AgentSpec) -> Execution {
        Execution::Hooks
    }
    fn status_behavior(&self) -> StatusBehavior {
        StatusBehavior {
            session_start_is_root: false,
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
            Some(vec!["-s".into(), id.into(), "--fork".into()])
        } else {
            Some(vec!["-s".into(), id.into()])
        }
    }
    fn prohibited_options(&self) -> &'static [&'static str] {
        &[]
    }
    fn resume_options(&self) -> ResumeOptions {
        ResumeOptions {
            values: &["--agent"],
            values_to_remove: &["-s"],
            ..Default::default()
        }
    }
    fn identity_options(&self) -> &'static [&'static str] {
        &["-s", "--session", "--continue"]
    }
    fn configure_launch(
        &self,
        _spec: &AgentSpec,
        dir: &Path,
        exe: &Path,
        env: &mut Environment,
        _argv: &mut Vec<String>,
    ) -> io::Result<()> {
        let overlay = dir.join("opencode");
        tower_pal::private_dir(&overlay)?;
        if let Some(real) = env.get("OPENCODE_CONFIG_DIR").map(PathBuf::from) {
            if !real.is_absolute() {
                return Err(invalid("OPENCODE_CONFIG_DIR must be absolute"));
            }
            mirror(&real, &overlay, &["plugins"], &[], &[])?;
            mirror(
                &real.join("plugins"),
                &overlay.join("plugins"),
                &["tower.ts"],
                &[],
                &[],
            )?;
        } else {
            tower_pal::private_dir(&overlay.join("plugins"))?;
        }
        write_private(
            &overlay.join("plugins/tower.ts"),
            crate::extensions::opencode(exe).as_bytes(),
        )?;
        env.insert(
            "OPENCODE_CONFIG_DIR".into(),
            overlay.to_string_lossy().into_owned(),
        );
        Ok(())
    }
}
