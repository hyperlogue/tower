use crate::{Harness, runtime::AgentSpec};
use std::{io, path::Path};
use tower_pal::Environment;

pub struct AgentInfo {
    pub id: &'static str,
    pub program: &'static str,
    pub label: &'static str,
    pub aliases: &'static [&'static str],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Execution {
    Hooks,
    CodexAppServer,
}

pub struct StatusBehavior {
    pub session_start_is_root: bool,
    pub reset_on_session_change: bool,
}

#[derive(Default)]
pub struct ResumeOptions {
    pub values: &'static [&'static str],
    pub switches_to_remove: &'static [&'static str],
    pub values_to_remove: &'static [&'static str],
    pub optional_values_to_remove: &'static [&'static str],
    pub multiple_values: &'static [&'static str],
}

pub trait AgentDefinition: Send + Sync {
    fn kind(&self) -> Harness;
    fn info(&self) -> AgentInfo;
    fn execution(&self) -> Execution;
    fn status_behavior(&self) -> StatusBehavior;
    fn normalize_hook(
        &self,
        name: &str,
        value: &serde_json::Value,
        event: &mut crate::events::AgentEvent,
    );
    fn hook_session(&self, env: &Environment) -> Box<dyn crate::hook::HookSession>;
    fn supports_fork(&self) -> bool;
    fn resume_args(&self, id: &str, fork: bool) -> Option<Vec<String>>;
    fn prohibited_options(&self) -> &'static [&'static str];
    fn resume_options(&self) -> ResumeOptions;
    fn identity_options(&self) -> &'static [&'static str];
    fn configure_launch(
        &self,
        spec: &AgentSpec,
        dir: &Path,
        exe: &Path,
        env: &mut Environment,
        argv: &mut Vec<String>,
    ) -> io::Result<()>;
}
