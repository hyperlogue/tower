use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Harness {
    ClaudeCode,
    Codex,
    Reasonix,
    Kimi,
    Grok,
    Opencode,
    Pi,
    Antigravity,
    Omp,
}
impl Harness {
    pub const ALL: [Self; 9] = [
        Self::ClaudeCode,
        Self::Codex,
        Self::Reasonix,
        Self::Kimi,
        Self::Grok,
        Self::Opencode,
        Self::Pi,
        Self::Antigravity,
        Self::Omp,
    ];
    pub fn definition(self) -> &'static dyn crate::definition::AgentDefinition {
        match self {
            Self::ClaudeCode => &crate::agents::claude::Definition,
            Self::Codex => &crate::agents::codex::Definition,
            Self::Reasonix => &crate::agents::reasonix::Definition,
            Self::Kimi => &crate::agents::kimi::Definition,
            Self::Grok => &crate::agents::grok::Definition,
            Self::Opencode => &crate::agents::opencode::Definition,
            Self::Pi => &crate::agents::pi::Definition,
            Self::Antigravity => &crate::agents::antigravity::Definition,
            Self::Omp => &crate::agents::omp::Definition,
        }
    }
    pub fn parse(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        Self::ALL
            .into_iter()
            .find(|h| h.definition().info().aliases.contains(&name.as_str()))
    }
    pub fn id(self) -> &'static str {
        self.definition().info().id
    }
    pub fn program(self) -> &'static str {
        self.definition().info().program
    }
    pub fn label(self) -> &'static str {
        self.definition().info().label
    }
    pub fn can_fork(self) -> bool {
        self.definition().supports_fork()
    }
    pub fn fork_args(self, id: &str) -> Option<Vec<String>> {
        self.definition().resume_args(id, true)
    }
    pub fn resume_args(self, id: &str) -> Vec<String> {
        self.definition().resume_args(id, false).unwrap_or_default()
    }
}
