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
    pub fn can_fork(self) -> bool {
        !matches!(self, Self::Kimi | Self::Antigravity)
    }
    pub fn fork_args(self, id: &str) -> Option<Vec<String>> {
        Some(match self {
            Self::Codex => vec!["fork".into(), id.into()],
            Self::Pi | Self::Omp => vec!["--fork".into(), id.into()],
            Self::ClaudeCode | Self::Grok => {
                let mut args = self.resume_args(id);
                args.push("--fork-session".into());
                args
            }
            Self::Reasonix => {
                let mut args = self.resume_args(id);
                args.push("--copy".into());
                args
            }
            Self::Opencode => {
                let mut args = self.resume_args(id);
                args.push("--fork".into());
                args
            }
            _ => return None,
        })
    }
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
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "claude" | "claude-code" | "claudecode" => Self::ClaudeCode,
            "codex" => Self::Codex,
            "reasonix" => Self::Reasonix,
            "kimi" | "kimi-code" => Self::Kimi,
            "grok" | "grok-build" => Self::Grok,
            "opencode" => Self::Opencode,
            "pi" => Self::Pi,
            "antigravity" | "agy" => Self::Antigravity,
            "omp" | "oh-my-pi" => Self::Omp,
            _ => return None,
        })
    }
    pub fn id(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude-code",
            Self::Codex => "codex",
            Self::Reasonix => "reasonix",
            Self::Kimi => "kimi",
            Self::Grok => "grok",
            Self::Opencode => "opencode",
            Self::Pi => "pi",
            Self::Antigravity => "antigravity",
            Self::Omp => "omp",
        }
    }
    pub fn program(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude",
            Self::Antigravity => "agy",
            _ => self.id(),
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::ClaudeCode => "Claude Code",
            Self::Codex => "Codex",
            Self::Reasonix => "Reasonix",
            Self::Kimi => "Kimi Code",
            Self::Grok => "Grok Build",
            Self::Opencode => "opencode",
            Self::Pi => "Pi",
            Self::Antigravity => "Antigravity",
            Self::Omp => "oh-my-pi",
        }
    }
    pub fn resume_args(self, id: &str) -> Vec<String> {
        let selector = match self {
            Self::ClaudeCode | Self::Grok | Self::Omp => "--resume",
            Self::Codex => "resume",
            Self::Reasonix => "-r",
            Self::Kimi | Self::Pi => "--session",
            Self::Opencode => "-s",
            Self::Antigravity => "--conversation",
        };
        vec![selector.into(), id.into()]
    }
}
