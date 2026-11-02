use crate::{
    Harness,
    events::native_id,
    runtime::{NativeSession, RuntimeState},
};
use serde::Deserialize;
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

#[derive(Clone, Deserialize)]
struct ClaudeState {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    status: Option<String>,
    name: Option<String>,
    #[serde(rename = "nameSource")]
    name_source: Option<String>,
}
pub struct Activity {
    directory: Option<PathBuf>,
    next: Instant,
    started: SystemTime,
    version: Option<(SystemTime, u64)>,
    cached: Option<ClaudeState>,
}
impl Activity {
    pub fn new(harness: Harness, env: &tower_pal::Environment) -> Self {
        let home = env
            .get("CLAUDE_CONFIG_DIR")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(|| env.get("HOME").map(|s| PathBuf::from(s).join(".claude")));
        Self {
            directory: if harness == Harness::ClaudeCode {
                home.map(|p| p.join("sessions"))
            } else {
                None
            },
            next: Instant::now(),
            started: SystemTime::now(),
            version: None,
            cached: None,
        }
    }
    pub fn refresh(&mut self, state: &mut RuntimeState, now: u64) -> bool {
        if Instant::now() < self.next || state.stop_requested {
            return false;
        }
        self.next = Instant::now() + Duration::from_millis(500);
        let Some(directory) = &self.directory else {
            return false;
        };
        let Some(pid) = state.agent_pid else {
            return false;
        };
        let path = directory.join(format!("{pid}.json"));
        let Ok(meta) = std::fs::metadata(&path) else {
            return false;
        };
        let Ok(modified) = meta.modified() else {
            return false;
        };
        if meta.len() > 65536 || modified + Duration::from_secs(1) < self.started {
            return false;
        }
        let version = (modified, meta.len());
        if self.version != Some(version) {
            let Ok(Some(text)) = tower_pal::read_config(&path) else {
                return false;
            };
            let Ok(value) = serde_json::from_str(&text) else {
                return false;
            };
            self.cached = Some(value);
            self.version = Some(version);
        }
        let Some(file) = &self.cached else {
            return false;
        };
        if file
            .session_id
            .as_ref()
            .is_some_and(|id| state.private.get("fork_parent").and_then(|v| v.as_str()) == Some(id))
        {
            return false;
        }
        let before = (
            state.status.clone(),
            state.native.as_ref().map(|n| n.id.clone()),
            state.display_name.clone(),
            state.detail.background_jobs,
        );
        if let Some(id) = file.session_id.as_ref().filter(|id| native_id(id)) {
            state.native = Some(NativeSession {
                id: id.clone(),
                resume_ref: None,
            });
        }
        if file.name_source.as_deref() != Some("derived")
            && let Some(name) = file.name.as_ref().filter(|s| !s.trim().is_empty())
        {
            state.display_name = Some(name.chars().filter(|c| !c.is_control()).take(200).collect());
        }
        // A coarse native activity flag cannot resolve a specific user question or permission gate.
        if !matches!(
            state.status.as_str(),
            "waiting_input" | "waiting_approval" | "exited" | "lost"
        ) {
            match file.status.as_deref() {
                Some("busy") => {
                    state.status = "working".into();
                    state.detail.background_jobs = None;
                }
                Some("shell") => {
                    state.status = "working".into();
                    state.detail.background_jobs = None;
                }
                Some("idle") => {
                    state.status = "idle".into();
                    state.detail.background_jobs = Some(0);
                }
                _ => {}
            }
        }
        let changed = before
            != (
                state.status.clone(),
                state.native.as_ref().map(|n| n.id.clone()),
                state.display_name.clone(),
                state.detail.background_jobs,
            );
        if changed {
            state.seq = state.seq.saturating_add(1);
            state.updated_at = now;
        }
        changed
    }
}
