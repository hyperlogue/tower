use super::*;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(default)]
pub(super) struct Config {
    enabled: bool,
    interval_s: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_s: 10,
        }
    }
}
impl Service {
    pub(super) fn scan_deadline(&self) -> Result<Option<u64>> {
        if !self.scan.enabled {
            return Ok(None);
        }
        let last = self
            .last_scan
            .lock()
            .map_err(|_| Error::Other("Scanner state unavailable".into()))?;
        let remaining =
            Duration::from_secs(self.scan.interval_s.max(1)).saturating_sub(last.elapsed());
        Ok(Some(now_ms().saturating_add(remaining.as_millis() as u64)))
    }
    pub(super) fn scan_processes(&self) -> Result<()> {
        if !self.scan.enabled {
            return Ok(());
        }
        let mut last = self
            .last_scan
            .lock()
            .map_err(|_| Error::Other("Scanner state unavailable".into()))?;
        if last.elapsed() < Duration::from_secs(self.scan.interval_s.max(1)) {
            return Ok(());
        }
        *last = Instant::now();
        let known = self.all().map_err(Error::Rpc)?;
        let harnesses = self
            .backends
            .iter()
            .flat_map(|b| b.harnesses())
            .collect::<Vec<_>>();
        for pid in tower_pal::processes()?.into_iter().take(50000) {
            if last.elapsed() > Duration::from_secs(2) {
                break;
            }
            if known.iter().any(|r| {
                r.pid == Some(pid)
                    && tower_pal::process_identity(pid)
                        .is_ok_and(|p| Some(p.start_time) == r.pid_start_time)
            }) {
                continue;
            }
            let Ok(process_name) = tower_pal::process_name(pid) else {
                continue;
            };
            if !harnesses.iter().any(|h| h.executable == process_name)
                && !matches!(
                    process_name.as_str(),
                    "node" | "bun" | "python" | "python3" | "sh" | "bash"
                )
            {
                continue;
            }
            let Ok(process) = tower_pal::inspect_process(pid) else {
                continue;
            };
            if process.tower_agent.is_some() {
                continue;
            }
            let Some(harness) = harnesses.iter().find(|h| {
                process.argv.iter().take(2).any(|arg| {
                    std::path::Path::new(arg)
                        .file_name()
                        .is_some_and(|name| name == h.executable.as_str())
                })
            }) else {
                continue;
            };
            if harness.harness == "codex" && process.argv.iter().take(3).any(|s| s == "app-server")
            {
                continue;
            }
            let mut parent = process.identity.parent;
            let mut owned = false;
            for _ in 0..64 {
                if known
                    .iter()
                    .any(|r| r.ownership == "owned" && r.pid == Some(parent))
                {
                    owned = true;
                    break;
                }
                let Ok(identity) = tower_pal::process_identity(parent) else {
                    break;
                };
                if identity.parent <= 1 || identity.parent == parent {
                    break;
                }
                parent = identity.parent;
            }
            if owned {
                continue;
            }
            let request = Request {
                jsonrpc: "2.0".into(),
                id: None,
                method: "agent.register".into(),
                context: Default::default(),
                params: value(RegisterParams {
                    shared_process: false,
                    delivery: None,
                    protocol: None,
                    harness: harness.harness.clone(),
                    cwd: process.cwd.to_string_lossy().into_owned(),
                    native_session_id: None,
                    pid: Some(pid),
                    display_name: None,
                })
                .map_err(Error::Rpc)?,
            };
            let _ = self.register_source(&request, &Actor::Human, now_ms(), "scan");
        }
        Ok(())
    }
}
