//! Discovers unmanaged agent sessions from the files their harnesses write.
use crate::{service, worker::ServiceWorker};
use std::{
    collections::BTreeMap,
    io::{BufRead, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

/// Waits until `dir` exists. Returns false when it cannot be observed.
async fn appear(dir: &Path, what: &str) -> bool {
    while !dir.is_dir() {
        match tower_pal::FileWatch::new(&[dir.to_path_buf()]) {
            Ok(mut watch) if !dir.is_dir() => watch.changed().await,
            Ok(_) => {}
            Err(e) => {
                eprintln!("tower: {what} are unobservable: {e}");
                return false;
            }
        }
    }
    true
}

/// Follows `~/.claude/sessions/<pid>.json`, which Claude Code writes at launch.
pub(crate) async fn claude_sessions(service: ServiceWorker, dir: PathBuf) {
    if !appear(&dir, "Claude sessions").await {
        return;
    }
    let mut watch = match tower_pal::TreeWatch::new(&dir, None) {
        Ok(watch) => watch,
        Err(e) => {
            eprintln!("tower: Claude sessions are unobservable: {e}");
            return;
        }
    };
    let mut seen: BTreeMap<PathBuf, (SystemTime, u64)> = BTreeMap::new();
    loop {
        let mut present = Vec::new();
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            let version = (
                meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                meta.len(),
            );
            present.push(path.clone());
            if meta.len() > 65536 || seen.get(&path) == Some(&version) {
                continue;
            }
            seen.insert(path.clone(), version);
            let Ok(text) = std::fs::read(&path) else {
                continue;
            };
            let Ok(file) = serde_json::from_slice::<service::ClaudeSession>(&text) else {
                continue;
            };
            let _ = service
                .execute(move |s| s.observe_claude_session(file))
                .await;
        }
        seen.retain(|path, _| present.contains(path));
        watch.changed().await;
    }
}

/// Follows Codex rollouts under `$CODEX_HOME/sessions`. A rollout names no
/// process, so the writer is the standalone Codex process holding it open.
pub(crate) async fn codex_rollouts(service: ServiceWorker, root: PathBuf) {
    if !appear(&root, "Codex rollouts").await {
        return;
    }
    let mut watch = match tower_pal::PathWatch::new(&root) {
        Ok(watch) => watch,
        Err(e) => {
            eprintln!("tower: Codex rollouts are unobservable: {e}");
            return;
        }
    };
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let mut rollouts: BTreeMap<PathBuf, Rollout> = BTreeMap::new();
    // Rollouts written by shared app-servers or Tower itself have no standalone
    // writer; look again only after a while so their writes stay cheap.
    let mut unheld: BTreeMap<PathBuf, Instant> = BTreeMap::new();
    let mut changed = None;
    loop {
        let mut holders = None;
        let paths = match changed {
            Some(paths) => paths,
            None => {
                unheld.clear();
                holders = Some(held_rollouts(&root));
                holders
                    .iter()
                    .flat_map(|h| h.keys().cloned())
                    .collect::<std::collections::BTreeSet<_>>()
            }
        };
        unheld.retain(|_, at| at.elapsed() < Duration::from_secs(2));
        for path in paths {
            if !is_rollout(&root, &path) || unheld.contains_key(&path) {
                continue;
            }
            let current = rollouts.remove(&path).filter(Rollout::alive);
            let Some(mut rollout) = current.or_else(|| {
                let (pid, start) = *holders
                    .get_or_insert_with(|| held_rollouts(&root))
                    .get(&path)?;
                Some(Rollout::new(pid, start))
            }) else {
                // A new file can be seen before its creator's descriptor is installed.
                if std::fs::metadata(&path).is_ok_and(|m| m.len() > 0) {
                    unheld.insert(path, Instant::now());
                }
                continue;
            };
            if let Some(session) = rollout.read(&path) {
                let accepted = service
                    .execute(move |s| s.observe_native_session(session))
                    .await;
                rollout.ignored = !matches!(accepted, Ok(Ok(true)));
            }
            rollouts.insert(path, rollout);
        }
        rollouts.retain(|_, r| r.alive());
        changed = watch.changed().await;
    }
}

fn is_rollout(root: &Path, path: &Path) -> bool {
    path.starts_with(root)
        && path.extension().is_some_and(|e| e == "jsonl")
        && path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("rollout-"))
}

/// Rollouts open in running standalone Codex processes, with their holder.
fn held_rollouts(root: &Path) -> BTreeMap<PathBuf, (u32, u64)> {
    codex_processes()
        .flat_map(|(pid, start)| {
            tower_pal::open_files(pid)
                .unwrap_or_default()
                .into_iter()
                .filter(|path| is_rollout(root, path))
                .map(move |path| (path, (pid, start)))
        })
        .collect()
}

/// Codex processes that run their own sessions, excluding shared app-servers.
fn codex_processes() -> impl Iterator<Item = (u32, u64)> {
    tower_pal::processes()
        .unwrap_or_default()
        .into_iter()
        .filter(|pid| tower_pal::process_name(*pid).is_ok_and(|n| n == "codex"))
        .filter_map(|pid| {
            let process = tower_pal::inspect_process(pid).ok()?;
            let codex = process
                .argv
                .iter()
                .take(2)
                .any(|arg| Path::new(arg).file_name().is_some_and(|n| n == "codex"));
            (codex && !process.argv.iter().take(3).any(|s| s == "app-server"))
                .then_some((pid, process.identity.start_time))
        })
}

struct Rollout {
    pid: u32,
    start: u64,
    offset: u64,
    meta: Option<(String, String)>,
    status: &'static str,
    reported: Option<&'static str>,
    ignored: bool,
}
impl Rollout {
    fn new(pid: u32, start: u64) -> Self {
        Self {
            pid,
            start,
            offset: 0,
            meta: None,
            status: "idle",
            reported: None,
            ignored: false,
        }
    }
    fn alive(&self) -> bool {
        tower_pal::process_identity(self.pid).is_ok_and(|p| p.start_time == self.start)
    }
    /// Reads appended lines and returns the session when its status changed.
    fn read(&mut self, path: &Path) -> Option<service::NativeSession> {
        const TAIL: u64 = 1 << 20;
        let mut file = std::fs::File::open(path).ok()?;
        let len = file.metadata().ok()?.len();
        if len < self.offset {
            *self = Self::new(self.pid, self.start);
        }
        let mut reader = std::io::BufReader::new(&mut file);
        if self.meta.is_none() {
            let mut first = Vec::new();
            reader
                .by_ref()
                .take(TAIL)
                .read_until(b'\n', &mut first)
                .ok()?;
            if first.last() != Some(&b'\n') {
                return None;
            }
            self.meta = Some(session_meta(&first)?);
            self.offset = first.len() as u64;
            if len.saturating_sub(self.offset) > TAIL {
                self.offset = len - TAIL;
                reader.seek(SeekFrom::Start(self.offset)).ok()?;
                let mut partial = Vec::new();
                reader.read_until(b'\n', &mut partial).ok()?;
                self.offset += partial.len() as u64;
            }
        }
        reader.seek(SeekFrom::Start(self.offset)).ok()?;
        let mut line = Vec::new();
        loop {
            line.clear();
            match reader.read_until(b'\n', &mut line) {
                Ok(n) if n > 0 && line.last() == Some(&b'\n') => {
                    self.offset += n as u64;
                    if let Some(status) = turn_status(&line) {
                        self.status = status;
                    }
                }
                _ => break,
            }
        }
        if self.ignored || self.reported == Some(self.status) {
            return None;
        }
        self.reported = Some(self.status);
        let (native, cwd) = self.meta.clone()?;
        Some(service::NativeSession {
            harness: "codex",
            pid: self.pid,
            start: self.start,
            native,
            cwd,
            status: self.status,
            display_name: None,
        })
    }
}

/// The thread ID and working directory of a top-level (non-subagent) rollout.
fn session_meta(line: &[u8]) -> Option<(String, String)> {
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    let payload = (value["type"] == "session_meta").then_some(&value["payload"])?;
    payload["source"].is_string().then_some(())?;
    Some((
        payload["id"].as_str()?.to_owned(),
        payload["cwd"].as_str()?.to_owned(),
    ))
}

fn turn_status(line: &[u8]) -> Option<&'static str> {
    let text = std::str::from_utf8(line).ok()?;
    if !text.contains("\"event_msg\"") {
        return None;
    }
    let kind = ["task_started", "task_complete", "turn_aborted"]
        .into_iter()
        .find(|k| text.contains(&format!("\"type\":\"{k}\"")))?;
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    (value["type"] == "event_msg" && value["payload"]["type"] == kind).then_some(
        if kind == "task_started" {
            "working"
        } else {
            "idle"
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rollout_lines_yield_identity_and_turn_status() {
        let meta = br#"{"type":"session_meta","payload":{"id":"t1","cwd":"/w","source":"cli"}}"#;
        assert_eq!(session_meta(meta), Some(("t1".into(), "/w".into())));
        let child = br#"{"type":"session_meta","payload":{"id":"t2","cwd":"/w","source":{"subagent":"review"}}}"#;
        assert_eq!(session_meta(child), None);
        let event = |kind: &str| format!(r#"{{"type":"event_msg","payload":{{"type":"{kind}"}}}}"#);
        assert_eq!(
            turn_status(event("task_started").as_bytes()),
            Some("working")
        );
        assert_eq!(turn_status(event("task_complete").as_bytes()), Some("idle"));
        assert_eq!(turn_status(event("turn_aborted").as_bytes()), Some("idle"));
        assert_eq!(turn_status(event("token_count").as_bytes()), None);
        let item = br#"{"type":"response_item","payload":{"text":"\"type\":\"task_started\""}}"#;
        assert_eq!(turn_status(item), None);
    }
}
