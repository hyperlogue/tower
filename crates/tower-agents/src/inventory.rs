use crate::{Harness, events::native_id, runtime::NativeSession};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tower_pal::Environment;

const MAX_ENTRIES: usize = 50_000;
const MAX_CANDIDATES: usize = 4096;
const WINDOW: usize = 64 * 1024;
const MAX_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Candidate {
    pub harness: Harness,
    pub native: NativeSession,
    pub cwd: String,
    pub title: Option<String>,
    pub updated_at: u64,
    pub namespace: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub struct InventoryResult {
    pub candidates: Vec<Candidate>,
    pub errors: Vec<String>,
    pub truncated: bool,
}
#[derive(Default)]
pub struct Inventory {
    caches: BTreeMap<String, (Instant, InventoryResult)>,
}
impl Inventory {
    pub fn list(
        &mut self,
        env: &Environment,
        harness: Option<Harness>,
        refresh: bool,
    ) -> InventoryResult {
        let selected: Vec<_> = Harness::ALL
            .into_iter()
            .filter(|candidate| harness.is_none_or(|h| h == *candidate))
            .collect();
        self.list_selected(env, &selected, refresh)
    }
    pub fn list_selected(
        &mut self,
        env: &Environment,
        selected: &[Harness],
        refresh: bool,
    ) -> InventoryResult {
        let mut result = InventoryResult::default();
        let until = Instant::now() + Duration::from_secs(5);
        for h in Harness::ALL {
            if !selected.contains(&h) {
                continue;
            }
            let key = format!("{}:{:?}:{:?}", h.id(), roots(h, env), namespace(h, env));
            let cached = self
                .caches
                .get(&key)
                .filter(|(at, _)| !refresh && at.elapsed() < Duration::from_secs(2));
            let entries = if let Some((_, entries)) = cached {
                entries.clone()
            } else {
                if Instant::now() >= until {
                    result.truncated = true;
                    result
                        .errors
                        .push(format!("{}: inventory_budget_exhausted", h.id()));
                    continue;
                }
                let mut entries = scan(h, env, until);
                for c in &mut entries.candidates {
                    c.namespace = namespace(h, env);
                }
                self.caches
                    .retain(|k, _| !k.starts_with(&format!("{}:", h.id())));
                self.caches.insert(key, (Instant::now(), entries.clone()));
                entries
            };
            result.candidates.extend(entries.candidates);
            result.errors.extend(entries.errors);
            result.truncated |= entries.truncated;
        }
        result.candidates.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.harness.id().cmp(b.harness.id()))
                .then_with(|| a.native.id.cmp(&b.native.id))
        });
        result
    }
    pub fn resolve(&mut self, env: &Environment, h: Harness, id: &str) -> io::Result<Candidate> {
        if !native_id(id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native session identity",
            ));
        }
        let result = self.list(env, Some(h), true);
        let mut matches = result.candidates.into_iter().filter(|c| c.native.id == id);
        let candidate = matches.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                if result.truncated || !result.errors.is_empty() {
                    "native inventory is incomplete; use the native CLI to locate this conversation"
                } else {
                    "native conversation was not found"
                },
            )
        })?;
        if matches.next().is_some() {
            return Err(io::Error::other("native session identity is ambiguous"));
        }
        Ok(candidate)
    }
}
pub fn namespace(h: Harness, env: &Environment) -> Option<String> {
    let path = match h {
        Harness::Kimi => configured(env, "KIMI_CODE_HOME", ".kimi")?.join("sessions"),
        Harness::Antigravity => {
            configured(env, "GEMINI_DIR", ".gemini")?.join("antigravity-cli/brain")
        }
        Harness::Opencode => configured(env, "XDG_DATA_HOME", ".local/share")?.join("opencode"),
        _ => roots(h, env)?.0,
    };
    for parent in path.ancestors() {
        if let Ok(real) = std::fs::canonicalize(parent) {
            let suffix = path.strip_prefix(parent).ok()?;
            let resolved = if suffix.as_os_str().is_empty() {
                real
            } else {
                real.join(suffix)
            };
            return Some(resolved.to_string_lossy().into_owned());
        }
    }
    Some(path.to_string_lossy().into_owned())
}
fn configured(env: &Environment, key: &str, fallback: &str) -> Option<PathBuf> {
    env.get(key)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| env.get("HOME").map(|home| Path::new(home).join(fallback)))
        .filter(|p| p.is_absolute())
}
fn roots(h: Harness, env: &Environment) -> Option<(PathBuf, usize)> {
    Some(match h {
        Harness::ClaudeCode => (
            configured(env, "CLAUDE_CONFIG_DIR", ".claude")?.join("projects"),
            1,
        ),
        Harness::Codex => (configured(env, "CODEX_HOME", ".codex")?.join("sessions"), 3),
        Harness::Reasonix => (
            env.get("REASONIX_STATE_HOME")
                .map(PathBuf::from)
                .or_else(|| configured(env, "REASONIX_HOME", ".reasonix"))?
                .join("sessions"),
            0,
        ),
        Harness::Kimi => (configured(env, "KIMI_CODE_HOME", ".kimi")?, 3),
        Harness::Grok => (configured(env, "GROK_HOME", ".grok")?.join("sessions"), 2),
        Harness::Pi => {
            if let Some(path) = env
                .get("PI_CODING_AGENT_SESSION_DIR")
                .filter(|s| !s.is_empty())
            {
                (PathBuf::from(path), 0)
            } else {
                (
                    configured(env, "PI_CODING_AGENT_DIR", ".pi/agent")?.join("sessions"),
                    1,
                )
            }
        }
        Harness::Omp => (
            configured(env, "PI_CODING_AGENT_DIR", ".omp/agent")?.join("sessions"),
            1,
        ),
        Harness::Antigravity => (
            configured(env, "GEMINI_DIR", ".gemini")?.join("antigravity-cli"),
            0,
        ),
        Harness::Opencode => return None,
    })
}
struct Scan {
    h: Harness,
    entries: usize,
    bytes: usize,
    until: Instant,
    truncated: bool,
    errors: HashSet<String>,
}
impl Scan {
    fn available(&mut self) -> bool {
        if self.entries >= MAX_ENTRIES || self.bytes >= MAX_BYTES || Instant::now() >= self.until {
            self.truncated = true;
            false
        } else {
            true
        }
    }
    fn error(&mut self, e: &io::Error) {
        if e.kind() != io::ErrorKind::NotFound {
            self.errors
                .insert(format!("{}: metadata_unavailable", self.h.id()));
        }
    }
    fn walk(&mut self, root: &Path, depth: usize, out: &mut Vec<(PathBuf, u64)>) {
        if !self.available() {
            return;
        }
        let entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(e) => {
                self.error(&e);
                return;
            }
        };
        for entry in entries {
            self.entries += 1;
            if !self.available() {
                break;
            }
            let Ok(entry) = entry else {
                self.errors
                    .insert(format!("{}: directory_unavailable", self.h.id()));
                continue;
            };
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() && depth > 0 {
                self.walk(&entry.path(), depth - 1, out);
            } else if kind.is_file()
                && depth == 0
                && let Ok(m) = entry.metadata()
            {
                out.push((entry.path(), millis(m.modified().unwrap_or(UNIX_EPOCH))));
            }
        }
    }
    fn bytes(&mut self, path: &Path, tail: bool) -> Option<Vec<u8>> {
        if !self.available() {
            return None;
        }
        let mut file = match tower_pal::open_regular(path) {
            Ok(f) => f,
            Err(e) => {
                self.error(&e);
                return None;
            }
        };
        if !file.metadata().ok()?.is_file() {
            return None;
        }
        let size = file.metadata().ok()?.len();
        let start = if tail {
            size.saturating_sub(WINDOW as u64)
        } else {
            0
        };
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut bytes = Vec::new();
        file.take(WINDOW as u64).read_to_end(&mut bytes).ok()?;
        self.bytes += bytes.len();
        if start > 0 {
            let n = bytes
                .iter()
                .position(|b| *b == b'\n')
                .map_or(bytes.len(), |n| n + 1);
            bytes.drain(..n);
        }
        Some(bytes)
    }
    fn records(&mut self, path: &Path, tail: bool) -> Vec<Value> {
        self.bytes(path, tail)
            .unwrap_or_default()
            .split(|b| *b == b'\n')
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }
    fn object(&mut self, path: &Path) -> Option<Value> {
        if std::fs::metadata(path).is_ok_and(|m| m.len() > WINDOW as u64) {
            self.truncated = true;
            return None;
        }
        let bytes = self.bytes(path, false)?;
        serde_json::from_slice(&bytes).ok()
    }
}
fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn string<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.pointer(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}
fn title(s: Option<&str>) -> Option<String> {
    s.map(|s| s.chars().filter(|c| !c.is_control()).take(200).collect())
}
fn candidate(
    h: Harness,
    id: &str,
    cwd: &str,
    name: Option<&str>,
    updated_at: u64,
    reference: Option<&Path>,
) -> Option<Candidate> {
    if !native_id(id)
        || reference.is_some_and(|p| p.to_str().is_none())
        || cwd.len() > 4096
        || !Path::new(cwd).is_absolute()
        || cwd.chars().any(char::is_control)
    {
        return None;
    }
    Some(Candidate {
        namespace: None,
        harness: h,
        native: NativeSession {
            id: id.into(),
            resume_ref: reference.map(|p| p.to_string_lossy().into_owned()),
        },
        cwd: cwd.into(),
        title: title(name),
        updated_at,
    })
}
fn scan(h: Harness, env: &Environment, budget: Instant) -> InventoryResult {
    if h == Harness::Opencode {
        return opencode(env, budget);
    }
    let Some((root, depth)) = roots(h, env) else {
        return InventoryResult::default();
    };
    if !root.is_absolute() {
        return InventoryResult {
            errors: vec![format!("{}: home_must_be_absolute", h.id())],
            ..Default::default()
        };
    }
    let mut scan = Scan {
        h,
        entries: 0,
        bytes: 0,
        until: (Instant::now() + Duration::from_secs(2)).min(budget),
        truncated: false,
        errors: Default::default(),
    };
    let mut files = Vec::new();
    let mut index = BTreeMap::new();
    if h == Harness::Kimi {
        let index_path = root.join("session_index.jsonl");
        if std::fs::metadata(&index_path).is_ok_and(|m| m.len() > WINDOW as u64) {
            scan.truncated = true;
        }
        for row in scan.records(&index_path, true) {
            if let (Some(id), Some(cwd)) = (string(&row, "/sessionId"), string(&row, "/workDir")) {
                index.insert(id.to_owned(), cwd.to_owned());
            }
        }
        scan.walk(&root.join("sessions"), 2, &mut files);
    } else if h == Harness::Antigravity {
        if let Some(Value::Object(map)) = scan.object(&root.join("cache/last_conversations.json")) {
            for (cwd, id) in map {
                if let Some(id) = id.as_str().filter(|id| native_id(id)) {
                    index.insert(id.to_owned(), cwd);
                }
            }
        }
        let history = root.join("history.jsonl");
        if std::fs::metadata(&history).is_ok_and(|m| m.len() > WINDOW as u64) {
            scan.truncated = true;
        }
        for row in scan.records(&history, true) {
            if let (Some(id), Some(cwd)) =
                (string(&row, "/conversationId"), string(&row, "/workspace"))
                && native_id(id)
            {
                index.insert(id.to_owned(), cwd.to_owned());
            }
        }
        for id in index.keys().filter(|id| native_id(id)) {
            let path = root
                .join("brain")
                .join(id)
                .join(".system_generated/logs/transcript.jsonl");
            if let Ok(meta) = path.metadata() {
                files.push((path, millis(meta.modified().unwrap_or(UNIX_EPOCH))));
            }
        }
    } else {
        scan.walk(&root, depth, &mut files);
    }
    files.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (path, mtime) in files {
        if out.len() >= MAX_CANDIDATES || !scan.available() {
            scan.truncated = true;
            break;
        }
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let found = match h {
            Harness::Kimi if name == "state.json" => {
                let id = path
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                let data = scan.object(&path).unwrap_or(Value::Null);
                index
                    .get(id)
                    .and_then(|cwd| candidate(h, id, cwd, string(&data, "/title"), mtime, None))
            }
            Harness::Grok if name == "summary.json" => scan.object(&path).and_then(|data| {
                if string(&data, "/session_kind")
                    .is_some_and(|s| s.eq_ignore_ascii_case("subagent"))
                {
                    return None;
                }
                let id = path.parent()?.file_name()?.to_str()?;
                candidate(
                    h,
                    id,
                    string(&data, "/info/cwd")?,
                    string(&data, "/generated_title"),
                    mtime,
                    None,
                )
            }),
            Harness::Reasonix
                if name.ends_with(".jsonl")
                    && ![".events.jsonl", ".conflicts.jsonl", ".guardian.jsonl"]
                        .iter()
                        .any(|suffix| name.ends_with(suffix)) =>
            {
                let meta = PathBuf::from(format!("{}.meta", path.to_string_lossy()));
                scan.object(&meta).and_then(|data| {
                    candidate(
                        h,
                        string(&data, "/id").unwrap_or(name.trim_end_matches(".jsonl")),
                        string(&data, "/workspace_root")?,
                        string(&data, "/custom_title"),
                        mtime,
                        None,
                    )
                })
            }
            Harness::Antigravity => {
                let id = path
                    .ancestors()
                    .nth(3)
                    .and_then(Path::file_name)
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                index
                    .get(id)
                    .and_then(|cwd| candidate(h, id, cwd, None, mtime, None))
            }
            Harness::ClaudeCode | Harness::Codex | Harness::Pi | Harness::Omp
                if name.ends_with(".jsonl") =>
            {
                let records = scan.records(&path, false);
                let tail = scan.records(&path, true);
                match h {
                    Harness::ClaudeCode => {
                        let cwd = tail
                            .iter()
                            .rev()
                            .filter(|r| matches!(r["type"].as_str(), Some("assistant" | "user")))
                            .find_map(|r| string(r, "/cwd"))
                            .or_else(|| records.iter().find_map(|r| string(r, "/cwd")));
                        let custom = tail
                            .iter()
                            .rev()
                            .find(|r| r["type"] == "custom-title")
                            .and_then(|r| string(r, "/customTitle"));
                        cwd.and_then(|cwd| {
                            candidate(h, name.trim_end_matches(".jsonl"), cwd, custom, mtime, None)
                        })
                    }
                    Harness::Codex => records
                        .iter()
                        .find(|r| r["type"] == "session_meta")
                        .and_then(|r| {
                            let p = &r["payload"];
                            if p.get("source")
                                .is_some_and(|s| s.is_object() && s.get("subagent").is_some())
                                || string(p, "/thread_source").is_some_and(|s| s != "user")
                            {
                                return None;
                            }
                            candidate(h, string(p, "/id")?, string(p, "/cwd")?, None, mtime, None)
                        }),
                    _ => records
                        .first()
                        .filter(|r| r["type"] == "session")
                        .and_then(|r| {
                            let title = if h == Harness::Omp {
                                records
                                    .iter()
                                    .find(|r| r["type"] == "title")
                                    .and_then(|r| string(r, "/title"))
                            } else {
                                tail.iter()
                                    .rev()
                                    .find(|r| r["type"] == "session_info")
                                    .and_then(|r| string(r, "/name"))
                            };
                            candidate(
                                h,
                                string(r, "/id")?,
                                string(r, "/cwd")?,
                                title,
                                mtime,
                                Some(&path),
                            )
                        }),
                }
            }
            _ => None,
        };
        if let Some(candidate) = found {
            out.push(candidate);
        }
    }
    if h == Harness::Codex
        && let Some(home) = root.parent()
    {
        let _ = codex_titles(&home.join("state_5.sqlite"), &mut out, budget);
    }
    InventoryResult {
        candidates: out,
        errors: scan.errors.into_iter().collect(),
        truncated: scan.truncated,
    }
}
fn codex_titles(path: &Path, candidates: &mut [Candidate], until: Instant) -> rusqlite::Result<()> {
    use rusqlite::{Connection, OpenFlags, OptionalExtension};
    if !path.is_file() {
        return Ok(());
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_millis(25))?;
    let query = "SELECT COALESCE(NULLIF(TRIM(name), ''),title) FROM threads WHERE id=?1 LIMIT 1";
    let mut query = conn
        .prepare(query)
        .or_else(|_| conn.prepare("SELECT title FROM threads WHERE id=?1 LIMIT 1"))?;
    for c in candidates {
        if Instant::now() >= until {
            break;
        }
        if let Some(value) = query
            .query_row([&c.native.id], |r| r.get::<_, Option<String>>(0))
            .optional()?
            .flatten()
        {
            c.title = title(Some(value.trim()));
        }
    }
    Ok(())
}
fn opencode(env: &Environment, budget: Instant) -> InventoryResult {
    if tower_pal::find_program("opencode", env).is_none() {
        return Default::default();
    }
    let result = (|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (code, body) = rt.block_on(tower_pal::capture(
            &[
                "opencode", "session", "list", "--format", "json", "-n", "4096",
            ]
            .map(str::to_owned),
            env,
            Path::new("/"),
            Duration::from_secs(3).min(budget.saturating_duration_since(Instant::now())),
            4 * 1024 * 1024,
        ))?;
        if code != 0 {
            return Err(io::Error::other("native command failed"));
        }
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(Vec::new());
        }
        let values: Vec<Value> = serde_json::from_slice(&body)?;
        Ok::<_, io::Error>(
            values
                .into_iter()
                .filter_map(|v| {
                    if v.get("parentID").is_some_and(|p| !p.is_null()) {
                        return None;
                    }
                    candidate(
                        Harness::Opencode,
                        string(&v, "/id")?,
                        string(&v, "/directory")?,
                        string(&v, "/title"),
                        v["updated"].as_u64().or(v["created"].as_u64()).unwrap_or(0),
                        None,
                    )
                })
                .collect::<Vec<_>>(),
        )
    })();
    match result {
        Ok(candidates) => InventoryResult {
            truncated: candidates.len() >= MAX_CANDIDATES,
            candidates,
            ..Default::default()
        },
        Err(_) => InventoryResult {
            errors: vec!["opencode: inventory_unavailable".into()],
            ..Default::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write(root: &Path, path: &str, data: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }
    #[test]
    fn inventories_use_native_identity_and_metadata_without_prompts_or_subagents() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        write(
            r,
            ".claude/projects/p/claude-id.jsonl",
            "{\"type\":\"user\",\"cwd\":\"/work/a\",\"message\":\"SECRET-PROMPT\"}\n{\"type\":\"custom-title\",\"customTitle\":\"Renamed\"}\n",
        );
        write(
            r,
            ".claude/projects/p/subagents/child.jsonl",
            "{\"cwd\":\"/child\"}\n",
        );
        write(
            r,
            ".codex/sessions/2026/01/01/rollout-one.jsonl",
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-id\",\"cwd\":\"/work/b\",\"source\":\"cli\"}}\n",
        );
        write(
            r,
            ".codex/sessions/2026/01/01/rollout-child.jsonl",
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-child\",\"cwd\":\"/child\",\"source\":{\"subagent\":\"review\"}}}\n",
        );
        write(r, ".reasonix/sessions/reasonix-id.jsonl", "{}\n");
        write(
            r,
            ".reasonix/sessions/reasonix-id.jsonl.meta",
            "{\"id\":\"reasonix-id\",\"workspace_root\":\"/work/c\",\"custom_title\":\"Reason\"}",
        );
        write(r, ".reasonix/sessions/damaged.events.jsonl", "{}\n");
        write(
            r,
            ".kimi/session_index.jsonl",
            "{\"sessionId\":\"kimi-id\",\"workDir\":\"/wrong\"}\n{\"sessionId\":\"kimi-id\",\"workDir\":\"/work/d\"}\n",
        );
        write(
            r,
            ".kimi/sessions/bucket/kimi-id/state.json",
            "{\"title\":\"Kimi\",\"lastPrompt\":\"SECRET-PROMPT\"}",
        );
        write(
            r,
            ".grok/sessions/bucket/grok-id/summary.json",
            "{\"info\":{\"cwd\":\"/work/e\"},\"generated_title\":\"Grok\"}",
        );
        write(
            r,
            ".grok/sessions/bucket/grok-child/summary.json",
            "{\"info\":{\"cwd\":\"/child\"},\"session_kind\":\"subagent\"}",
        );
        write(
            r,
            ".pi/agent/sessions/bucket/session.jsonl",
            "{\"type\":\"session\",\"id\":\"pi-id\",\"cwd\":\"/work/f\"}\n{\"type\":\"session_info\",\"name\":\"Pi\"}\n",
        );
        write(
            r,
            ".omp/agent/sessions/bucket/session.jsonl",
            "{\"type\":\"session\",\"id\":\"omp-id\",\"cwd\":\"/work/g\"}\n{\"type\":\"title\",\"title\":\"omp\"}\n",
        );
        write(
            r,
            ".gemini/antigravity-cli/cache/last_conversations.json",
            "{\"/work/h\":\"agy-id\",\"/unsafe\":\"..\"}",
        );
        write(
            r,
            ".gemini/antigravity-cli/brain/agy-id/.system_generated/logs/transcript.jsonl",
            "{}\n",
        );
        write(
            r,
            "bin/opencode",
            "#!/bin/sh\nprintf '%s' '[{\"id\":\"opencode-id\",\"directory\":\"/work/i\",\"title\":\"Open\",\"updated\":17}]'\n",
        );
        tower_pal::make_executable(&r.join("bin/opencode")).unwrap();
        let env = Environment::from([
            ("HOME".into(), r.to_string_lossy().into_owned()),
            ("PATH".into(), r.join("bin").to_string_lossy().into_owned()),
        ]);
        let mut inventory = Inventory::default();
        let found = inventory.list(&env, None, true);
        assert!(found.errors.is_empty(), "{:?}", found.errors);
        assert!(!found.truncated);
        assert_eq!(found.candidates.len(), 9, "{found:?}");
        assert!(!format!("{found:?}").contains("SECRET-PROMPT"));
        let pi = inventory.resolve(&env, Harness::Pi, "pi-id").unwrap();
        assert!(pi.native.resume_ref.unwrap().ends_with("session.jsonl"));
        assert_eq!(
            inventory
                .resolve(&env, Harness::Kimi, "kimi-id")
                .unwrap()
                .cwd,
            "/work/d"
        );
        assert!(
            inventory
                .resolve(&env, Harness::Codex, "codex-child")
                .is_err()
        );
        assert_eq!(
            found
                .candidates
                .iter()
                .find(|c| c.harness == Harness::ClaudeCode)
                .unwrap()
                .title
                .as_deref(),
            Some("Renamed")
        );
        write(r, ".claude/projects/p/new-id.jsonl", "{\"cwd\":\"/new\"}\n");
        assert_eq!(
            inventory
                .list(&env, Some(Harness::ClaudeCode), false)
                .candidates
                .len(),
            1
        );
        assert_eq!(
            inventory
                .list(&env, Some(Harness::ClaudeCode), true)
                .candidates
                .len(),
            2
        );
    }
    #[test]
    fn duplicate_native_ids_are_not_resolved_arbitrarily() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        for p in ["a", "b"] {
            write(
                r,
                &format!(".pi/agent/sessions/{p}/session.jsonl"),
                &format!("{{\"type\":\"session\",\"id\":\"duplicate\",\"cwd\":\"/{p}\"}}\n"),
            );
        }
        let env = Environment::from([("HOME".into(), r.to_string_lossy().into_owned())]);
        assert!(
            Inventory::default()
                .resolve(&env, Harness::Pi, "duplicate")
                .is_err()
        );
        assert!(!native_id(".."));
        assert!(!native_id("-option"));
    }
}
