//! Discovers unmanaged agent sessions from the files their harnesses write.
use crate::{service, worker::ServiceWorker};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

/// Starts one discovery task per harness that leaves followable files.
pub(crate) fn start(service: ServiceWorker, home: &Path) -> Vec<tokio::task::JoinHandle<()>> {
    let dir = |var: &str, default: &str| {
        std::env::var_os(var)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(default))
    };
    let mut tasks = vec![tokio::spawn(claude_sessions(
        service.clone(),
        dir("CLAUDE_CONFIG_DIR", ".claude").join("sessions"),
    ))];
    for spec in [
        Spec {
            harness: "codex",
            executable: "codex",
            root: dir("CODEX_HOME", ".codex").join("sessions"),
            format: Format::Codex,
        },
        Spec {
            harness: "pi",
            executable: "pi",
            root: dir("PI_CODING_AGENT_DIR", ".pi/agent").join("sessions"),
            format: Format::Pi,
        },
        Spec {
            harness: "omp",
            executable: "omp",
            root: home.join(".omp/agent/sessions"),
            format: Format::Pi,
        },
        Spec {
            harness: "grok",
            executable: "grok",
            root: dir("GROK_HOME", ".grok").join("sessions"),
            format: Format::Grok,
        },
        Spec {
            harness: "kimi",
            executable: "kimi",
            root: dir("KIMI_SHARE_DIR", ".kimi").join("sessions"),
            format: Format::Kimi,
        },
        Spec {
            harness: "reasonix",
            executable: "reasonix",
            root: dir("REASONIX_HOME", ".reasonix").join("projects"),
            format: Format::Reasonix,
        },
    ] {
        tasks.push(tokio::spawn(transcripts(service.clone(), spec)));
    }
    let data = std::env::var_os("XDG_DATA_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    let database = std::env::var("OPENCODE_DB")
        .ok()
        .filter(|v| !v.is_empty() && v != ":memory:")
        .unwrap_or_else(|| "opencode.db".into());
    tasks.push(tokio::spawn(opencode_sessions(
        service,
        data.join("opencode").join(database),
    )));
    tasks
}

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
async fn claude_sessions(service: ServiceWorker, dir: PathBuf) {
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

/// A harness transcript layout that Tower can follow without hooks.
#[derive(Clone, Copy, PartialEq)]
enum Format {
    /// `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`, held open by its writer.
    Codex,
    /// `<agent>/sessions/<cwd>/<time>_<id>.jsonl` (Pi and its omp fork).
    Pi,
    /// `$GROK_HOME/sessions/<cwd>/<id>/events.jsonl`.
    Grok,
    /// `$REASONIX_HOME/projects/<cwd>/sessions/<id>.jsonl.lease.lock`, held from launch.
    Reasonix,
    /// `$KIMI_SHARE_DIR/sessions/<md5 of cwd>/<id>/wire.jsonl`.
    Kimi,
}
struct Spec {
    harness: &'static str,
    executable: &'static str,
    root: PathBuf,
    format: Format,
}
impl Format {
    /// The followed file for a path below `root`, if it belongs to a session.
    fn tracked(self, root: &Path, path: &Path) -> Option<PathBuf> {
        let rest = path.strip_prefix(root).ok()?;
        let parts = rest.iter().collect::<Vec<_>>();
        let name = path.file_name()?.to_str()?;
        match self {
            Format::Codex => (name.starts_with("rollout-") && name.ends_with(".jsonl"))
                .then(|| path.to_path_buf()),
            Format::Pi => {
                (parts.len() == 2 && name.ends_with(".jsonl")).then(|| path.to_path_buf())
            }
            // Any file a running Grok holds in its session directory names the session.
            Format::Grok => {
                (parts.len() >= 3).then(|| root.join(parts[0]).join(parts[1]).join("events.jsonl"))
            }
            Format::Reasonix => {
                (parts.len() == 3 && parts[1] == "sessions" && name.ends_with(".jsonl.lease.lock"))
                    .then(|| path.to_path_buf())
            }
            Format::Kimi => (parts.len() == 3 && name == "wire.jsonl").then(|| path.to_path_buf()),
        }
    }
    /// Whether a file is only ever written by a process that holds it open.
    fn exact(self) -> bool {
        matches!(self, Format::Codex | Format::Reasonix)
    }
    /// Native ID and working directory known without reading lines.
    fn named(self, path: &Path) -> Option<(String, Option<String>)> {
        match self {
            Format::Grok => {
                let session = path.parent()?;
                let cwd = percent_decode(session.parent()?.file_name()?.to_str()?)?;
                Some((session.file_name()?.to_str()?.to_owned(), Some(cwd)))
            }
            Format::Reasonix => Some((
                path.file_name()?
                    .to_str()?
                    .strip_suffix(".jsonl.lease.lock")?
                    .to_owned(),
                None,
            )),
            Format::Kimi => Some((path.parent()?.file_name()?.to_str()?.to_owned(), None)),
            Format::Codex | Format::Pi => None,
        }
    }
    /// Native ID and working directory from the first line.
    fn header(self, line: &[u8]) -> Option<(String, Option<String>)> {
        let value: serde_json::Value = serde_json::from_slice(line).ok()?;
        let (id, cwd) = match self {
            Format::Codex => {
                let payload = (value["type"] == "session_meta").then_some(&value["payload"])?;
                // Subagent threads run inside their parent's process.
                payload["source"].is_string().then_some(())?;
                (&payload["id"], &payload["cwd"])
            }
            Format::Pi => {
                (value["type"] == "session").then_some(())?;
                (&value["id"], &value["cwd"])
            }
            Format::Grok | Format::Reasonix | Format::Kimi => return None,
        };
        Some((id.as_str()?.to_owned(), Some(cwd.as_str()?.to_owned())))
    }
    fn initial(self) -> &'static str {
        if self == Format::Reasonix {
            "unknown"
        } else {
            "idle"
        }
    }
    fn status(self, line: &[u8]) -> Option<&'static str> {
        let text = std::str::from_utf8(line).ok()?;
        let has = |kind: &str| text.contains(&format!("\"type\":\"{kind}\""));
        match self {
            Format::Codex => {
                if !text.contains("\"event_msg\"") {
                    return None;
                }
                let kind = ["task_started", "task_complete", "turn_aborted"]
                    .into_iter()
                    .find(|k| has(k))?;
                let value: serde_json::Value = serde_json::from_str(text).ok()?;
                (value["type"] == "event_msg" && value["payload"]["type"] == kind).then_some(
                    if kind == "task_started" {
                        "working"
                    } else {
                        "idle"
                    },
                )
            }
            Format::Pi => {
                if !has("message") {
                    return None;
                }
                let value: serde_json::Value = serde_json::from_str(text).ok()?;
                if value["type"] != "message" {
                    return None;
                }
                let message = &value["message"];
                match message["role"].as_str()? {
                    "user" | "toolResult" => Some("working"),
                    "assistant" if message["stopReason"] == "toolUse" => Some("working"),
                    "assistant" => Some("idle"),
                    _ => None,
                }
            }
            Format::Grok => {
                let kind = [
                    "turn_started",
                    "turn_ended",
                    "permission_requested",
                    "permission_resolved",
                ]
                .into_iter()
                .find(|k| has(k))?;
                let value: serde_json::Value = serde_json::from_str(text).ok()?;
                (value["type"] == kind).then_some(match kind {
                    "turn_ended" => "idle",
                    "permission_requested" => "waiting_approval",
                    _ => "working",
                })
            }
            Format::Kimi => {
                let kind = [
                    "TurnBegin",
                    "TurnEnd",
                    "ApprovalRequest",
                    "ApprovalResponse",
                    "QuestionRequest",
                    "QuestionResponse",
                ]
                .into_iter()
                .find(|k| text.contains(k))?;
                let value: serde_json::Value = serde_json::from_str(text).ok()?;
                (value["message"]["type"] == kind).then_some(match kind {
                    "TurnEnd" => "idle",
                    "ApprovalRequest" => "waiting_approval",
                    "QuestionRequest" => "waiting_input",
                    _ => "working",
                })
            }
            Format::Reasonix => None,
        }
    }
}

fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Follows one harness's transcripts and reports the sessions they belong to.
/// Transcripts name no process: the writer is the harness process holding the
/// file open or, for harnesses that close between writes, the only otherwise
/// unclaimed harness process in the session's directory.
async fn transcripts(service: ServiceWorker, spec: Spec) {
    let what = format!("{} sessions", spec.harness);
    if !appear(&spec.root, &what).await {
        return;
    }
    let mut watch = match tower_pal::PathWatch::new(&spec.root) {
        Ok(watch) => watch,
        Err(e) => {
            eprintln!("tower: {what} are unobservable: {e}");
            return;
        }
    };
    let root = std::fs::canonicalize(&spec.root).unwrap_or_else(|_| spec.root.clone());
    let mut sessions: BTreeMap<PathBuf, Transcript> = BTreeMap::new();
    // Files with no standalone writer (shared app-servers, Tower itself, or an
    // ambiguous directory) are looked at again only after a while.
    let mut unheld: BTreeMap<PathBuf, Instant> = BTreeMap::new();
    let mut changed = None;
    loop {
        let mut holders = None;
        let paths = match changed {
            Some(paths) => paths,
            None => {
                unheld.clear();
                let held = held_files(&spec, &root);
                let mut paths = held.keys().cloned().collect::<BTreeSet<_>>();
                if !spec.format.exact() {
                    paths.extend(newest_per_directory(&spec, &root));
                }
                holders = Some(held);
                paths
            }
        };
        unheld.retain(|_, at| at.elapsed() < Duration::from_secs(2));
        let paths = paths
            .iter()
            .filter_map(|p| spec.format.tracked(&root, p))
            .collect::<BTreeSet<_>>();
        for path in paths {
            if unheld.contains_key(&path) {
                continue;
            }
            let current = sessions.remove(&path).filter(Transcript::alive);
            let mut session = match current {
                Some(session) => session,
                None => {
                    let held = holders.get_or_insert_with(|| held_files(&spec, &root));
                    let writer = match held.get(&path) {
                        Some(&writer) => Some(writer),
                        None if spec.format.exact() => None,
                        None => writer_by_directory(&spec, &path, &sessions),
                    };
                    let Some((pid, start)) = writer else {
                        // A new file can be seen before its creator's descriptor is installed.
                        if std::fs::metadata(&path).is_ok_and(|m| m.len() > 0) {
                            unheld.insert(path, Instant::now());
                        }
                        continue;
                    };
                    Transcript::new(spec.format, pid, start)
                }
            };
            if let Some(found) = session.read(spec.harness, &path) {
                let accepted = service
                    .execute(move |s| s.observe_native_session(found))
                    .await;
                session.ignored = !matches!(accepted, Ok(Ok(true)));
            }
            sessions.insert(path, session);
        }
        sessions.retain(|_, s| s.alive());
        // A file may have grown between its last read and the start of its watch.
        let followed = watch.follow(sessions.keys().cloned());
        changed = if followed.is_empty() {
            watch.changed().await
        } else {
            Some(followed)
        };
    }
}

/// Followed files open in running harness processes, with their holder.
fn held_files(spec: &Spec, root: &Path) -> BTreeMap<PathBuf, (u32, u64)> {
    harness_processes(spec.executable)
        .flat_map(|(pid, start, _)| {
            tower_pal::open_files(pid)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|path| spec.format.tracked(root, &path))
                .map(move |path| (path, (pid, start)))
        })
        .collect()
}

/// The most recently written transcript in each working directory, which is
/// the one a running harness there would be appending to.
fn newest_per_directory(spec: &Spec, root: &Path) -> Vec<PathBuf> {
    let entries = |dir: &Path| {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .take(100_000)
            .map(|e| e.path())
            .collect::<Vec<_>>()
    };
    let modified = |path: &Path| std::fs::metadata(path).and_then(|m| m.modified()).ok();
    entries(root)
        .into_iter()
        .filter(|dir| dir.is_dir())
        .filter_map(|dir| {
            entries(&dir)
                .into_iter()
                .filter_map(|path| {
                    let tracked = spec
                        .format
                        .tracked(root, &path)
                        .or_else(|| spec.format.tracked(root, &path.join("events.jsonl")))
                        .or_else(|| spec.format.tracked(root, &path.join("wire.jsonl")))?;
                    Some((modified(&tracked)?, tracked))
                })
                .max()
                .map(|(_, path)| path)
        })
        .collect()
}

/// The harness process in the transcript's working directory. A process already
/// following another transcript counts only when it is the sole candidate: it
/// started a new conversation.
fn writer_by_directory(
    spec: &Spec,
    path: &Path,
    sessions: &BTreeMap<PathBuf, Transcript>,
) -> Option<(u32, u64)> {
    let taken = |c| sessions.values().any(|s| (s.pid, s.start) == c);
    if spec.format == Format::Kimi {
        // Kimi names the directory after the MD5 digest of its working directory.
        use md5::Digest;
        let hash = path.parent()?.parent()?.file_name()?.to_str()?;
        return process_where(
            spec.executable,
            |cwd| {
                cwd.to_str().is_some_and(|cwd| {
                    let digest = md5::Md5::digest(cwd.as_bytes());
                    digest
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                        == hash
                })
            },
            taken,
        );
    }
    let cwd = match spec.format.named(path) {
        Some((_, cwd)) => cwd,
        None => {
            let mut first = Vec::new();
            std::io::BufReader::new(std::fs::File::open(path).ok()?)
                .take(1 << 20)
                .read_until(b'\n', &mut first)
                .ok()?;
            spec.format.header(&first)?.1
        }
    }?;
    let cwd = std::fs::canonicalize(&cwd).unwrap_or_else(|_| PathBuf::from(cwd));
    process_in(spec.executable, &cwd, taken)
}

/// The `executable` process running in `cwd`. A process `taken` by another
/// session counts only when it is the sole candidate: it started a new one.
fn process_in(
    executable: &str,
    cwd: &Path,
    taken: impl Fn((u32, u64)) -> bool,
) -> Option<(u32, u64)> {
    process_where(executable, |dir| dir == cwd, taken)
}

fn process_where(
    executable: &str,
    cwd: impl Fn(&Path) -> bool,
    taken: impl Fn((u32, u64)) -> bool,
) -> Option<(u32, u64)> {
    let candidates = harness_processes(executable)
        .filter(|(_, _, dir)| cwd(dir))
        .map(|(pid, start, _)| (pid, start))
        .collect::<Vec<_>>();
    let free = candidates
        .iter()
        .filter(|c| !taken(**c))
        .collect::<Vec<_>>();
    match free.as_slice() {
        [only] => Some(**only),
        [] if candidates.len() == 1 => Some(candidates[0]),
        _ => None,
    }
}

/// Running processes of `executable` that host their own sessions. A launcher
/// script and the binary it runs count once, as the launcher.
fn harness_processes(executable: &str) -> impl Iterator<Item = (u32, u64, PathBuf)> {
    let found = tower_pal::processes()
        .unwrap_or_default()
        .into_iter()
        .filter(move |pid| {
            tower_pal::process_name(*pid)
                .is_ok_and(|n| n == executable || matches!(n.as_str(), "node" | "bun"))
        })
        .filter_map(move |pid| {
            let process = tower_pal::inspect_process(pid).ok()?;
            let named = process
                .argv
                .iter()
                .take(2)
                .any(|arg| Path::new(arg).file_name().is_some_and(|n| n == executable));
            // A shared Codex app-server hosts sessions for other clients.
            (named && !process.argv.iter().take(3).any(|s| s == "app-server")).then_some((
                pid,
                process.identity,
                process.cwd,
            ))
        })
        .collect::<Vec<_>>();
    let pids = found.iter().map(|(pid, ..)| *pid).collect::<BTreeSet<_>>();
    found
        .into_iter()
        .filter(move |(_, identity, _)| !pids.contains(&identity.parent))
        .map(|(pid, identity, cwd)| (pid, identity.start_time, cwd))
}

struct Transcript {
    format: Format,
    pid: u32,
    start: u64,
    offset: u64,
    identity: Option<(String, Option<String>)>,
    status: &'static str,
    reported: Option<&'static str>,
    ignored: bool,
}
impl Transcript {
    fn new(format: Format, pid: u32, start: u64) -> Self {
        Self {
            format,
            pid,
            start,
            offset: 0,
            identity: None,
            status: format.initial(),
            reported: None,
            ignored: false,
        }
    }
    fn alive(&self) -> bool {
        tower_pal::process_identity(self.pid).is_ok_and(|p| p.start_time == self.start)
    }
    /// Reads appended lines and returns the session when its status changed.
    fn read(&mut self, harness: &'static str, path: &Path) -> Option<service::NativeSession> {
        const TAIL: u64 = 1 << 20;
        if self.identity.is_none() {
            self.identity = self.format.named(path);
        }
        if self.format != Format::Reasonix {
            let mut file = std::fs::File::open(path).ok()?;
            let len = file.metadata().ok()?.len();
            if len < self.offset {
                let named = self.format.named(path);
                *self = Self {
                    identity: named,
                    ..Self::new(self.format, self.pid, self.start)
                };
            }
            let mut reader = std::io::BufReader::new(&mut file);
            if self.identity.is_none() {
                let mut first = Vec::new();
                reader
                    .by_ref()
                    .take(TAIL)
                    .read_until(b'\n', &mut first)
                    .ok()?;
                if first.last() != Some(&b'\n') {
                    return None;
                }
                self.identity = Some(self.format.header(&first)?);
                self.offset = first.len() as u64;
            }
            if len.saturating_sub(self.offset) > TAIL {
                self.offset = len - TAIL;
                reader.seek(SeekFrom::Start(self.offset)).ok()?;
                let mut partial = Vec::new();
                reader.read_until(b'\n', &mut partial).ok()?;
                self.offset += partial.len() as u64;
            }
            reader.seek(SeekFrom::Start(self.offset)).ok()?;
            let mut line = Vec::new();
            loop {
                line.clear();
                match reader.read_until(b'\n', &mut line) {
                    Ok(n) if n > 0 && line.last() == Some(&b'\n') => {
                        self.offset += n as u64;
                        if let Some(status) = self.format.status(&line) {
                            self.status = status;
                        }
                    }
                    _ => break,
                }
            }
        }
        if self.ignored || self.reported == Some(self.status) {
            return None;
        }
        let (native, cwd) = self.identity.clone()?;
        let cwd = match cwd {
            Some(cwd) => cwd,
            None => tower_pal::inspect_process(self.pid)
                .ok()?
                .cwd
                .to_string_lossy()
                .into_owned(),
        };
        self.reported = Some(self.status);
        Some(service::NativeSession {
            harness,
            pid: self.pid,
            start: self.start,
            native,
            cwd,
            status: self.status,
            display_name: None,
        })
    }
}

/// Follows OpenCode's session database. Releases with a `session_v2` table
/// record when each session turns busy or idle; older ones only record that a
/// session changed, so their status is unknown.
async fn opencode_sessions(service: ServiceWorker, database: PathBuf) {
    let Some(dir) = database.parent().map(Path::to_path_buf) else {
        return;
    };
    if !appear(&dir, "OpenCode sessions").await {
        return;
    }
    let mut wal = database.clone().into_os_string();
    wal.push("-wal");
    let mut watch = match tower_pal::FileWatch::new(&[database.clone(), wal.into()]) {
        Ok(watch) => watch,
        Err(e) => {
            eprintln!("tower: OpenCode sessions are unobservable: {e}");
            return;
        }
    };
    let mut since = 0;
    // Sessions bound to the live process that runs them, with the last report.
    let mut bound: BTreeMap<String, (u32, u64, service::NativeSession)> = BTreeMap::new();
    loop {
        let path = database.clone();
        let rows = tokio::task::spawn_blocking(move || opencode_rows(&path, since))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        bound.retain(|_, (pid, start, _)| {
            tower_pal::process_identity(*pid).is_ok_and(|p| p.start_time == *start)
        });
        for row in rows {
            since = since.max(row.changed);
            let writer = match bound.get(&row.id) {
                Some((pid, start, _)) => Some((*pid, *start)),
                None => {
                    let cwd = std::fs::canonicalize(&row.directory)
                        .unwrap_or_else(|_| PathBuf::from(&row.directory));
                    process_in("opencode", &cwd, |c| {
                        bound.values().any(|(pid, start, _)| (*pid, *start) == c)
                    })
                }
            };
            let Some((pid, start)) = writer else { continue };
            let found = service::NativeSession {
                harness: "opencode",
                pid,
                start,
                native: row.id.clone(),
                cwd: row.directory,
                status: row.status,
                display_name: row.title.filter(|t| !t.is_empty()),
            };
            if bound.get(&row.id).is_some_and(|(.., last)| *last == found) {
                continue;
            }
            let report = found.clone();
            if matches!(
                service
                    .execute(move |s| s.observe_native_session(report))
                    .await,
                Ok(Ok(true))
            ) {
                bound.retain(|_, (p, s, _)| (*p, *s) != (pid, start));
                bound.insert(row.id, (pid, start, found));
            }
        }
        watch.changed().await;
    }
}

struct OpencodeRow {
    id: String,
    directory: String,
    title: Option<String>,
    changed: i64,
    status: &'static str,
}

/// The most recently changed top-level session in each directory, among those
/// changed after `since` (milliseconds).
fn opencode_rows(database: &Path, since: i64) -> rusqlite::Result<Vec<OpencodeRow>> {
    let db = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.busy_timeout(Duration::from_secs(1))?;
    let tracked: bool = db.query_row(
        "SELECT count(*) = 2 FROM pragma_table_info('session_v2') WHERE name IN ('time_busy', 'time_idle')",
        [],
        |r| r.get(0),
    )?;
    let query = if tracked {
        "SELECT id, directory, title, max(max(time_updated, coalesce(time_busy, 0), coalesce(time_idle, 0))) AS changed, time_busy, time_idle
         FROM session_v2
         WHERE parent_id IS NULL AND time_archived IS NULL
           AND max(time_updated, coalesce(time_busy, 0), coalesce(time_idle, 0)) > ?1
         GROUP BY directory"
    } else {
        "SELECT id, directory, title, max(time_updated) AS changed, NULL, NULL
         FROM session
         WHERE parent_id IS NULL AND time_archived IS NULL AND time_updated > ?1
         GROUP BY directory"
    };
    let mut statement = db.prepare(query)?;
    let rows = statement.query_map([since], |r| {
        let busy: Option<i64> = r.get(4)?;
        let idle: Option<i64> = r.get(5)?;
        Ok(OpencodeRow {
            id: r.get(0)?,
            directory: r.get(1)?,
            title: r.get(2)?,
            changed: r.get(3)?,
            status: match (tracked, busy, idle) {
                (false, ..) => "unknown",
                (true, Some(busy), idle) if idle.is_none_or(|idle| busy > idle) => "working",
                (true, ..) => "idle",
            },
        })
    })?;
    rows.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transcript_lines_yield_identity_and_turn_status() {
        let codex = Format::Codex;
        let meta = br#"{"type":"session_meta","payload":{"id":"t1","cwd":"/w","source":"cli"}}"#;
        assert_eq!(codex.header(meta), Some(("t1".into(), Some("/w".into()))));
        let child = br#"{"type":"session_meta","payload":{"id":"t2","cwd":"/w","source":{"subagent":"review"}}}"#;
        assert_eq!(codex.header(child), None);
        let event = |kind: &str| format!(r#"{{"type":"event_msg","payload":{{"type":"{kind}"}}}}"#);
        assert_eq!(
            codex.status(event("task_started").as_bytes()),
            Some("working")
        );
        assert_eq!(
            codex.status(event("task_complete").as_bytes()),
            Some("idle")
        );
        assert_eq!(codex.status(event("turn_aborted").as_bytes()), Some("idle"));
        assert_eq!(codex.status(event("token_count").as_bytes()), None);
        let item = br#"{"type":"response_item","payload":{"text":"\"type\":\"task_started\""}}"#;
        assert_eq!(codex.status(item), None);

        let pi = Format::Pi;
        let header = br#"{"type":"session","version":3,"id":"p1","cwd":"/w"}"#;
        assert_eq!(pi.header(header), Some(("p1".into(), Some("/w".into()))));
        let message = |role: &str, stop: &str| {
            format!(r#"{{"type":"message","message":{{"role":"{role}","stopReason":"{stop}"}}}}"#)
        };
        assert_eq!(pi.status(message("user", "").as_bytes()), Some("working"));
        assert_eq!(
            pi.status(message("assistant", "toolUse").as_bytes()),
            Some("working")
        );
        assert_eq!(
            pi.status(message("assistant", "stop").as_bytes()),
            Some("idle")
        );
        assert_eq!(
            pi.status(message("assistant", "aborted").as_bytes()),
            Some("idle")
        );

        let grok = Format::Grok;
        let event = |kind: &str| format!(r#"{{"ts":"t","type":"{kind}"}}"#);
        assert_eq!(
            grok.status(event("turn_started").as_bytes()),
            Some("working")
        );
        assert_eq!(
            grok.status(event("permission_requested").as_bytes()),
            Some("waiting_approval")
        );
        assert_eq!(
            grok.status(event("permission_resolved").as_bytes()),
            Some("working")
        );
        assert_eq!(grok.status(event("turn_ended").as_bytes()), Some("idle"));
        assert_eq!(grok.status(event("phase_changed").as_bytes()), None);
    }
    #[test]
    fn layouts_map_files_to_sessions() {
        let root = Path::new("/h/sessions");
        let grok = Format::Grok;
        let lock = root.join("%2Fw%2Fa b/01ab/chat_history.jsonl.lock");
        let events = root.join("%2Fw%2Fa b/01ab/events.jsonl");
        assert_eq!(grok.tracked(root, &lock), Some(events.clone()));
        assert_eq!(
            grok.tracked(root, &root.join("%2Fw/prompt_history.jsonl")),
            None
        );
        assert_eq!(
            grok.named(&events),
            Some(("01ab".into(), Some("/w/a b".into())))
        );
        let reasonix = Format::Reasonix;
        let root = Path::new("/h/projects");
        let lease = root.join("-w/sessions/20261112-1.2-m.jsonl.lease.lock");
        assert_eq!(reasonix.tracked(root, &lease), Some(lease.clone()));
        assert_eq!(
            reasonix.named(&lease),
            Some(("20261112-1.2-m".into(), None))
        );
        assert_eq!(
            reasonix.tracked(root, &root.join("-w/sessions/x.jsonl")),
            None
        );
        let pi = Format::Pi;
        let root = Path::new("/h/agent/sessions");
        assert!(pi.tracked(root, &root.join("--w--/t_p1.jsonl")).is_some());
        assert!(
            pi.tracked(root, &root.join("--w--/sub/t_p1.jsonl"))
                .is_none()
        );
    }
}
