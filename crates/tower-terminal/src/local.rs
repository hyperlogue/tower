//! Sessions an embedding program runs in its own terminal windows. Each
//! launcher owns its session's state file; any process of the program may
//! list, watch and stop sessions through the files below the state root.

use crate::launcher;
use std::{
    io,
    path::{Path, PathBuf},
};
use tower_agents::runtime as contract;
use tower_agents::{
    Harness,
    inventory::Candidate,
    runtime::{AgentSpec, Embedding, LauncherPlan, RuntimeState},
};
use tower_pal::Paths;
use tower_protocol::{AgentRecord, Capabilities};

pub struct Local {
    embedding: Embedding,
    paths: Paths,
}

/// One launch of a session, as its launcher last recorded it.
#[derive(Clone, Debug)]
pub struct Session {
    pub plan: LauncherPlan,
    pub state: RuntimeState,
    /// The launcher that wrote `state`, or its agent, is still running.
    pub live: bool,
}

/// What a user attached to a session, kept beside its launches.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Notes {
    #[serde(default)]
    tags: std::collections::BTreeSet<String>,
    /// The launch and `seq` up to which attention was acknowledged.
    #[serde(default)]
    acknowledged: Option<(String, u64)>,
}

impl Session {
    /// Its launcher ended without recording an exit.
    fn lost(&self) -> bool {
        !self.live && self.state.exit_code.is_none()
    }
    /// The `seq` at which the session began to need its user. A launcher
    /// cannot record its own loss, so that one is decided here.
    fn attention(&self) -> Option<u64> {
        if !self.lost() {
            return self.state.attention;
        }
        match tower_sessions::attention::on_status(
            &self.state.status,
            "lost",
            self.state.stop_requested,
        ) {
            Some(true) => Some(self.state.seq.saturating_add(1)),
            Some(false) => None,
            None => self.state.attention,
        }
    }
}

fn random(bytes: usize) -> io::Result<String> {
    let mut buffer = vec![0; bytes];
    getrandom::fill(&mut buffer).map_err(|_| io::Error::other("random source unavailable"))?;
    Ok(buffer.iter().map(|b| format!("{b:02x}")).collect())
}

impl Local {
    pub fn new(embedding: Embedding) -> io::Result<Self> {
        if !Path::new(&embedding.helper).is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "helper executable must be absolute",
            ));
        }
        let paths = Paths::rooted(PathBuf::from(&embedding.state))?;
        Ok(Self { embedding, paths })
    }
    fn sessions_dir(&self) -> PathBuf {
        self.paths.state.join("sessions")
    }

    /// Records a new launch of `id` and returns the plan file for
    /// [`run`]. A resumed session keeps its `id` and gets a new generation.
    pub fn prepare(&self, id: &str, spec: AgentSpec) -> io::Result<PathBuf> {
        let token_path = self.paths.token_path(id)?;
        tower_agents::config::validate(&spec)?;
        if self.sessions()?.iter().any(|s| s.plan.id == id && s.live) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "session is already running",
            ));
        }
        let plan = contract::plan(&self.paths, id, spec, Some(self.embedding.clone()))?;
        tower_pal::private_dir(&self.paths.tokens)?;
        tower_pal::write_private(&token_path, random(32)?.as_bytes())?;
        let path = Path::new(&plan.state_dir).join("plan.json");
        tower_pal::write_private(&path, &serde_json::to_vec(&plan)?)?;
        Ok(path)
    }

    /// Finds a conversation the agent already holds, as the agent's own
    /// history or the configured Codex app-server records it. Codex in
    /// app-server mode is asked over its socket: call this off an async
    /// runtime.
    pub fn conversation(
        &self,
        harness: Harness,
        mode: Option<&str>,
        native_id: &str,
    ) -> io::Result<Candidate> {
        if harness != Harness::Codex || mode == Some("native") {
            return tower_agents::inventory::Inventory::default().resolve(
                &tower_pal::environment(),
                harness,
                native_id,
            );
        }
        if !tower_agents::events::native_id(native_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native session identity",
            ));
        }
        let config = tower_agents::codex_service::Config::endpoint(
            &self.paths,
            self.embedding.codex_endpoint.as_deref(),
        )
        .map_err(io::Error::other)?;
        contract::codex_block(contract::codex_thread(&self.paths, config, native_id))
            .map_err(|error| io::Error::other(error.message))
    }

    /// Records a launch of `id` that continues `conversation` where the agent
    /// left it, in the directory the conversation ran in.
    pub fn prepare_conversation(
        &self,
        id: &str,
        conversation: Candidate,
        mode: Option<String>,
        args: Vec<String>,
    ) -> io::Result<PathBuf> {
        self.prepare(
            id,
            AgentSpec {
                harness: conversation.harness,
                cwd: conversation.cwd,
                args,
                mode,
                resume: Some(conversation.native),
                fork: false,
                native_home: conversation.namespace,
            },
        )
    }

    /// Records a new launch of the ended session `id` that continues its
    /// conversation with the options it was started with.
    pub fn prepare_restart(&self, id: &str) -> io::Result<PathBuf> {
        let unavailable = |reason| io::Error::new(io::ErrorKind::NotFound, reason);
        let last = self
            .sessions()?
            .into_iter()
            .filter(|s| s.plan.id == id)
            .max_by_key(|s| s.state.updated_at)
            .ok_or_else(|| unavailable("session is not recorded"))?;
        let mut spec = contract::continued(last.plan.spec, last.state).map_err(unavailable)?;
        spec.args = tower_agents::config::resume_options(spec.harness, &spec.args)?;
        spec.fork = false;
        self.prepare(id, spec)
    }

    /// Every recorded launch, including finished ones that [`forget`] has
    /// not removed.
    ///
    /// [`forget`]: Self::forget
    pub fn sessions(&self) -> io::Result<Vec<Session>> {
        let mut found = Vec::new();
        for id in entries(&self.sessions_dir())? {
            for generation in entries(&id)? {
                let (Ok(plan), Ok(state)) = (
                    tower_pal::read_private(&generation.join("plan.json")),
                    contract::read_state(&generation.join("state.json")),
                ) else {
                    continue;
                };
                let Ok(plan) = serde_json::from_slice::<LauncherPlan>(&plan) else {
                    continue;
                };
                if state.id != plan.id || state.generation != plan.generation {
                    continue;
                }
                let live =
                    state.exit_code.is_none() && contract::launcher_alive(&state).unwrap_or(false);
                found.push(Session { plan, state, live });
            }
        }
        Ok(found)
    }

    /// Signals whenever a launcher records new state or a session's tags or
    /// acknowledgement change.
    pub fn watch(&self) -> io::Result<tower_pal::TreeWatch> {
        let dir = self.sessions_dir();
        tower_pal::private_dir(&dir)?;
        tower_pal::TreeWatch::new(&dir, None)
    }

    fn notes(&self, id: &str) -> io::Result<Notes> {
        match tower_pal::read_private(&self.sessions_dir().join(id).join("notes.json")) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Notes::default()),
            Err(e) => Err(e),
        }
    }

    /// Each session as the daemon's registry would list it, from its latest
    /// launch. `needs_attention` holds until [`patch_tags`] acknowledges it.
    ///
    /// [`patch_tags`]: Self::patch_tags
    pub fn records(&self) -> io::Result<Vec<AgentRecord>> {
        let mut latest = std::collections::BTreeMap::<String, Session>::new();
        for session in self.sessions()? {
            let newer = latest.get(&session.plan.id).is_none_or(|known| {
                (session.live, session.state.updated_at) > (known.live, known.state.updated_at)
            });
            if newer {
                latest.insert(session.plan.id.clone(), session);
            }
        }
        latest
            .into_values()
            .map(|session| self.record(session))
            .collect()
    }
    fn record(&self, session: Session) -> io::Result<AgentRecord> {
        let (lost, attention) = (session.lost(), session.attention());
        let Session { plan, state, live } = session;
        let notes = self.notes(&plan.id)?;
        let resumable = state.native.is_some()
            && state.private.get("native_resumable") != Some(&serde_json::Value::Bool(false));
        let acknowledged = notes
            .acknowledged
            .filter(|(generation, _)| *generation == state.generation)
            .map(|(_, seq)| seq);
        Ok(AgentRecord {
            display_name: state.display_name,
            node: "local".into(),
            harness: state.harness.id().into(),
            mode: plan.spec.mode.clone(),
            ownership: "owned".into(),
            protocol: Some("terminal".into()),
            trust: "strong".into(),
            native_session_id: state.native.map(|n| n.id),
            pid: state.agent_pid,
            pid_start_time: state.agent_start_time,
            shared_process: plan.spec.uses_app_server(),
            cwd: self.paths.display_cwd(&state.cwd).unwrap_or(state.cwd),
            checkout_id: None,
            status: if live {
                state.status
            } else if lost {
                "lost".into()
            } else {
                "exited".into()
            },
            status_detail: Some(
                serde_json::from_value(serde_json::to_value(&state.detail)?)
                    .map_err(io::Error::other)?,
            ),
            caps: Capabilities {
                kill: live,
                resume: resumable,
                ..Capabilities::unavailable("embedded")
            },
            pool_session: None,
            attach_count: 0,
            sources: vec!["launcher".into()],
            registered_at: std::fs::metadata(self.sessions_dir().join(&plan.id))
                .and_then(|meta| meta.created())
                .ok()
                .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(state.updated_at, |at| at.as_millis() as u64),
            updated_at: state.updated_at,
            last_seen: state.updated_at,
            revision: state.seq,
            labels: Default::default(),
            tags: notes.tags,
            needs_attention: attention
                .is_some_and(|since| acknowledged.is_none_or(|seq| since > seq)),
            task_refs: Vec::new(),
            sync_state: "synced".into(),
            id: plan.id,
        })
    }

    /// Adds and removes tags on a session, and with `clear_attention`
    /// acknowledges what its latest launch has reported so far.
    pub fn patch_tags(
        &self,
        id: &str,
        add: &[String],
        remove: &[String],
        clear_attention: bool,
    ) -> io::Result<AgentRecord> {
        let invalid = |reason| io::Error::new(io::ErrorKind::InvalidInput, reason);
        // Validates `id` before it names a directory.
        self.paths.token_path(id)?;
        let session = self
            .sessions()?
            .into_iter()
            .filter(|s| s.plan.id == id)
            .max_by_key(|s| (s.live, s.state.updated_at))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "session is not recorded"))?;
        let dir = self.sessions_dir().join(id);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let _lock = loop {
            match tower_pal::SingletonLock::acquire(&dir.join("notes.lock")) {
                Ok(lock) => break lock,
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < until =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                Err(e) => return Err(e),
            }
        };
        let mut notes = self.notes(id)?;
        tower_sessions::tags::patch(&mut notes.tags, add, remove).map_err(invalid)?;
        if clear_attention {
            notes.acknowledged = Some((
                session.state.generation.clone(),
                session.attention().unwrap_or(0).max(session.state.seq),
            ));
        }
        tower_pal::write_private(&dir.join("notes.json"), &serde_json::to_vec(&notes)?)?;
        self.record(session)
    }

    /// Asks the running launcher of `id` to clean up and end its agent. This
    /// blocks for the launcher's reply; call it off an async runtime.
    pub fn stop(&self, id: &str) -> io::Result<()> {
        let token = tower_pal::read_private(&self.paths.token_path(id)?)?;
        let token = String::from_utf8(token).map_err(io::Error::other)?;
        let session = self
            .sessions()?
            .into_iter()
            .find(|s| s.plan.id == id && s.live)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "session is not running"))?;
        contract::stop(&session.plan, &token)
    }

    /// Removes the records of a session that is no longer running.
    pub fn forget(&self, id: &str) -> io::Result<()> {
        let token = self.paths.token_path(id)?;
        if self.sessions()?.iter().any(|s| s.plan.id == id && s.live) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "session is still running",
            ));
        }
        for result in [
            std::fs::remove_dir_all(self.sessions_dir().join(id)),
            std::fs::remove_file(token),
        ] {
            match result {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}

fn entries(dir: &Path) -> io::Result<Vec<PathBuf>> {
    match std::fs::read_dir(dir) {
        Ok(found) => found.map(|e| e.map(|e| e.path())).collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Supervises the agent of a prepared plan in this process's terminal until
/// it exits. The process must lead its own process group.
pub async fn run(plan: &Path) -> io::Result<i32> {
    launcher::run(plan).await
}

/// Answers the commands launchers and agents send to the helper executable.
/// `args` excludes the program name; `None` means the command is not one of
/// them.
pub fn helper(args: &[String]) -> Option<io::Result<()>> {
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["hook", harness, event] => {
            launcher::hook(harness, event);
            Some(Ok(()))
        }
        ["__agent", "env"] => Some(
            serde_json::to_string(&tower_pal::environment())
                .map(|env| println!("{env}"))
                .map_err(io::Error::from),
        ),
        _ => None,
    }
}
