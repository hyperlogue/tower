//! Sessions an embedding program runs in its own terminal windows. Each
//! launcher owns its session's state file; any process of the program may
//! list, watch and stop sessions through the files below the state root.

use crate::launcher;
use std::{
    io,
    path::{Path, PathBuf},
};
use tower_agents::runtime::{AgentSpec, Embedding, LauncherPlan, RuntimeState};
use tower_pal::Paths;

pub struct Local {
    embedding: Embedding,
    paths: Paths,
}

/// One launch of a session, as its launcher last recorded it.
#[derive(Clone, Debug)]
pub struct Session {
    pub plan: LauncherPlan,
    pub state: RuntimeState,
    /// The launcher that wrote `state` is still running.
    pub live: bool,
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
        let generation = random(8)?;
        let root = self.sessions_dir().join(id).join(&generation);
        let run = self.paths.run.join("agents");
        for dir in [&root, &run, &self.paths.tokens] {
            tower_pal::private_dir(dir)?;
        }
        tower_pal::write_private(&token_path, random(32)?.as_bytes())?;
        let plan = LauncherPlan {
            id: id.into(),
            generation: generation.clone(),
            state_dir: root.to_string_lossy().into_owned(),
            tower_socket: String::new(),
            control_socket: run
                .join(format!("{generation}.sock"))
                .to_string_lossy()
                .into_owned(),
            spec,
            continue_turn: false,
            embedding: Some(self.embedding.clone()),
        };
        let path = root.join("plan.json");
        tower_pal::write_private(&path, &serde_json::to_vec(&plan)?)?;
        Ok(path)
    }

    /// Every recorded launch, including finished ones that [`forget`] has
    /// not removed.
    ///
    /// [`forget`]: Self::forget
    pub fn sessions(&self) -> io::Result<Vec<Session>> {
        let boot = tower_pal::boot_id()?;
        let mut found = Vec::new();
        for id in entries(&self.sessions_dir())? {
            for generation in entries(&id)? {
                let (Ok(plan), Ok(state)) = (
                    tower_pal::read_private(&generation.join("plan.json")),
                    launcher::read_state(&generation.join("state.json")),
                ) else {
                    continue;
                };
                let Ok(plan) = serde_json::from_slice::<LauncherPlan>(&plan) else {
                    continue;
                };
                if state.id != plan.id || state.generation != plan.generation {
                    continue;
                }
                let live = state.exit_code.is_none()
                    && state.boot_id == boot
                    && tower_pal::process_identity(state.launcher_pid)
                        .is_ok_and(|p| p.start_time == state.launcher_start_time);
                found.push(Session { plan, state, live });
            }
        }
        Ok(found)
    }

    /// Signals whenever a launcher records new state.
    pub fn watch(&self) -> io::Result<tower_pal::TreeWatch> {
        let dir = self.sessions_dir();
        tower_pal::private_dir(&dir)?;
        tower_pal::TreeWatch::new(&dir, "state.json")
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
        launcher::stop(&session.plan, &token)
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
