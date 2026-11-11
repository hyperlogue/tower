//! OS operations live here. The rest of tower uses these platform-neutral names.

use std::{
    env, io,
    path::{Path, PathBuf},
};
#[cfg(unix)]
mod program;
pub use program::Program;
mod unix;
#[cfg(unix)]
pub use unix::*;
mod watch;
pub use watch::{FileWatch, PathWatch, TreeWatch};
mod metrics;
pub use metrics::{HostSample, HostSampler};
mod command;
mod process;
pub use command::*;
pub use process::*;
mod services;
pub use services::*;
#[cfg(all(unix, feature = "terminal"))]
#[allow(unsafe_code)]
mod pty;
#[cfg(all(unix, feature = "terminal"))]
pub use pty::*;
#[cfg(not(unix))]
compile_error!("tower currently supports Linux and macOS");

#[derive(Clone, Debug)]
pub struct Paths {
    pub state: PathBuf,
    pub run: PathBuf,
    pub socket: PathBuf,
    pub database: PathBuf,
    pub tokens: PathBuf,
    pub logs: PathBuf,
    pub config_home: PathBuf,
    pub home: PathBuf,
}
impl Paths {
    pub fn pool_socket(&self) -> PathBuf {
        self.run.join("pool.sock")
    }
    pub fn discover() -> io::Result<Self> {
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "HOME must be an absolute path")
            })?;
        let xdg = |key: &str, fallback: PathBuf| {
            env::var_os(key)
                .map(PathBuf::from)
                .filter(|p| p.is_absolute())
                .unwrap_or(fallback)
        };
        let state = xdg("XDG_STATE_HOME", home.join(".local/state")).join("tower");
        let config_home = xdg("XDG_CONFIG_HOME", home.join(".config"));
        let run = state.join("run");
        let socket = env::var_os("TOWER_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| run.join("towerd.sock"));
        if !socket.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TOWER_SOCKET must be absolute",
            ));
        }
        Ok(Self {
            database: state.join("tower.db"),
            tokens: state.join("tokens"),
            logs: state.join("logs"),
            state,
            run,
            socket,
            config_home,
            home,
        })
    }
    pub fn prepare(&self) -> io::Result<()> {
        for dir in [&self.state, &self.run, &self.tokens, &self.logs] {
            private_dir(dir)?;
        }
        let parent = self
            .socket
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket has no parent"))?;
        if parent != self.run {
            require_private_dir(parent)?;
        }
        Ok(())
    }
    pub fn token_path(&self, id: &str) -> io::Result<PathBuf> {
        if id.is_empty()
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid session ID for token file",
            ));
        }
        Ok(self.tokens.join(id))
    }
    pub fn display_cwd(&self, input: &str) -> io::Result<String> {
        let normalized = self.absolute_cwd(input)?;
        Ok(match normalized.strip_prefix(&self.home) {
            Ok(rest) if rest.as_os_str().is_empty() => "~".into(),
            Ok(rest) => format!("~/{}", rest.display()),
            Err(_) => normalized.to_string_lossy().into_owned(),
        })
    }
    pub fn absolute_cwd(&self, input: &str) -> io::Result<PathBuf> {
        let path = if input == "~" {
            self.home.clone()
        } else if let Some(rest) = input.strip_prefix("~/") {
            self.home.join(rest)
        } else {
            PathBuf::from(input)
        };
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cwd must be absolute or start with ~/",
            ));
        }
        // Lexical normalization also works when a registered process's cwd was deleted.
        let mut normalized = PathBuf::new();
        for part in path.components() {
            match part {
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                std::path::Component::CurDir => {}
                _ => normalized.push(part.as_os_str()),
            }
        }
        Ok(normalized)
    }
}

pub fn cwd() -> io::Result<PathBuf> {
    env::current_dir()
}
pub fn executable() -> io::Result<PathBuf> {
    env::current_exe()
}
pub fn pid() -> u32 {
    std::process::id()
}
pub fn path_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| prefix.ends_with('/') || tail.starts_with('/'))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub parent: u32,
    pub start_time: u64,
}

pub fn read_private(path: &Path) -> io::Result<Vec<u8>> {
    read_private_bounded(path, 64 * 1024)
}

pub fn read_private_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let read_limit = u64::try_from(limit)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid private file limit"))?;
    let file = private_file(path, false)?;
    if file.metadata()?.len() >= read_limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private file is too large",
        ));
    }
    let mut bytes = Vec::new();
    file.take(read_limit).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private file is too large",
        ));
    }
    Ok(bytes)
}

/// Bounded configuration input; absence is distinct from invalid contents.
pub fn read_config(path: &Path) -> io::Result<Option<String>> {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut text = String::new();
    file.take(65537).read_to_string(&mut text)?;
    if text.len() > 65536 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "configuration is too large",
        ));
    }
    Ok(Some(text))
}

pub fn operating_system() -> &'static str {
    std::env::consts::OS
}
