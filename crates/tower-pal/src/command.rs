use std::{collections::BTreeMap, io, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
};

pub type Environment = BTreeMap<String, String>;
pub fn spawn_service(argv: &[String], env: &Environment, cwd: &Path) -> io::Result<Child> {
    command(argv, env, cwd)?
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
}
pub fn spawn_protocol(argv: &[String], env: &Environment, cwd: &Path) -> io::Result<Child> {
    command(argv, env, cwd)?
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
}
pub fn spawn_protocol_child(argv: &[String], env: &Environment, cwd: &Path) -> io::Result<Child> {
    command(argv, env, cwd)?
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
}
pub fn kill_protocol_group(identity: crate::ProcessIdentity) -> io::Result<()> {
    let current = crate::process_incarnation(identity.pid)?;
    if current.start_time != identity.start_time {
        return Err(io::Error::other("process incarnation changed"));
    }
    nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(identity.pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .map_err(io::Error::from)
}
pub fn protocol_group_terminated(identity: crate::ProcessIdentity) -> io::Result<bool> {
    let group = nix::unistd::Pid::from_raw(identity.pid as i32);
    match nix::sys::signal::killpg(group, None) {
        Err(nix::errno::Errno::ESRCH) => Ok(true),
        Err(e) => Err(io::Error::from(e)),
        Ok(()) => Ok(false),
    }
}
pub fn environment() -> Environment {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect()
}
pub fn find_program(program: &str, env: &Environment) -> Option<std::path::PathBuf> {
    let p = Path::new(program);
    if p.is_absolute() {
        return is_executable(p).then(|| p.to_path_buf());
    }
    env.get("PATH")
        .into_iter()
        .flat_map(|p| std::env::split_paths(p))
        .filter(|p| p.is_absolute())
        .map(|p| p.join(program))
        .find(|p| is_executable(p))
}
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}
fn command(argv: &[String], env: &Environment, cwd: &Path) -> io::Result<Command> {
    let name = argv
        .first()
        .ok_or_else(|| io::Error::other("empty command"))?;
    let program = find_program(name, env).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "agent executable not found on PATH",
        )
    })?;
    let mut cmd = Command::new(program);
    cmd.args(&argv[1..])
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .kill_on_drop(true);
    Ok(cmd)
}
pub fn spawn_inherited(argv: &[String], env: &Environment, cwd: &Path) -> io::Result<Child> {
    command(argv, env, cwd)?
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
}
struct CommandGroup {
    pid: u32,
    armed: bool,
}
impl CommandGroup {
    fn kill(&mut self) {
        if self.armed {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(self.pid as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
            self.armed = false;
        }
    }
}
impl Drop for CommandGroup {
    fn drop(&mut self) {
        self.kill();
    }
}
pub async fn capture(
    argv: &[String],
    env: &Environment,
    cwd: &Path,
    timeout: Duration,
    limit: usize,
) -> io::Result<(i32, Vec<u8>)> {
    let mut child = command(argv, env, cwd)?
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("missing child PID"))?;
    let mut group = CommandGroup { pid, armed: true };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("missing stdout"))?;
    let result = tokio::time::timeout(timeout, async {
        let mut bytes = Vec::new();
        stdout
            .take((limit + 1) as u64)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > limit {
            return Err(io::Error::other("command output exceeds limit"));
        }
        let status = child.wait().await?;
        group.armed = false;
        Ok((status.code().unwrap_or(128), bytes))
    })
    .await;
    // A timed-out helper must not leave its shell or grandchildren running.
    if child.id().is_some() {
        group.kill();
    }
    let _ = child.wait().await;
    result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "command timed out"))?
}

pub fn is_group_leader() -> bool {
    nix::unistd::getpgrp() == nix::unistd::getpid()
}
pub fn terminate_own_group() -> io::Result<()> {
    if !is_group_leader() {
        return Err(io::Error::other("launcher must own its process group"));
    }
    nix::sys::signal::killpg(nix::unistd::getpid(), nix::sys::signal::Signal::SIGTERM)
        .map_err(io::Error::from)
}
/// SIGTERM requests for a supervisor. Creating it also stops Ctrl-C, which
/// belongs to the foreground agent, from terminating the supervisor.
pub struct Termination {
    term: tokio::signal::unix::Signal,
    _interrupt: tokio::signal::unix::Signal,
}
impl Termination {
    pub fn new() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            _interrupt: signal(SignalKind::interrupt())?,
        })
    }
    pub async fn recv(&mut self) {
        if self.term.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    }
    /// Forward each request into a channel for code that only selects on receivers.
    pub fn into_channel(mut self) -> tokio::sync::mpsc::Receiver<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                self.recv().await;
                if tx.send(()).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}
pub fn symlink(source: &Path, destination: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(source, destination)
}
pub fn ensure_user_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}
pub fn cache_home(env: &Environment) -> io::Result<std::path::PathBuf> {
    let home = env
        .get("HOME")
        .ok_or_else(|| io::Error::other("HOME is required"))?;
    #[cfg(target_os = "macos")]
    let path = Path::new(home).join("Library/Caches");
    #[cfg(not(target_os = "macos"))]
    let path = env
        .get("XDG_CACHE_HOME")
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| Path::new(home).join(".cache"));
    Ok(path)
}
pub fn read_stdin_bounded(limit: usize, deadline: Duration) -> io::Result<Vec<u8>> {
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::{io::Read, os::fd::AsFd, time::Instant};
    let stdin = std::io::stdin();
    let mut input = std::fs::File::from(nix::unistd::dup(stdin.as_fd())?);
    let until = Instant::now() + deadline;
    let mut bytes = Vec::new();
    loop {
        let remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "stdin timed out"));
        }
        let mut fds = [PollFd::new(input.as_fd(), PollFlags::POLLIN)];
        if poll(
            &mut fds,
            PollTimeout::try_from(remaining).unwrap_or(PollTimeout::MAX),
        )? == 0
        {
            continue;
        }
        let mut chunk = [0; 8192];
        let n = input.read(&mut chunk)?;
        if n == 0 {
            return Ok(bytes);
        }
        if bytes.len() + n > limit {
            return Err(io::Error::other("stdin exceeds limit"));
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
}
#[cfg(target_os = "linux")]
pub fn boot_id() -> io::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned())
}
#[cfg(target_os = "macos")]
pub fn boot_id() -> io::Result<String> {
    let out = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.boottime"])
        .output()?;
    if !out.status.success() {
        return Err(io::Error::other("cannot read boot identity"));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

pub fn temporary_dir_in(path: &Path) -> io::Result<tempfile::TempDir> {
    crate::private_dir(path)?;
    tempfile::Builder::new().prefix("run-").tempdir_in(path)
}

pub async fn command_input(
    argv: &[String],
    env: &Environment,
    input: &[u8],
    timeout: Duration,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = command(argv, env, Path::new("/"))?
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("Missing command PID"))?;
    let mut group = CommandGroup { pid, armed: true };
    let result = tokio::time::timeout(timeout, async {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("Missing command input"))?;
        stdin.write_all(input).await?;
        stdin.shutdown().await?;
        drop(stdin);
        let status = child.wait().await?;
        group.armed = false;
        if !status.success() {
            return Err(io::Error::other("Event command failed"));
        }
        Ok(())
    })
    .await;
    if child.id().is_some() {
        group.kill();
        let _ = child.wait().await;
    }
    result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Event command timed out"))?
}

pub fn monotonic_ms() -> u64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    (now.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add(now.tv_nsec as u64 / 1_000_000)
}
