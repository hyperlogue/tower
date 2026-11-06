use super::{Paths, ProcessIdentity};
use fs2::FileExt;
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::net::{UnixListener, UnixStream};

pub type IpcStream = UnixStream;
pub fn uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
pub fn file_identity(path: &Path) -> io::Result<(u64, u64)> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Expected a regular file or directory",
        ));
    }
    Ok((metadata.dev(), metadata.ino()))
}
fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
pub fn open_regular(path: &Path) -> io::Result<File> {
    if !fs::metadata(path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    Ok(file)
}

pub fn require_private_dir(path: &Path) -> io::Result<()> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_dir() || m.uid() != uid() || m.permissions().mode() & 0o077 != 0 {
        return Err(denied(
            "tower requires a private directory owned by the current user",
        ));
    }
    Ok(())
}
pub fn private_dir(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) if !m.is_dir() || m.uid() != uid() => {
            return Err(denied("invalid state directory owner or type"));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            DirBuilder::new().recursive(true).mode(0o700).create(path)?;
        }
        Err(e) => return Err(e),
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    require_private_dir(path)
}
pub fn private_file(path: &Path, create: bool) -> io::Result<File> {
    match fs::symlink_metadata(path) {
        Ok(m) if !m.is_file() || m.uid() != uid() => {
            return Err(denied("invalid private file owner or type"));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound && create => {}
        Err(e) => return Err(e),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.uid() != uid() {
        return Err(denied("invalid private file owner or type"));
    }
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}
pub fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| denied("invalid private file path"))?;
    require_private_dir(parent)?;
    write_owned_config(path, bytes)
}

pub fn write_owned_config(path: &Path, bytes: &[u8]) -> io::Result<()> {
    write_owned(path, bytes, true)
}
pub fn write_new_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    write_owned(path, bytes, false)
}
fn write_owned(path: &Path, bytes: &[u8], replace: bool) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| denied("invalid configuration path"))?;
    let directory = fs::metadata(parent)?;
    if !directory.is_dir()
        || directory.uid() != uid()
        || directory.permissions().mode() & 0o022 != 0
    {
        return Err(denied(
            "configuration directory must be owned and not writable by others",
        ));
    }
    if let Ok(m) = fs::symlink_metadata(path)
        && (!m.is_file() || m.uid() != uid())
    {
        return Err(denied("invalid private file owner or type"));
    }
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    if replace {
        temp.persist(path).map_err(|e| e.error)?;
    } else {
        temp.persist_noclobber(path).map_err(|e| e.error)?;
    }
    File::open(parent)?.sync_all()
}
pub fn remove_private(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_file() && m.uid() == uid() => fs::remove_file(path),
        Ok(_) => Err(denied("refusing to remove an unexpected private file")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
pub fn remove_private_tree(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && m.uid() == uid() => fs::remove_dir_all(path),
        Ok(_) => Err(denied("refusing to remove an unexpected state directory")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// The lock file is never unlinked: replacing its inode would permit two owners.
pub struct SingletonLock {
    _file: File,
}
impl SingletonLock {
    pub fn acquire(path: &Path) -> io::Result<Self> {
        let file = private_file(path, true)?;
        file.try_lock_exclusive()?;
        Ok(Self { _file: file })
    }
}
impl Drop for SingletonLock {
    fn drop(&mut self) {
        // Children can inherit this descriptor between fork and exec. Release
        // the owner's lease now instead of waiting for every copy to close.
        let _ = FileExt::unlock(&self._file);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Peer {
    pub uid: u32,
    pub pid: Option<u32>,
}
pub fn peer(stream: &IpcStream) -> io::Result<Peer> {
    let c = stream.peer_cred()?;
    Ok(Peer {
        uid: c.uid(),
        pid: c.pid().and_then(|p| u32::try_from(p).ok()),
    })
}
pub async fn connect(path: &Path) -> io::Result<IpcStream> {
    let stream = UnixStream::connect(path).await?;
    if peer(&stream)?.uid != uid() {
        return Err(denied("tower socket peer belongs to another user"));
    }
    Ok(stream)
}
/// Call while holding the owning service's singleton lock.
pub async fn prepare_socket_path(path: &Path) -> io::Result<()> {
    require_private_dir(path.parent().ok_or_else(|| denied("invalid socket path"))?)?;
    match fs::symlink_metadata(path) {
        Ok(m) => {
            if (!m.file_type().is_socket() && !m.file_type().is_symlink()) || m.uid() != uid() {
                return Err(denied(
                    "refusing to replace a foreign or invalid socket path",
                ));
            }
            if m.file_type().is_symlink() {
                match fs::metadata(path) {
                    Ok(target) if target.file_type().is_socket() && target.uid() == uid() => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    _ => return Err(denied("refusing to replace an invalid socket alias")),
                }
            }
            match tokio::time::timeout(Duration::from_millis(250), connect(path)).await {
                Ok(Err(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                    ) =>
                {
                    fs::remove_file(path)?
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "socket is active or could not be verified stale",
                    ));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    Ok(())
}
fn resolve_owned_socket(path: &Path) -> io::Result<(PathBuf, fs::Metadata)> {
    let alias = fs::symlink_metadata(path)?;
    if alias.uid() != uid() || (!alias.file_type().is_socket() && !alias.file_type().is_symlink()) {
        return Err(denied("invalid socket alias owner or type"));
    }
    let resolved = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(&resolved)?;
    if !metadata.file_type().is_socket() || metadata.uid() != uid() {
        return Err(denied("invalid private socket owner or type"));
    }
    Ok((resolved, metadata))
}
pub fn require_private_socket(path: &Path) -> io::Result<PathBuf> {
    let (resolved, metadata) = resolve_owned_socket(path)?;
    if metadata.permissions().mode() & 0o077 == 0 {
        return Ok(resolved);
    }
    let parent = resolved
        .parent()
        .ok_or_else(|| denied("invalid socket path"))?;
    if parent
        .ancestors()
        .any(|directory| require_private_dir(directory).is_ok())
    {
        return Ok(resolved);
    }
    Err(denied(
        "socket must be owner-only or inside a private directory",
    ))
}
pub fn private_socket(path: &Path) -> io::Result<()> {
    let (resolved, _) = resolve_owned_socket(path)?;
    fs::set_permissions(resolved, fs::Permissions::from_mode(0o600))
}
pub fn private_socket_identity(path: &Path) -> io::Result<(PathBuf, (u64, u64))> {
    let path = require_private_socket(path)?;
    let (current, metadata) = resolve_owned_socket(&path)?;
    if current != path {
        return Err(denied("private socket changed during validation"));
    }
    Ok((path, (metadata.dev(), metadata.ino())))
}
pub struct IpcListener {
    listener: UnixListener,
    path: PathBuf,
    inode: u64,
}
impl IpcListener {
    /// Call only while holding the corresponding SingletonLock.
    pub async fn bind(path: &Path) -> io::Result<Self> {
        prepare_socket_path(path).await?;
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            listener,
            path: path.into(),
            inode: fs::symlink_metadata(path)?.ino(),
        })
    }
    pub async fn accept(&self) -> io::Result<(IpcStream, Peer)> {
        let (stream, _) = self.listener.accept().await?;
        let p = peer(&stream)?;
        if p.uid != uid() {
            return Err(denied("tower only accepts the current system user"));
        }
        Ok((stream, p))
    }
}
impl Drop for IpcListener {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.ino() == self.inode && m.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub fn ignore_hangup() -> io::Result<()> {
    signal_hook::flag::register(
        signal_hook::consts::SIGHUP,
        Arc::new(AtomicBool::new(false)),
    )?;
    Ok(())
}
/// Called before starting any runtime threads.
pub fn detach(paths: &Paths) -> io::Result<()> {
    detach_log(paths, "tower.log")
}
#[cfg(feature = "terminal")]
pub fn detach_pool(paths: &Paths) -> io::Result<()> {
    detach_log(paths, "pool.log")
}
fn detach_log(paths: &Paths, name: &str) -> io::Result<()> {
    paths.prepare()?;
    ignore_hangup()?;
    let mut log = private_file(&paths.logs.join(name), true)?;
    use std::io::{Seek, SeekFrom};
    log.seek(SeekFrom::End(0))?;
    let null = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    if let nix::unistd::ForkResult::Parent { child } = fork()? {
        let code = match nix::sys::wait::waitpid(child, None)? {
            nix::sys::wait::WaitStatus::Exited(_, code) => code,
            _ => 1,
        };
        std::process::exit(code);
    }
    std::env::set_current_dir("/")?;
    nix::unistd::setsid()?;
    nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(0o077));
    // A second fork leaves the session, so the daemon cannot regain a terminal.
    if let nix::unistd::ForkResult::Parent { .. } = fork()? {
        std::process::exit(0);
    }
    nix::unistd::dup2_stdin(&null)?;
    nix::unistd::dup2_stdout(&log)?;
    nix::unistd::dup2_stderr(&log)?;
    Ok(())
}
#[allow(unsafe_code)]
fn fork() -> nix::Result<nix::unistd::ForkResult> {
    // SAFETY: detach runs before the process starts any other thread, so the
    // child cannot inherit a lock held by a thread that no longer exists.
    unsafe { nix::unistd::fork() }
}

pub type BlockingIpcStream = std::os::unix::net::UnixStream;
pub fn connect_blocking(path: &Path) -> io::Result<BlockingIpcStream> {
    let stream = std::os::unix::net::UnixStream::connect(path)?;
    #[cfg(target_os = "linux")]
    let peer_uid =
        nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::PeerCredentials)?.uid();
    #[cfg(target_os = "macos")]
    let peer_uid = nix::unistd::getpeereid(&stream)?.0.as_raw();
    if peer_uid != uid() {
        return Err(denied("pool belongs to another system user"));
    }
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    Ok(stream)
}

#[cfg(feature = "terminal")]
pub fn start_pool() -> io::Result<()> {
    start_service(&Paths::discover()?, super::Program::TerminalPool)
}
pub async fn shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! { _ = term.recv() => {}, _ = int.recv() => {} }
    Ok(())
}

/// Prefer installed jobs. A fallback is detached from the terminal, not its cgroup.
pub fn start_daemon(paths: &Paths) -> io::Result<()> {
    start_service(paths, super::Program::Server)
}
fn start_service(paths: &Paths, program: super::Program) -> io::Result<()> {
    let name = program.name();
    let unit = format!("{name}.service");
    let job = format!("io.tower.{name}");
    let command = if cfg!(target_os = "linux")
        && paths.config_home.join("systemd/user").join(&unit).is_file()
    {
        let mut command = Command::new("systemctl");
        command.args(["--user", "--no-block", "start", &unit]);
        command
    } else if cfg!(target_os = "macos")
        && paths
            .home
            .join("Library/LaunchAgents")
            .join(format!("{job}.plist"))
            .is_file()
    {
        let mut command = Command::new("launchctl");
        command.args(["kickstart", &format!("gui/{}/{job}", uid())]);
        command
    } else {
        let mut command = Command::new(program.path()?);
        command.args(["run", "--detach"]);
        command
    };
    start_background(command)
}
fn start_background(mut command: Command) -> io::Result<()> {
    let program = std::path::Path::new(command.get_program())
        .file_name()
        .map(|s| s.to_owned());
    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env_remove("TOWER_AGENT_TOKEN")
        .env_remove("TOWER_AGENT_ID")
        .env_remove("TOWER_BEARER_TOKEN")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .env_remove("CODEX_THREAD_ID")
        .env_remove("CODEX_SESSION_ID")
        .status()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound && program.as_ref().is_some_and(|name| {
                [super::Program::Server, super::Program::Cli, super::Program::TerminalPool]
                    .iter().any(|p| name == p.name())
            }) {
                io::Error::new(error.kind(), "Missing Tower companion. Install tower, tower-cli, and tower-terminal-pool together in one directory.")
            } else { error }
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("background process startup failed"))
    }
}

pub fn process_identity(pid: u32) -> io::Result<ProcessIdentity> {
    process_identity_detail(pid, false)
}
pub(crate) fn process_incarnation(pid: u32) -> io::Result<ProcessIdentity> {
    process_identity_detail(pid, true)
}
#[cfg(target_os = "linux")]
fn process_identity_detail(pid: u32, include_exited: bool) -> io::Result<ProcessIdentity> {
    let data = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = data
        .rfind(')')
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let fields: Vec<&str> = data[close + 1..].split_whitespace().collect();
    let parse = |index: usize| -> io::Result<u64> {
        fields
            .get(index)
            .ok_or_else(|| io::Error::other("short process stat"))?
            .parse()
            .map_err(io::Error::other)
    };
    // A zombie has exited, even though kill(pid, 0) could still succeed.
    if !include_exited && fields.first().is_some_and(|s| *s == "Z" || *s == "X") {
        return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
    }
    Ok(ProcessIdentity {
        pid,
        parent: u32::try_from(parse(1)?).map_err(io::Error::other)?,
        start_time: parse(19)?,
    })
}
#[cfg(target_os = "macos")]
fn process_identity_detail(pid: u32, include_exited: bool) -> io::Result<ProcessIdentity> {
    use libproc::bsd_info::BSDInfo;
    let raw = i32::try_from(pid).map_err(io::Error::other)?;
    let process = rustix::process::Pid::from_raw(raw)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid PID"))?;
    // A nonzero argument includes zombies in PROC_PIDTBSDINFO. Darwin's
    // kill(pid, 0) succeeds for zombies, so it cannot establish liveness.
    let info = libproc::proc_pid::pidinfo::<BSDInfo>(raw, 1).map_err(|error| {
        // libproc returns text errors. Confirm absence through the OS before
        // treating a lookup failure as an exited process.
        if rustix::process::test_kill_process(process) == Err(rustix::io::Errno::SRCH) {
            io::Error::new(io::ErrorKind::NotFound, "process exited")
        } else {
            io::Error::other(error)
        }
    })?;
    if !include_exited && info.pbi_status == libc::SZOMB {
        return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
    }
    Ok(ProcessIdentity {
        pid,
        parent: info.pbi_ppid,
        start_time: info
            .pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    })
}

pub fn make_executable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

pub fn owned_socket(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let metadata = std::fs::metadata(path)?;
    if !metadata.file_type().is_socket() || metadata.uid() != uid() {
        return Err(denied("socket is not owned by the current user"));
    }
    Ok(())
}

pub async fn loopback_listeners(port: u16) -> io::Result<Vec<tokio::net::TcpListener>> {
    let v4 = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let port = v4.local_addr()?.port();
    let mut listeners = vec![v4];
    match tokio::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, port)).await {
        Ok(v6) => listeners.push(v6),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) => {}
        Err(e) => return Err(e),
    }
    Ok(listeners)
}

pub fn process_name(pid: u32) -> io::Result<String> {
    #[cfg(target_os = "linux")]
    {
        Ok(std::fs::read_to_string(format!("/proc/{pid}/comm"))?
            .trim()
            .into())
    }
    #[cfg(target_os = "macos")]
    {
        libproc::proc_pid::name(i32::try_from(pid).map_err(io::Error::other)?)
            .map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dropping_owner_releases_lock_despite_an_inherited_descriptor() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("service.lock");
        let owner = SingletonLock::acquire(&path).unwrap();
        // dup and fork share an open file description. CLOEXEC only closes the
        // child's copy after exec, which may overlap another thread's shutdown.
        let inherited = owner._file.try_clone().unwrap();
        drop(owner);
        let next = SingletonLock::acquire(&path).unwrap();
        drop(inherited);
        assert!(SingletonLock::acquire(&path).is_err());
        drop(next);
        assert!(SingletonLock::acquire(&path).is_ok());
    }
    #[test]
    fn unreaped_exited_process_is_not_live() {
        let mut child = Command::new("sh").args(["-c", "exit 0"]).spawn().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let ended = loop {
            if process_identity(child.id()).is_err_and(|e| e.kind() == io::ErrorKind::NotFound) {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        // Deliberately reap only after inspecting the exited process.
        child.wait().unwrap();
        assert!(ended, "unreaped process was considered alive");
    }
    #[tokio::test]
    async fn private_socket_peer_and_lock_survive_contention() {
        let temp = tempfile::tempdir().unwrap();
        private_dir(temp.path()).unwrap();
        let path = temp.path().join("service.sock");
        let lockpath = temp.path().join("service.lock");
        let lock = SingletonLock::acquire(&lockpath).unwrap();
        assert!(SingletonLock::acquire(&lockpath).is_err());
        let listener = IpcListener::bind(&path).await.unwrap();
        let stream = connect(&path).await.unwrap();
        let (_, p) = listener.accept().await.unwrap();
        assert_eq!(p.uid, uid());
        assert_eq!(peer(&stream).unwrap().uid, uid());
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        assert!(IpcListener::bind(&path).await.is_err());
        drop(listener);
        drop(lock);
        assert!(!path.exists());
        assert!(SingletonLock::acquire(&lockpath).is_ok());
    }
    #[tokio::test]
    async fn native_socket_alias_checks_target_and_preserves_live_service() {
        let temp = tempfile::tempdir().unwrap();
        private_dir(temp.path()).unwrap();
        let path = temp.path().join("native.sock");
        let listener = IpcListener::bind(&path).await.unwrap();
        let alias = temp.path().join("configured.sock");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert_eq!(
            require_private_socket(&alias).unwrap(),
            path.canonicalize().unwrap()
        );
        private_socket(&alias).unwrap();
        assert_eq!(
            prepare_socket_path(&alias).await.unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        assert!(
            fs::symlink_metadata(&alias)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        drop(listener);
        prepare_socket_path(&alias).await.unwrap();
        assert!(fs::symlink_metadata(&alias).is_err());
        fs::write(&path, b"ordinary file").unwrap();
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(require_private_socket(&alias).is_err());
        assert!(private_socket(&alias).is_err());
        assert!(prepare_socket_path(&alias).await.is_err());
        assert_eq!(fs::read(path).unwrap(), b"ordinary file");
    }
    #[test]
    fn private_reads_apply_explicit_limits_and_reject_nonregular_paths() {
        let temp = tempfile::tempdir().unwrap();
        private_dir(temp.path()).unwrap();
        let path = temp.path().join("image");
        let bytes = vec![42; 64 * 1024 + 1];
        write_private(&path, &bytes).unwrap();
        assert_eq!(
            super::super::read_private(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            super::super::read_private_bounded(&path, bytes.len()).unwrap(),
            bytes
        );
        assert_eq!(
            super::super::read_private_bounded(&path, bytes.len() - 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(super::super::read_private_bounded(&alias, bytes.len()).is_err());
        assert!(super::super::read_private_bounded(temp.path(), bytes.len()).is_err());
        let fifo = temp.path().join("fifo");
        nix::unistd::mkfifo(
            &fifo,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .unwrap();
        assert!(super::super::read_private_bounded(&fifo, bytes.len()).is_err());
    }
    #[test]
    fn rejects_symlinks_and_identifies_current_process() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        fs::write(&target, "unchanged").unwrap();
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(write_private(&link, b"replacement").is_err());
        assert_eq!(fs::read_to_string(target).unwrap(), "unchanged");
        let first = process_identity(super::super::pid()).unwrap();
        assert!(first.start_time > 0);
        assert_eq!(first, process_identity(first.pid).unwrap());
    }
}
