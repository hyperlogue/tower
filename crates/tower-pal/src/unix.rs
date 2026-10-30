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
fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
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
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}
pub fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| denied("invalid private file path"))?;
    require_private_dir(parent)?;
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
    temp.persist(path).map_err(|e| e.error)?;
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
pub struct IpcListener {
    listener: UnixListener,
    path: PathBuf,
    inode: u64,
}
impl IpcListener {
    /// Call only while holding the corresponding SingletonLock.
    pub async fn bind(path: &Path) -> io::Result<Self> {
        require_private_dir(path.parent().ok_or_else(|| denied("invalid socket path"))?)?;
        match fs::symlink_metadata(path) {
            Ok(m) => {
                if !m.file_type().is_socket() || m.uid() != uid() {
                    return Err(denied("refusing to replace a non-socket or foreign socket"));
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
    paths.prepare()?;
    ignore_hangup()?;
    let mut log = private_file(&paths.logs.join("towerd.log"), true)?;
    use std::io::{Seek, SeekFrom};
    log.seek(SeekFrom::End(0))?;
    daemonize::Daemonize::new()
        .working_directory("/")
        .umask(0o077)
        .stdout(log.try_clone()?)
        .stderr(log)
        .start()
        .map_err(io::Error::other)
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
    let unit = paths.config_home.join("systemd/user/towerd.service");
    let launch_job = paths
        .home
        .join("Library/LaunchAgents/io.tower.towerd.plist");
    let mut command = if cfg!(target_os = "linux") && unit.is_file() {
        let mut c = Command::new("systemctl");
        c.args(["--user", "--no-block", "start", "towerd.service"]);
        c
    } else if cfg!(target_os = "macos") && launch_job.is_file() {
        let mut c = Command::new("launchctl");
        c.args(["kickstart", &format!("gui/{}/io.tower.towerd", uid())]);
        c
    } else {
        let mut c = Command::new(super::executable()?);
        c.args(["daemon", "run", "--detach"]);
        c
    };
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
        .status()?;
    if !status.success() {
        return Err(io::Error::other("daemon startup command failed"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn process_identity(pid: u32) -> io::Result<ProcessIdentity> {
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
    if fields.first().is_some_and(|s| *s == "Z" || *s == "X") {
        return Err(io::Error::new(io::ErrorKind::NotFound, "process exited"));
    }
    Ok(ProcessIdentity {
        pid,
        parent: u32::try_from(parse(1)?).map_err(io::Error::other)?,
        start_time: parse(19)?,
    })
}
#[cfg(target_os = "macos")]
pub fn process_identity(pid: u32) -> io::Result<ProcessIdentity> {
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
    if info.pbi_status == libc::SZOMB {
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

#[cfg(test)]
mod tests {
    use super::*;
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
