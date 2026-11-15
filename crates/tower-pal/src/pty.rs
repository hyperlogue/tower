use nix::{
    fcntl::{FcntlArg, FdFlag, OFlag, fcntl},
    sys::{
        signal::{Signal, killpg},
        termios::{self, SetArg, Termios},
    },
    unistd::{Pid, setsid},
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
};
use tokio::io::unix::AsyncFd;

pub struct PtyProcess {
    io: Arc<AsyncFd<File>>,
    child: Child,
    reaped: bool,
    pub start_time: u64,
}
impl PtyProcess {
    pub fn spawn(
        argv: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Path,
        rows: u16,
        cols: u16,
    ) -> io::Result<Self> {
        let program = argv
            .first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty argv"))?;
        let pair = nix::pty::openpty(
            Some(&nix::pty::Winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            }),
            None,
        )?;
        for fd in [&pair.master, &pair.slave] {
            fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
        }
        let slave = File::from(pair.slave);
        let mut command = Command::new(program);
        for key in [
            "TOWER_AGENT_ID",
            "TOWER_AGENT_TOKEN",
            "TOWER_BEARER_TOKEN",
            "CLAUDE_CODE_SESSION_ID",
            "CODEX_THREAD_ID",
            "CODEX_SESSION_ID",
        ] {
            command.env_remove(key);
        }
        command
            .args(&argv[1..])
            .envs(env)
            .current_dir(cwd)
            .stdin(Stdio::from(slave.try_clone()?))
            .stdout(Stdio::from(slave.try_clone()?))
            .stderr(Stdio::from(slave));
        // Only async-signal-safe syscalls run between fork and exec. Stdio has
        // already been installed by Command; no allocator or lock is used here.
        unsafe {
            command.pre_exec(|| {
                setsid()?;
                if libc::ioctl(0, libc::TIOCSCTTY as _, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let master = File::from(pair.master);
        let flags = OFlag::from_bits_truncate(fcntl(&master, FcntlArg::F_GETFL)?);
        fcntl(&master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        // File owns a stable descriptor until AsyncFd deregisters and drops it.
        let io = Arc::new(unsafe { AsyncFd::register(master)? });
        let child = command.spawn()?;
        let start_time = super::process_identity(child.id())
            .map(|p| p.start_time)
            .unwrap_or(0);
        Ok(Self {
            io,
            child,
            reaped: false,
            start_time,
        })
    }
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
    pub async fn read(&self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.io.readable().await?;
            match guard.try_io(|fd| {
                let mut file = fd.get_ref();
                file.read(bytes)
            }) {
                Ok(Err(e)) if e.raw_os_error() == Some(libc::EIO) => return Ok(0),
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }
    pub async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.io.writable().await?;
            match guard.try_io(|fd| {
                let mut file = fd.get_ref();
                file.write(bytes)
            }) {
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }
    pub fn resize(&self, rows: u16, cols: u16) -> io::Result<()> {
        let size = libc::winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // ioctl reads a live winsize value and does not retain its pointer.
        if unsafe { libc::ioctl(self.io.get_ref().as_raw_fd(), libc::TIOCSWINSZ as _, &size) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub fn signal(&self, force: bool) -> io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        killpg(
            Pid::from_raw(self.child.id() as i32),
            if force {
                Signal::SIGKILL
            } else {
                Signal::SIGTERM
            },
        )
        .or_else(|e| {
            if e == nix::errno::Errno::ESRCH {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        Ok(())
    }
    pub fn poll_exit(&mut self) -> io::Result<Option<i32>> {
        if self.reaped {
            return Ok(None);
        }
        // WNOWAIT pins the leader's PID until descendants in its group are
        // signalled, so an unrelated reused PID can never be killed here.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::waitid(
                libc::P_PID,
                self.child.id(),
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        if unsafe { info.si_pid() } == 0 {
            return Ok(None);
        }
        match self.signal(true) {
            // macOS answers EPERM, not ESRCH, for a group in which only
            // exited processes remain.
            Err(e) if e.raw_os_error() == Some(libc::EPERM) => {}
            signalled => signalled?,
        }
        let status = self.child.wait()?;
        self.reaped = true;
        use std::os::unix::process::ExitStatusExt;
        Ok(Some(
            status
                .code()
                .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
        ))
    }
}
impl Drop for PtyProcess {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.signal(true);
            let _ = self.child.wait();
        }
    }
}

pub struct LocalTerminal {
    tty: File,
    original: Termios,
    input: AsyncFd<File>,
    output: AsyncFd<File>,
}

fn open_terminal() -> io::Result<File> {
    #[cfg(target_os = "macos")]
    let path = {
        use libproc::libproc::{bsd_info::BSDInfo, proc_pid::pidinfo};
        use std::{ffi::CStr, os::unix::ffi::OsStrExt};
        unsafe extern "C" {
            fn devname_r(
                dev: libc::dev_t,
                kind: libc::mode_t,
                buffer: *mut libc::c_char,
                length: libc::c_int,
            ) -> *mut libc::c_char;
        }
        let info = pidinfo::<BSDInfo>(std::process::id() as i32, 0).map_err(io::Error::other)?;
        if info.e_tdev == u32::MAX {
            return Err(io::Error::other("No controlling terminal"));
        }
        let mut name = [0; 128];
        // /dev/tty is an indirect device on Darwin and cannot be registered
        // with kqueue. Resolve this process's controlling device instead.
        let result = unsafe {
            devname_r(
                info.e_tdev as libc::dev_t,
                libc::S_IFCHR,
                name.as_mut_ptr(),
                name.len() as i32,
            )
        };
        if result.is_null() {
            return Err(io::Error::other("Cannot resolve controlling terminal"));
        }
        let name = unsafe { CStr::from_ptr(name.as_ptr()) };
        Path::new("/dev").join(std::ffi::OsStr::from_bytes(name.to_bytes()))
    };
    #[cfg(not(target_os = "macos"))]
    let path = Path::new("/dev/tty");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
}

impl LocalTerminal {
    pub fn open() -> io::Result<Self> {
        let tty = open_terminal()?;
        let original = termios::tcgetattr(&tty)?;
        let mut raw = original.clone();
        termios::cfmakeraw(&mut raw);
        // Independent open descriptions keep O_NONBLOCK off the shell's stdio.
        let input = open_terminal()?;
        fcntl(&input, FcntlArg::F_SETFL(OFlag::O_NONBLOCK))?;
        let output = input.try_clone()?;
        // These owned files are private and their descriptors are never replaced.
        let input = unsafe { AsyncFd::register(input)? };
        let output = unsafe { AsyncFd::register(output)? };
        termios::tcsetattr(&tty, SetArg::TCSANOW, &raw)?;
        Ok(Self {
            tty,
            original,
            input,
            output,
        })
    }
    pub fn size(&self) -> io::Result<(u16, u16)> {
        terminal_size(&self.tty)
    }
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut g = self.input.readable().await?;
            if let Ok(r) = g.try_io(|f| f.get_ref().read(buf)) {
                return r;
            }
        }
    }
    pub async fn write(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let mut g = self.output.writable().await?;
            if let Ok(r) = g.try_io(|f| f.get_ref().write(buf)) {
                let n = r?;
                if n == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                buf = &buf[n..];
            }
        }
        Ok(())
    }
}
impl Drop for LocalTerminal {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(&self.tty, SetArg::TCSANOW, &self.original);
        let mut output = self.output.get_ref();
        let _ = output.write_all(b"\x18\x1b[?2026l\x1b[?1049h\x1b[<65535u\x1b[=0u\x1b[?1049l\x1b[<65535u\x1b[=0u\x1b[>4;0m\x1b>\x1b[?1l\x1b[?6l\x1b[?7h\x1b[4l\x1b[r\x1b[0 q\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l\x1b[?1005l\x1b[?1006l\x1b[?1007l\x1b[?1015l\x1b[?1016l\x1b[?2004l\x1b[?25h\x1b[0m");
    }
}
fn terminal_size(file: &File) -> io::Result<(u16, u16)> {
    let mut size = libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    if unsafe { libc::ioctl(file.as_raw_fd(), libc::TIOCGWINSZ as _, &mut size) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok((size.ws_row.max(1), size.ws_col.max(1)))
}
pub fn is_terminal() -> bool {
    use std::io::IsTerminal;
    io::stdin().is_terminal() && io::stdout().is_terminal()
}
pub fn terminal_resize_signal() -> io::Result<tokio::signal::unix::Signal> {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
}

pub fn prompt_secret(prompt: &str, limit: usize) -> io::Result<String> {
    use nix::poll::{PollFd, PollFlags, poll};
    use nix::sys::signal::SigSet;
    use std::os::fd::AsFd;

    struct Restore {
        tty: File,
        original: Termios,
        mask: SigSet,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = termios::tcsetattr(&self.tty, SetArg::TCSAFLUSH, &self.original);
            let _ = self.tty.write_all(b"\n");
            let _ = self.mask.thread_set_mask();
        }
    }
    let tty = open_terminal().map_err(|_| {
        io::Error::other("No interactive terminal; supply the pairing code as an argument")
    })?;
    let original = termios::tcgetattr(&tty)?;
    let mut hidden = original.clone();
    hidden
        .local_flags
        .remove(termios::LocalFlags::ECHO | termios::LocalFlags::ECHONL);
    let mut restore = Restore {
        tty,
        original,
        mask: SigSet::thread_get_mask()?,
    };
    let signals = [
        Signal::SIGINT,
        Signal::SIGTERM,
        Signal::SIGHUP,
        Signal::SIGTSTP,
    ];
    let mut blocked = SigSet::empty();
    for signal in signals {
        blocked.add(signal);
    }
    // Call before starting worker threads. Restore the terminal before releasing
    // pending signals, without replacing the process's signal handlers.
    blocked.thread_block()?;
    termios::tcsetattr(&restore.tty, SetArg::TCSANOW, &hidden)?;
    restore.tty.write_all(prompt.as_bytes())?;
    restore.tty.flush()?;
    let mut bytes = Vec::new();
    loop {
        let mut pending: libc::sigset_t = unsafe { std::mem::zeroed() };
        if unsafe { libc::sigpending(&mut pending) } == -1 {
            return Err(io::Error::last_os_error());
        }
        if signals
            .iter()
            .any(|signal| unsafe { libc::sigismember(&pending, *signal as i32) } == 1)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Input cancelled",
            ));
        }
        let mut fds = [PollFd::new(restore.tty.as_fd(), PollFlags::POLLIN)];
        match poll(&mut fds, 100u16) {
            Ok(0) | Err(nix::errno::Errno::EINTR) => continue,
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
        let mut byte = [0];
        if restore.tty.read(&mut byte)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Input cancelled",
            ));
        }
        if byte[0] == b'\n' {
            return String::from_utf8(bytes)
                .map_err(|_| io::Error::other("Invalid input encoding"));
        }
        if bytes.len() == limit {
            return Err(io::Error::other("Input exceeds limit"));
        }
        bytes.push(byte[0]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn local_terminal_keeps_stdio_blocking_and_delivers_input() {
        const CHILD: &str = "TOWER_TEST_LOCAL_TERMINAL_CHILD";
        const INPUT: &[u8] = b"terminal-round-trip\n";
        if std::env::var_os(CHILD).is_some() {
            let stdin = io::stdin();
            let flags = fcntl(&stdin, FcntlArg::F_GETFL).unwrap();
            let terminal = LocalTerminal::open().unwrap();
            assert_eq!(fcntl(&stdin, FcntlArg::F_GETFL).unwrap(), flags);
            terminal.write(b"TERMINAL READY\n").await.unwrap();
            let mut received = Vec::new();
            while received.len() < INPUT.len() {
                let mut buf = [0; 64];
                let n = terminal.read(&mut buf).await.unwrap();
                assert_ne!(n, 0);
                received.extend_from_slice(&buf[..n]);
            }
            assert_eq!(received, INPUT);
            terminal.write(b"TERMINAL RECEIVED\n").await.unwrap();
            drop(terminal);
            assert_eq!(fcntl(&stdin, FcntlArg::F_GETFL).unwrap(), flags);
            return;
        }
        let mut child = PtyProcess::spawn(
            &[
                std::env::current_exe()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                "--exact".into(),
                "pty::tests::local_terminal_keeps_stdio_blocking_and_delivers_input".into(),
                "--nocapture".into(),
            ],
            &BTreeMap::from([(CHILD.into(), "1".into())]),
            &std::env::current_dir().unwrap(),
            24,
            80,
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut output = String::new();
            let mut sent = false;
            loop {
                let mut buf = [0; 1024];
                let n = child.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                output.push_str(&String::from_utf8_lossy(&buf[..n]));
                if !sent && output.contains("TERMINAL READY") {
                    let mut remaining = INPUT;
                    while !remaining.is_empty() {
                        let n = child.write(remaining).await.unwrap();
                        assert_ne!(n, 0);
                        remaining = &remaining[n..];
                    }
                    sent = true;
                }
            }
            assert!(output.contains("TERMINAL RECEIVED"), "{output}");
            loop {
                if let Some(code) = child.poll_exit().unwrap() {
                    assert_eq!(code, 0, "{output}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
