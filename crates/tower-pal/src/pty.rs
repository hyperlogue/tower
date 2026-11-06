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
        self.signal(true)?;
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
impl LocalTerminal {
    pub fn open() -> io::Result<Self> {
        let tty = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")?;
        let original = termios::tcgetattr(&tty)?;
        let mut raw = original.clone();
        termios::cfmakeraw(&mut raw);
        // Independent open descriptions keep O_NONBLOCK off the shell's stdio.
        let input = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")?;
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
    let tty = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .map_err(|_| {
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
