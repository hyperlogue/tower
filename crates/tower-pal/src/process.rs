use crate::{ProcessIdentity, process_identity, uid};
use std::{io, path::PathBuf};

pub struct ProcessInfo {
    pub identity: ProcessIdentity,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub tower_agent: Option<String>,
}
#[cfg(target_os = "linux")]
pub fn processes() -> io::Result<Vec<u32>> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::read_dir("/proc")?
        .take(50000)
        .filter_map(|e| {
            let e = e.ok()?;
            let pid = e.file_name().to_str()?.parse().ok()?;
            (e.metadata().ok()?.uid() == uid()).then_some(pid)
        })
        .collect())
}
#[cfg(target_os = "macos")]
pub fn processes() -> io::Result<Vec<u32>> {
    libproc::processes::pids_by_type(libproc::processes::ProcFilter::ByUID { uid: uid() })
}
#[cfg(target_os = "linux")]
pub fn inspect_process(pid: u32) -> io::Result<ProcessInfo> {
    use std::io::Read;
    let identity = process_identity(pid)?;
    let root = PathBuf::from(format!("/proc/{pid}"));
    let read = |name: &str| -> io::Result<Vec<u8>> {
        let mut data = Vec::new();
        std::fs::File::open(root.join(name))?
            .take(256 * 1024)
            .read_to_end(&mut data)?;
        Ok(data)
    };
    let argv = read("cmdline")?
        .split(|b| *b == 0)
        .filter(|b| !b.is_empty())
        .take(256)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    let tower_agent = read("environ")?.split(|b| *b == 0).find_map(|b| {
        b.strip_prefix(b"TOWER_AGENT_ID=")
            .map(|s| String::from_utf8_lossy(s).into_owned())
    });
    let cwd = std::fs::read_link(root.join("cwd"))?;
    if process_identity(pid)?.start_time != identity.start_time {
        return Err(io::Error::other("Process changed during inspection"));
    }
    Ok(ProcessInfo {
        identity,
        argv,
        cwd,
        tower_agent,
    })
}
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn inspect_process(pid: u32) -> io::Result<ProcessInfo> {
    let identity = process_identity(pid)?;
    let raw = i32::try_from(pid).map_err(io::Error::other)?;
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, raw];
    let mut bytes = vec![0u8; 256 * 1024];
    let mut len = bytes.len();
    // sysctl writes at most the supplied buffer length. Parse only returned bytes.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            bytes.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    bytes.truncate(len);
    let (argv, tower_agent) = mac_arguments(&bytes)?;
    let mut info = std::mem::MaybeUninit::<libc::proc_vnodepathinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_vnodepathinfo>();
    // proc_pidinfo initializes this fixed-size structure; reject a short result.
    if unsafe {
        libc::proc_pidinfo(
            raw,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            info.as_mut_ptr().cast(),
            size as i32,
        )
    } != size as i32
    {
        return Err(io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    let bytes = info
        .pvi_cdir
        .vip_path
        .iter()
        .take_while(|b| **b != 0)
        .map(|b| *b as u8)
        .collect::<Vec<_>>();
    let cwd = PathBuf::from(String::from_utf8(bytes).map_err(io::Error::other)?);
    if process_identity(pid)?.start_time != identity.start_time {
        return Err(io::Error::other("Process changed during inspection"));
    }
    Ok(ProcessInfo {
        identity,
        argv,
        cwd,
        tower_agent,
    })
}
#[cfg(any(target_os = "macos", test))]
fn mac_arguments(bytes: &[u8]) -> io::Result<(Vec<String>, Option<String>)> {
    let argc = i32::from_ne_bytes(
        bytes
            .get(..4)
            .ok_or_else(|| io::Error::other("Short process args"))?
            .try_into()
            .map_err(io::Error::other)?,
    );
    if !(0..=256).contains(&argc) {
        return Err(io::Error::other("Process argument limit"));
    }
    let mut rest = &bytes[4..];
    let end = rest
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| io::Error::other("Missing executable terminator"))?;
    rest = &rest[end + 1..];
    while rest.first() == Some(&0) {
        rest = &rest[1..];
    }
    let mut argv = Vec::new();
    for _ in 0..argc {
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| io::Error::other("Truncated process argument"))?;
        argv.push(String::from_utf8_lossy(&rest[..end]).into_owned());
        rest = &rest[end + 1..];
    }
    let tower_agent = rest.split(|b| *b == 0).find_map(|s| {
        s.strip_prefix(b"TOWER_AGENT_ID=")
            .map(|s| String::from_utf8_lossy(s).into_owned())
    });
    Ok((argv, tower_agent))
}
pub fn terminate_process(pid: u32, start: u64) -> io::Result<()> {
    signal_process(pid, start, rustix::process::Signal::TERM)
}
pub fn force_terminate_process(pid: u32, start: u64) -> io::Result<()> {
    signal_process(pid, start, rustix::process::Signal::KILL)
}
fn signal_process(pid: u32, start: u64, signal: rustix::process::Signal) -> io::Result<()> {
    let process = rustix::process::Pid::from_raw(i32::try_from(pid).map_err(io::Error::other)?)
        .ok_or_else(|| io::Error::other("Invalid PID"))?;
    #[cfg(target_os = "linux")]
    let handle = rustix::process::pidfd_open(process, rustix::process::PidfdFlags::empty())?;
    if process_identity(pid)?.start_time != start {
        return Err(io::Error::other("Process incarnation changed"));
    }
    #[cfg(target_os = "linux")]
    {
        rustix::process::pidfd_send_signal(&handle, signal).map_err(io::Error::from)
    }
    #[cfg(target_os = "macos")]
    {
        rustix::process::kill_process(process, signal).map_err(io::Error::from)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn mac_empty_arguments_do_not_consume_the_ownership_marker() {
        let mut bytes = 3i32.to_ne_bytes().to_vec();
        bytes.extend_from_slice(
            b"/bin/codex\0\0\0codex\0\0prompt\0TOWER_AGENT_ID=owned\0OTHER=value\0",
        );
        let (argv, id) = super::mac_arguments(&bytes).unwrap();
        assert_eq!(argv, ["codex", "", "prompt"]);
        assert_eq!(id.as_deref(), Some("owned"));
    }
}
