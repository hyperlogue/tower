use std::{
    collections::BTreeSet,
    io,
    path::Path,
    time::{Duration, Instant},
};

fn active_locks(state: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut active = Vec::new();
    let mut paths = vec![state.join("run/towerd.lock"), state.join("run/pool.lock")];
    match std::fs::read_dir(state.join("sessions")) {
        Ok(entries) => {
            for entry in entries {
                paths.push(entry?.path().join("launcher.lock"));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    for path in paths {
        match tower_pal::SingletonLock::acquire(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                active.push(path.canonicalize()?)
            }
            Err(error) => return Err(error),
        }
    }
    Ok(active)
}

fn force_owners(locks: &[std::path::PathBuf], killed: &mut BTreeSet<(u32, u64)>) -> io::Result<()> {
    for pid in tower_pal::processes()? {
        if pid == tower_pal::pid() {
            continue;
        }
        let Ok(name) = tower_pal::process_name(pid) else {
            continue;
        };
        if !matches!(
            name.as_str(),
            "tower" | "tower-cli" | "tower-terminal-" | "tower-terminal-pool"
        ) {
            continue;
        }
        let Ok(identity) = tower_pal::process_identity(pid) else {
            continue;
        };
        let Ok(files) = tower_pal::open_files(pid) else {
            continue;
        };
        if !locks.iter().any(|lock| files.contains(lock))
            || killed.contains(&(pid, identity.start_time))
        {
            continue;
        }
        eprintln!(
            "Tower test graceful shutdown failed; force-killing verified fixture process {pid} ({name})"
        );
        if let Err(error) = tower_pal::force_terminate_process(pid, identity.start_time)
            && tower_pal::process_identity(pid)
                .is_ok_and(|now| now.start_time == identity.start_time)
        {
            return Err(error);
        }
        killed.insert((pid, identity.start_time));
    }
    Ok(())
}

fn finish_inner(state: &Path) -> io::Result<bool> {
    let grace = Instant::now() + Duration::from_secs(1);
    let deadline = grace + Duration::from_secs(3);
    let mut killed = BTreeSet::new();
    loop {
        let active = active_locks(state)?;
        let alive = killed.iter().any(|&(pid, start)| {
            tower_pal::process_identity(pid).is_ok_and(|identity| identity.start_time == start)
        });
        if active.is_empty() && !alive {
            return Ok(!killed.is_empty());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test service did not stop after forced cleanup",
            ));
        }
        if Instant::now() >= grace {
            force_owners(&active, &mut killed)?;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn reap(child: &mut std::process::Child) -> io::Result<bool> {
    if child.try_wait()?.is_some() {
        return Ok(false);
    }
    eprintln!(
        "Tower test child did not exit; force-killing and reaping PID {}",
        child.id()
    );
    child.kill()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test child did not exit after SIGKILL",
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(true)
}

pub fn finish(root: &mut tempfile::TempDir, state: &str, child: Option<&mut std::process::Child>) {
    let result = finish_inner(&root.path().join(state).join("tower"));
    // Reap owned foreground children even when service cleanup has failed.
    let reaped = child.map(reap).unwrap_or(Ok(false));
    let failure = match result.and_then(|forced| reaped.map(|reaped| forced || reaped)) {
        Ok(false) => return,
        Ok(true) => {
            "Tower test graceful shutdown failed; fixture processes were forcibly terminated"
                .to_owned()
        }
        Err(error) => {
            root.disable_cleanup(true);
            format!(
                "Tower test cleanup FAILED: {error}; preserving {}",
                root.path().display()
            )
        }
    };
    if std::thread::panicking() {
        eprintln!("{failure}");
    } else {
        panic!("{failure}");
    }
}
