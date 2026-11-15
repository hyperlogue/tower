use std::{
    io,
    time::{Duration, Instant},
};

pub fn preserve_if_running(root: &mut tempfile::TempDir, state: &str) {
    let run = root.path().join(state).join("tower/run");
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let active =
            ["towerd.lock", "pool.lock"].iter().any(
                |name| match tower_pal::SingletonLock::acquire(&run.join(name)) {
                    Ok(_) => false,
                    Err(error) => error.kind() != io::ErrorKind::NotFound,
                },
            );
        if !active {
            return;
        }
        if Instant::now() >= deadline {
            root.disable_cleanup(true);
            eprintln!(
                "Tower test shutdown was not confirmed; preserving {} for cleanup",
                root.path().display()
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
