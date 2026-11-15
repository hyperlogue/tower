#[path = "support/cleanup.rs"]
mod cleanup;

use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Pool {
    root: tempfile::TempDir,
    child: Child,
}
impl Pool {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-clean-")
            .tempdir_in("/tmp")
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_tower-terminal-pool"))
            .arg("run")
            .env("HOME", root.path())
            .env("XDG_STATE_HOME", root.path().join("state"))
            .env("TOKIO_WORKER_THREADS", "2")
            .env_remove("TOWER_SOCKET")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut pool = Self { root, child };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.root.path().join("state/tower/run/pool.sock").exists() {
            assert!(
                pool.child.try_wait().unwrap().is_none(),
                "pool exited during startup"
            );
            assert!(Instant::now() < deadline, "pool did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        pool
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none()
            && let Err(error) = self.child.kill()
        {
            self.root.disable_cleanup(true);
            eprintln!("cleanup regression could not kill its child: {error}");
            return;
        }
        let _ = self.child.wait();
        self.root.disable_cleanup(false);
    }
}

#[test]
fn failed_graceful_shutdown_forces_only_the_fixture_owned_process() {
    let mut target = Pool::new();
    let mut unrelated = Pool::new();
    let reported = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cleanup::finish(&mut target.root, "state", Some(&mut target.child));
    }));
    assert!(
        reported.is_err(),
        "unexpected forced cleanup must fail the test"
    );
    assert!(
        target.child.try_wait().unwrap().is_some(),
        "cleanup left the test pool running"
    );
    assert!(
        unrelated.child.try_wait().unwrap().is_none(),
        "cleanup killed another fixture's pool"
    );
}

#[test]
fn confirmed_shutdown_allows_fixture_removal() {
    let mut root = tempfile::tempdir().unwrap();
    let saved = root.path().to_path_buf();
    let run = saved.join("state/tower/run");
    tower_pal::private_dir(&run).unwrap();
    let lock = tower_pal::SingletonLock::acquire(&run.join("towerd.lock")).unwrap();
    drop(lock);
    cleanup::finish(&mut root, "state", None);
    drop(root);
    assert!(!saved.exists());
}
