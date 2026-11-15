#[path = "support/cleanup.rs"]
mod cleanup;

#[test]
fn failed_shutdown_keeps_a_live_services_state_reachable() {
    let mut root = tempfile::tempdir().unwrap();
    let saved = root.path().to_path_buf();
    let run = saved.join("state/tower/run");
    tower_pal::private_dir(&run).unwrap();
    let lock = tower_pal::SingletonLock::acquire(&run.join("towerd.lock")).unwrap();
    cleanup::preserve_if_running(&mut root, "state");
    drop(root);
    assert!(run.is_dir());
    drop(lock);
    std::fs::remove_dir_all(saved).unwrap();
}

#[test]
fn confirmed_shutdown_allows_fixture_removal() {
    let mut root = tempfile::tempdir().unwrap();
    let saved = root.path().to_path_buf();
    let run = saved.join("state/tower/run");
    tower_pal::private_dir(&run).unwrap();
    let lock = tower_pal::SingletonLock::acquire(&run.join("towerd.lock")).unwrap();
    drop(lock);
    cleanup::preserve_if_running(&mut root, "state");
    drop(root);
    assert!(!saved.exists());
}
