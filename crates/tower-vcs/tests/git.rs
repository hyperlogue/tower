use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tower_vcs::{Binding, CheckoutTarget, Operation, Prepared};

struct Fixture {
    root: tempfile::TempDir,
    repo: PathBuf,
    worktrees: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("main");
        let worktrees = root.path().join("checkouts");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.name", "Fixture"]);
        git(&repo, &["config", "user.email", "fixture@example.invalid"]);
        std::fs::write(repo.join("file"), "first\n").unwrap();
        std::fs::write(repo.join(".gitignore"), "ignored.log\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "Initial"]);
        Self {
            root,
            repo,
            worktrees,
        }
    }
    async fn binding(&self) -> Binding {
        tower_vcs::discover(&self.repo).await.unwrap().source
    }
    async fn create(&self, name: &str) -> Binding {
        let p = tower_vcs::prepare(
            &self.binding().await,
            Operation::CheckoutCreate {
                name: name.into(),
                target: CheckoutTarget::NewBranch {
                    branch: name.into(),
                    start_point: None,
                },
            },
            &self.worktrees,
        )
        .await
        .unwrap();
        tower_vcs::execute(&p, |_| Ok(()))
            .await
            .unwrap()
            .binding
            .unwrap()
    }
}
fn git(path: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
        ])
        .args(args)
        .current_dir(path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn commit(path: &Path, text: &str) -> String {
    std::fs::write(path.join("file"), text).unwrap();
    git(path, &["add", "file"]);
    git(
        path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "Next",
        ],
    );
    git(path, &["rev-parse", "HEAD"])
}
async fn prepare(f: &Fixture, b: &Binding, op: Operation) -> Prepared {
    tower_vcs::prepare(b, op, &f.worktrees).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_distinguishes_linked_worktrees_storage_and_independent_clones() {
    let f = Fixture::new();
    let main = f.binding().await;
    let a = f.create("alpha").await;
    let b = f.create("beta").await;
    assert_eq!(a.common_dir, b.common_dir);
    assert_ne!(a.git_dir, b.git_dir);
    assert_ne!(a.identity_key(), b.identity_key());
    let found = tower_vcs::discover(&main.common_dir).await.unwrap();
    assert!(found.source.directory.is_none());
    assert!(!found.source.bare);
    assert_eq!(found.worktrees.len(), 3);
    let nested = f.repo.join("sub");
    std::fs::create_dir(&nested).unwrap();
    assert_eq!(
        tower_vcs::discover(&nested)
            .await
            .unwrap()
            .source
            .identity_key(),
        main.identity_key()
    );
    let clone = f.root.path().join("clone");
    git(
        f.root.path(),
        &["clone", f.repo.to_str().unwrap(), clone.to_str().unwrap()],
    );
    let independent = tower_vcs::discover(&clone).await.unwrap().source;
    assert_ne!(independent.identity_key(), main.identity_key());
    assert_ne!(independent.common_dir, main.common_dir);
    let s = tower_vcs::status(&a).await.unwrap();
    assert!(s.available);
    assert_eq!(s.branch.as_deref(), Some("alpha"));
    assert!(!s.dirty);
    let old = b.identity_key();
    std::fs::remove_dir_all(b.directory.as_ref().unwrap()).unwrap();
    let missing = tower_vcs::discover(&main.common_dir)
        .await
        .unwrap()
        .worktrees
        .into_iter()
        .find(|b| b.identity_key() == old)
        .unwrap();
    assert!(!tower_vcs::status(&missing).await.unwrap().available);
}
#[tokio::test]
async fn bare_source_creates_first_checkout_and_identity_survives_move_and_repair() {
    let f = Fixture::new();
    let bare = f.root.path().join("storage.git");
    git(
        f.root.path(),
        &[
            "clone",
            "--bare",
            f.repo.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
    );
    let discovery = tower_vcs::discover(&bare).await.unwrap();
    assert!(discovery.source.bare);
    assert!(discovery.source.directory.is_none());
    assert!(discovery.worktrees.is_empty());
    let p = prepare(
        &f,
        &discovery.source,
        Operation::CheckoutCreate {
            name: "first".into(),
            target: CheckoutTarget::ExistingBranch {
                branch: "main".into(),
            },
        },
    )
    .await;
    let first = tower_vcs::execute(&p, |_| Ok(()))
        .await
        .unwrap()
        .binding
        .unwrap();
    let key = first.identity_key();
    let moved = tower_vcs::execute(
        &prepare(
            &f,
            &first,
            Operation::CheckoutMove {
                name: "moved".into(),
            },
        )
        .await,
        |_| Ok(()),
    )
    .await
    .unwrap()
    .binding
    .unwrap();
    assert_eq!(key, moved.identity_key());
    assert!(moved.can_reconcile(&first));
    assert!(moved.directory.as_ref().unwrap().ends_with("moved"));
    let outside = f.worktrees.join("relocated");
    std::fs::rename(moved.directory.as_ref().unwrap(), &outside).unwrap();
    let repair = prepare(
        &f,
        &moved,
        Operation::CheckoutRepair {
            name: Some("relocated".into()),
        },
    )
    .await;
    let fixed = tower_vcs::execute(&repair, |_| Ok(()))
        .await
        .unwrap()
        .binding
        .unwrap();
    assert_eq!(fixed.identity_key(), key);
    assert!(tower_vcs::status(&fixed).await.unwrap().available);
    let removed = tower_vcs::execute(
        &prepare(&f, &fixed, Operation::CheckoutRemove).await,
        |_| Ok(()),
    )
    .await
    .unwrap();
    assert!(removed.binding.is_none());
    assert!(!outside.exists());
    assert!(
        tower_vcs::discover(&bare)
            .await
            .unwrap()
            .worktrees
            .is_empty()
    );
}
#[tokio::test]
async fn dirty_untracked_ignored_locked_and_main_checkouts_are_protected() {
    let f = Fixture::new();
    let main = f.binding().await;
    let b = f.create("branch").await;
    let dir = b.directory.as_ref().unwrap();
    assert_eq!(
        tower_vcs::prepare(&main, Operation::CheckoutRemove, &f.worktrees)
            .await
            .unwrap_err()
            .code,
        "main_checkout"
    );
    std::fs::write(dir.join("untracked"), "keep").unwrap();
    assert_eq!(
        tower_vcs::prepare(&b, Operation::CheckoutRemove, &f.worktrees)
            .await
            .unwrap_err()
            .code,
        "checkout_dirty"
    );
    std::fs::remove_file(dir.join("untracked")).unwrap();
    std::fs::write(dir.join("file"), "changed").unwrap();
    assert_eq!(
        tower_vcs::prepare(
            &b,
            Operation::CheckoutMove {
                name: "elsewhere".into()
            },
            &f.worktrees
        )
        .await
        .unwrap_err()
        .code,
        "checkout_dirty"
    );
    git(dir, &["restore", "file"]);
    std::fs::write(dir.join("ignored.log"), "keep ignored data").unwrap();
    assert_eq!(
        tower_vcs::prepare(&b, Operation::CheckoutRemove, &f.worktrees)
            .await
            .unwrap_err()
            .code,
        "checkout_has_ignored_files"
    );
    std::fs::remove_file(dir.join("ignored.log")).unwrap();
    git(&f.repo, &["worktree", "lock", dir.to_str().unwrap()]);
    assert_eq!(
        tower_vcs::prepare(&b, Operation::CheckoutRemove, &f.worktrees)
            .await
            .unwrap_err()
            .code,
        "checkout_locked"
    );
    assert_eq!(
        tower_vcs::prepare(
            &main,
            Operation::CheckoutCreate {
                name: "duplicate".into(),
                target: CheckoutTarget::ExistingBranch {
                    branch: "branch".into()
                }
            },
            &f.worktrees
        )
        .await
        .unwrap_err()
        .code,
        "branch_in_use"
    );
    assert!(dir.exists());
}
#[tokio::test]
async fn reviewed_plans_reject_head_configuration_destination_and_callback_drift() {
    let f = Fixture::new();
    let main = f.binding().await;
    let create = || Operation::CheckoutCreate {
        name: "new".into(),
        target: CheckoutTarget::Detached {
            revision: "HEAD".into(),
        },
    };
    let p = prepare(&f, &main, create()).await;
    commit(&f.repo, "second");
    assert_eq!(
        tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err().code,
        "checkout_changed"
    );
    assert!(!f.worktrees.join("new").exists());
    let p = prepare(&f, &main, create()).await;
    git(
        &f.repo,
        &[
            "config",
            "remote.origin.url",
            "https://user:PRIVATE@example.invalid/new?token=SECRET",
        ],
    );
    assert_eq!(
        tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err().code,
        "configuration_changed"
    );
    let s = tower_vcs::status(&main).await.unwrap();
    assert_eq!(s.remote_urls, ["https://example.invalid/new"]);
    let p = prepare(&f, &main, create()).await;
    std::fs::create_dir(f.worktrees.join("new")).unwrap();
    assert_eq!(
        tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err().code,
        "destination_exists"
    );
    std::fs::remove_dir(f.worktrees.join("new")).unwrap();
    let p = prepare(&f, &main, create()).await;
    let err = tower_vcs::execute(&p, |_| Err(tower_vcs::Error::new("checkout_in_use", false)))
        .await
        .unwrap_err();
    assert_eq!(err.code, "checkout_in_use");
    assert!(!err.may_have_effect);
    assert!(!f.worktrees.join("new").exists());
    assert!(!format!("{p:?}").contains("PRIVATE"));
}
#[tokio::test]
async fn pull_and_push_pin_commits_and_never_force_divergent_history() {
    let f = Fixture::new();
    let remote = f.root.path().join("remote.git");
    git(
        f.root.path(),
        &[
            "clone",
            "--bare",
            f.repo.to_str().unwrap(),
            remote.to_str().unwrap(),
        ],
    );
    git(
        &f.repo,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&f.repo, &["fetch", "origin"]);
    git(
        &f.repo,
        &["branch", "--set-upstream-to=origin/main", "main"],
    );
    let producer = f.root.path().join("producer");
    git(
        f.root.path(),
        &[
            "clone",
            remote.to_str().unwrap(),
            producer.to_str().unwrap(),
        ],
    );
    let remote_head = commit(&producer, "remote");
    git(&producer, &["push", "origin", "main"]);
    let b = f.binding().await;
    let old = git(&f.repo, &["rev-parse", "HEAD"]);
    let p = prepare(
        &f,
        &b,
        Operation::Pull {
            remote: None,
            reference: None,
        },
    )
    .await;
    assert_eq!(git(&f.repo, &["rev-parse", "HEAD"]), old);
    assert_eq!(p.review().target.as_deref(), Some(remote_head.as_str()));
    assert!(
        !tower_vcs::execute(&p, |_| Ok(()))
            .await
            .unwrap()
            .refresh_pending
    );
    assert_eq!(git(&f.repo, &["rev-parse", "HEAD"]), remote_head);
    let status = tower_vcs::status(&b).await.unwrap();
    assert_eq!((status.ahead, status.behind), (Some(0), Some(0)));
    let local = commit(&f.repo, "local");
    let p = prepare(
        &f,
        &b,
        Operation::Push {
            remote: None,
            reference: None,
        },
    )
    .await;
    assert!(
        !tower_vcs::execute(&p, |_| Ok(()))
            .await
            .unwrap()
            .refresh_pending
    );
    assert_eq!(git(&remote, &["rev-parse", "refs/heads/main"]), local);
    let status = tower_vcs::status(&b).await.unwrap();
    assert_eq!((status.ahead, status.behind), (Some(0), Some(0)));
    let p = prepare(
        &f,
        &b,
        Operation::Push {
            remote: None,
            reference: None,
        },
    )
    .await;
    let divergent = commit(&producer, "different");
    git(&remote, &["fetch", producer.to_str().unwrap(), "main"]);
    git(&remote, &["update-ref", "refs/heads/main", &divergent]);
    let err = tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err();
    assert_eq!(err.code, "remote_changed");
    assert!(!err.may_have_effect);
    assert_eq!(
        tower_vcs::prepare(
            &b,
            Operation::Push {
                remote: None,
                reference: None
            },
            &f.worktrees
        )
        .await
        .unwrap_err()
        .code,
        "not_fast_forward"
    );
    assert_eq!(git(&remote, &["rev-parse", "refs/heads/main"]), divergent);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn siblings_serialize_but_separate_repositories_can_mutate_concurrently() {
    let f = Fixture::new();
    let a = f.create("alpha").await;
    let b = f.create("beta").await;
    let pa = prepare(&f, &a, Operation::CheckoutRepair { name: None }).await;
    let pb = prepare(&f, &b, Operation::CheckoutRepair { name: None }).await;
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let run = |plan: Prepared, active: Arc<AtomicUsize>, maximum: Arc<AtomicUsize>| {
        tokio::spawn(async move {
            tower_vcs::execute(&plan, move |_| {
                tokio::task::block_in_place(|| {
                    let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(n, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
            })
            .await
        })
    };
    let (left, right) = tokio::join!(
        run(pa.clone(), active.clone(), maximum.clone()),
        run(pb, active, maximum.clone())
    );
    left.unwrap().unwrap();
    right.unwrap().unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    let other = Fixture::new();
    let c = other.create("gamma").await;
    let pc = prepare(&other, &c, Operation::CheckoutRepair { name: None }).await;
    let entered = Arc::new(AtomicUsize::new(0));
    let launch = |p: Prepared, entered: Arc<AtomicUsize>| {
        tokio::spawn(async move {
            tower_vcs::execute(&p, move |_| {
                tokio::task::block_in_place(|| {
                    entered.fetch_add(1, Ordering::SeqCst);
                    let until = Instant::now() + Duration::from_secs(5);
                    while entered.load(Ordering::SeqCst) < 2 && Instant::now() < until {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    if entered.load(Ordering::SeqCst) != 2 {
                        return Err(tower_vcs::Error::new("unexpected_global_lock", false));
                    }
                    Ok(())
                })
            })
            .await
        })
    };
    let (left, right) = tokio::join!(launch(pa, entered.clone()), launch(pc, entered));
    assert!(
        matches!(&left, Ok(Ok(_))) && matches!(&right, Ok(Ok(_))),
        "left: {left:?}, right: {right:?}"
    );
}

#[tokio::test]
async fn directory_copy_with_reused_git_pointer_cannot_take_the_old_identity() {
    let f = Fixture::new();
    let original = f.create("original").await;
    tower_vcs::validate_binding(&original).unwrap();
    let path = original.directory.as_ref().unwrap();
    let saved = f.worktrees.join("saved");
    std::fs::rename(path, &saved).unwrap();
    assert!(!tower_vcs::status(&original).await.unwrap().available);
    let absent = tower_vcs::discover(&original.common_dir)
        .await
        .unwrap()
        .worktrees
        .into_iter()
        .find(|b| b.identity_key() == original.identity_key())
        .unwrap();
    assert!(!absent.can_reconcile(&original));
    std::fs::create_dir(path).unwrap();
    for name in [".git", ".gitignore", "file"] {
        std::fs::copy(saved.join(name), path.join(name)).unwrap();
    }
    let replacement = tower_vcs::discover(path).await.unwrap().source;
    assert_eq!(replacement.identity_key(), original.identity_key());
    assert!(!replacement.can_reconcile(&original));
    assert_eq!(
        tower_vcs::validate_binding(&original).unwrap_err().code,
        "identity_changed"
    );
    assert!(!tower_vcs::status(&original).await.unwrap().available);
    let err = tower_vcs::prepare(&original, Operation::CheckoutRemove, &f.worktrees)
        .await
        .unwrap_err();
    assert_eq!(err.code, "identity_changed");
    assert!(!err.may_have_effect);
    assert!(path.join("file").exists());
    std::fs::remove_dir_all(path).unwrap();
    std::fs::rename(saved, path).unwrap();
    assert!(
        tower_vcs::discover(path)
            .await
            .unwrap()
            .source
            .can_reconcile(&original)
    );
    std::fs::rename(&original.git_dir, original.git_dir.with_extension("old")).unwrap();
    assert!(!tower_vcs::status(&original).await.unwrap().available);
}
#[tokio::test]
async fn names_root_replacement_and_git_pointer_redirection_are_checked() {
    let f = Fixture::new();
    let b = f.create("source").await;
    for name in [
        "../escape",
        "/absolute",
        "nested/name",
        "..",
        ".",
        "back\\slash",
    ] {
        assert_eq!(
            tower_vcs::prepare(
                &b,
                Operation::CheckoutMove { name: name.into() },
                &f.worktrees
            )
            .await
            .unwrap_err()
            .code,
            "invalid_worktree_name"
        );
    }
    let p = prepare(
        &f,
        &b,
        Operation::CheckoutCreate {
            name: "destination".into(),
            target: CheckoutTarget::Detached {
                revision: "HEAD".into(),
            },
        },
    )
    .await;
    let replacement = Fixture::new();
    std::fs::write(
        b.directory.as_ref().unwrap().join(".git"),
        format!("gitdir: {}\n", replacement.repo.join(".git").display()),
    )
    .unwrap();
    assert_eq!(
        tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err().code,
        "stale_checkout"
    );
    assert!(!f.worktrees.join("destination").exists());
    std::fs::write(
        b.directory.as_ref().unwrap().join(".git"),
        format!("gitdir: {}\n", b.git_dir.display()),
    )
    .unwrap();
    let main = f.binding().await;
    let p = prepare(
        &f,
        &main,
        Operation::CheckoutCreate {
            name: "review copy".into(),
            target: CheckoutTarget::Detached {
                revision: "HEAD".into(),
            },
        },
    )
    .await;
    let moved = f.root.path().join("old-root");
    std::fs::rename(&f.worktrees, &moved).unwrap();
    std::fs::create_dir(&f.worktrees).unwrap();
    let error = tower_vcs::execute(&p, |_| Ok(())).await.unwrap_err();
    assert!(matches!(
        error.code.as_str(),
        "worktrees_changed" | "worktree_root_changed"
    ));
    assert!(!error.may_have_effect);
    assert!(!f.worktrees.join("review copy").exists());
}
#[tokio::test]
async fn push_configuration_redirection_invalidates_the_private_destination() {
    let f = Fixture::new();
    let old = f.root.path().join("old.git");
    let new = f.root.path().join("new.git");
    for path in [&old, &new] {
        git(
            f.root.path(),
            &[
                "clone",
                "--bare",
                f.repo.to_str().unwrap(),
                path.to_str().unwrap(),
            ],
        );
    }
    git(&f.repo, &["remote", "add", "origin", old.to_str().unwrap()]);
    commit(&f.repo, "reviewed");
    let binding = f.binding().await;
    let p = prepare(
        &f,
        &binding,
        Operation::Push {
            remote: Some("origin".into()),
            reference: Some("main".into()),
        },
    )
    .await;
    let before = git(&old, &["rev-parse", "main"]);
    git(
        &f.repo,
        &["config", "remote.origin.pushurl", new.to_str().unwrap()],
    );
    let err = tower_vcs::execute(&p, |_| {
        panic!("authority callback must not run for stale configuration")
    })
    .await
    .unwrap_err();
    assert_eq!(err.code, "configuration_changed");
    assert!(!err.may_have_effect);
    assert_eq!(git(&old, &["rev-parse", "main"]), before);
    assert_eq!(git(&new, &["rev-parse", "main"]), before);
}
#[tokio::test]
async fn unresolved_merge_is_reported_and_blocks_checkout_mutation() {
    let f = Fixture::new();
    let b = f.create("conflict").await;
    let dir = b.directory.as_ref().unwrap();
    git(dir, &["checkout", "-b", "other"]);
    commit(dir, "other");
    git(dir, &["checkout", "conflict"]);
    commit(dir, "local");
    let merged = Command::new("git")
        .args(["-c", "core.hooksPath=/dev/null", "merge", "other"])
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(!merged.status.success());
    let snapshot = tower_vcs::status(&b).await.unwrap();
    assert!(snapshot.conflicts);
    assert!(snapshot.operation_in_progress);
    let err = tower_vcs::prepare(
        &b,
        Operation::CheckoutMove {
            name: "moved".into(),
        },
        &f.worktrees,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "checkout_conflicted");
    assert!(dir.join("file").exists());
}

#[tokio::test]
async fn native_failure_after_dispatch_is_not_reported_as_safe_to_retry() {
    let f = Fixture::new();
    std::fs::write(f.repo.join(".gitattributes"), "file filter=broken\n").unwrap();
    git(&f.repo, &["add", ".gitattributes"]);
    git(&f.repo, &["commit", "-m", "Configure filter"]);
    git(&f.repo, &["config", "filter.broken.clean", "cat"]);
    git(&f.repo, &["config", "filter.broken.smudge", "false"]);
    git(&f.repo, &["config", "filter.broken.required", "true"]);
    let p = prepare(
        &f,
        &f.binding().await,
        Operation::CheckoutCreate {
            name: "failed".into(),
            target: CheckoutTarget::NewBranch {
                branch: "failed".into(),
                start_point: None,
            },
        },
    )
    .await;
    let called = AtomicUsize::new(0);
    let err = tower_vcs::execute(&p, |_| {
        called.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await
    .unwrap_err();
    assert_eq!(err.code, "operation_failed");
    assert!(err.may_have_effect);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        git(&f.repo, &["rev-parse", "failed"]),
        git(&f.repo, &["rev-parse", "main"])
    );
}
#[tokio::test]
async fn private_plan_rehydration_still_binds_the_reviewed_destination() {
    let f = Fixture::new();
    let p = prepare(
        &f,
        &f.binding().await,
        Operation::CheckoutCreate {
            name: "reviewed".into(),
            target: CheckoutTarget::Detached {
                revision: "HEAD".into(),
            },
        },
    )
    .await;
    let mut value = serde_json::to_value(&p).unwrap();
    value["intent"]["path"] = serde_json::json!(f.worktrees.join("unreviewed"));
    let altered: Prepared = serde_json::from_value(value).unwrap();
    let err = tower_vcs::execute(&altered, |_| panic!("invalid plan must not pass admission"))
        .await
        .unwrap_err();
    assert_eq!(err.code, "invalid_plan");
    assert!(!err.may_have_effect);
    assert!(!f.worktrees.join("unreviewed").exists());
    assert!(!f.worktrees.join("reviewed").exists());
}

#[tokio::test]
async fn concurrent_tracking_update_is_preserved_without_failing_a_completed_pull() {
    let f = Fixture::new();
    let remote = f.root.path().join("remote.git");
    git(
        f.root.path(),
        &[
            "clone",
            "--bare",
            f.repo.to_str().unwrap(),
            remote.to_str().unwrap(),
        ],
    );
    git(
        &f.repo,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&f.repo, &["fetch", "origin"]);
    git(
        &f.repo,
        &["branch", "--set-upstream-to=origin/main", "main"],
    );
    let producer = f.root.path().join("producer");
    git(
        f.root.path(),
        &[
            "clone",
            remote.to_str().unwrap(),
            producer.to_str().unwrap(),
        ],
    );
    let reviewed = commit(&producer, "reviewed");
    git(&producer, &["push", "origin", "main"]);
    let b = f.binding().await;
    let plan = prepare(
        &f,
        &b,
        Operation::Pull {
            remote: None,
            reference: None,
        },
    )
    .await;
    let concurrent = commit(&producer, "later native fetch");
    git(&f.repo, &["fetch", producer.to_str().unwrap(), "main"]);
    let outcome = tower_vcs::execute(&plan, |_| {
        git(
            &f.repo,
            &["update-ref", "refs/remotes/origin/main", &concurrent],
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(outcome.state, "applied");
    assert!(outcome.refresh_pending);
    assert_eq!(git(&f.repo, &["rev-parse", "HEAD"]), reviewed);
    assert_eq!(
        git(&f.repo, &["rev-parse", "refs/remotes/origin/main"]),
        concurrent
    );
}
#[tokio::test]
async fn missing_relative_worktree_keeps_its_administrative_identity() {
    let f = Fixture::new();
    std::fs::create_dir(&f.worktrees).unwrap();
    let path = f.worktrees.join("relative");
    git(
        &f.repo,
        &[
            "worktree",
            "add",
            "--relative-paths",
            "-b",
            "relative",
            path.to_str().unwrap(),
            "HEAD",
        ],
    );
    let binding = tower_vcs::discover(&path).await.unwrap().source;
    let pointer = std::fs::read_to_string(binding.git_dir.join("gitdir")).unwrap();
    assert!(!Path::new(pointer.trim()).is_absolute());
    std::fs::remove_dir_all(&path).unwrap();
    let discovery = tower_vcs::discover(&binding.common_dir).await.unwrap();
    let missing = discovery
        .worktrees
        .iter()
        .find(|b| b.identity_key() == binding.identity_key())
        .expect("relative backlink must retain checkout identity");
    assert_eq!(missing.directory.as_ref(), Some(&path));
    assert!(!tower_vcs::status(missing).await.unwrap().available);
    assert!(!missing.can_reconcile(&binding));
}
