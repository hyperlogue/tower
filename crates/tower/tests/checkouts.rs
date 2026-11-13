use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

struct Fixture {
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("tw-vcs-")
            .tempdir_in("/tmp")
            .unwrap();
        Self { root }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower-cli"));
        c.args(args)
            .env_clear()
            .env("HOME", self.root.path())
            .env("PATH", std::env::var("PATH").unwrap())
            .env("XDG_STATE_HOME", self.root.path().join("state"))
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("TOKIO_WORKER_THREADS", "2")
            .current_dir(self.root.path());
        c
    }
    fn call(&self, args: &[&str]) -> Value {
        let o = self.command(&["--json"]).args(args).output().unwrap();
        assert!(
            o.status.success(),
            "{args:?}: {} {}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        serde_json::from_slice(&o.stdout).unwrap()
    }
    fn rpc(&self, m: &str, p: Value) -> Value {
        self.call(&["rpc", m, &p.to_string()])
    }
    fn git(&self, path: &Path, args: &[&str]) -> String {
        let o = Command::new("git")
            .args(args)
            .current_dir(path)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("HOME", self.root.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        String::from_utf8(o.stdout).unwrap().trim().into()
    }
    fn repo(&self) -> PathBuf {
        let p = self.root.path().join("repo");
        std::fs::create_dir(&p).unwrap();
        self.git(&p, &["init", "-b", "main"]);
        self.git(&p, &["config", "user.name", "Fixture"]);
        self.git(&p, &["config", "user.email", "fixture@example.invalid"]);
        std::fs::write(p.join("file"), "initial\n").unwrap();
        self.git(&p, &["add", "file"]);
        self.git(&p, &["commit", "-m", "Initial fixture"]);
        p
    }
    fn prepare(&self, id: &str, operation: &str, options: Value) -> Value {
        self.rpc(
            "vcs.prepare",
            json!({"checkout_id":id,"operation":operation,"options":options}),
        )
    }
    fn execute(&self, plan: &Value) -> Value {
        let mut result = self.call(&["vcs", "execute", plan["plan_id"].as_str().unwrap()]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while result["state"] == "completed" && result["result"]["refresh_pending"] == true {
            assert!(
                std::time::Instant::now() < deadline,
                "metadata refresh: {result}"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
            result = self.call(&["vcs", "operation", plan["plan_id"].as_str().unwrap()]);
        }
        result
    }
    fn path(&self, wire: &Value) -> PathBuf {
        let s = wire["path"].as_str().unwrap();
        if let Some(p) = s.strip_prefix("~/") {
            self.root.path().join(p)
        } else {
            PathBuf::from(s)
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self
            .command(&["--no-start", "daemon", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
#[test]
fn worktree_lifecycle_is_addressed_by_id_and_active_sessions_block_moves() {
    let f = Fixture::new();
    let repo = f.repo();
    let registered = f.call(&["checkout", "register", repo.to_str().unwrap()]);
    let main = registered["checkouts"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let plan = f.prepare(
        &main,
        "checkout.create",
        json!({"name":"feature","target":{"kind":"new_branch","branch":"feature"}}),
    );
    assert_eq!(plan["state"], "prepared");
    let done = f.execute(&plan);
    assert_eq!(done["state"], "completed", "{done}");
    assert_eq!(f.execute(&plan)["state"], "completed");
    let rows = f.call(&["checkout", "list"]);
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let feature = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["branch"] == "feature")
        .unwrap();
    let feature_id = feature["id"].as_str().unwrap().to_owned();
    let old = f.path(feature);
    let registered = f.call(&[
        "register",
        "--harness",
        "fixture",
        "--cwd",
        old.to_str().unwrap(),
    ]);
    let plan = f.prepare(&feature_id, "checkout.move", json!({"name":"moved"}));
    let rejected = f
        .command(&[
            "--json",
            "vcs",
            "execute",
            plan["plan_id"].as_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stdout).contains("checkout_in_use"));
    let o = f
        .command(&["unregister"])
        .env("TOWER_AGENT_ID", registered["id"].as_str().unwrap())
        .env("TOWER_AGENT_TOKEN", registered["token"].as_str().unwrap())
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let done = f.execute(&plan);
    assert_eq!(done["state"], "completed", "{done}");
    let moved = f.call(&["checkout", "get", &feature_id]);
    assert_eq!(moved["id"], feature_id);
    assert_ne!(f.path(&moved), old);
    assert!(!old.exists());
    let moved_path = f.path(&moved);
    std::fs::write(moved_path.join("untracked"), "keep").unwrap();
    let unsafe_plan = f
        .command(&["--json", "vcs", "prepare", &feature_id, "checkout.remove"])
        .output()
        .unwrap();
    assert!(!unsafe_plan.status.success());
    assert!(moved_path.join("untracked").exists());
    std::fs::remove_file(moved_path.join("untracked")).unwrap();
    let plan = f.prepare(&feature_id, "checkout.remove", json!({}));
    let done = f.execute(&plan);
    assert_eq!(done["state"], "completed", "{done}");
    assert!(!moved_path.exists());
    assert_eq!(f.call(&["vcs", "status", &feature_id])["available"], false);
    f.call(&["checkout", "forget", &feature_id]);
    f.call(&["checkout", "register", repo.to_str().unwrap()]);
    assert!(
        !f.call(&["checkout", "list"])
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == feature_id || r["path"] == moved["path"])
    );

    assert_eq!(f.call(&["vcs", "status", &main])["available"], true);
    f.call(&["daemon", "stop"]);
    assert_eq!(f.call(&["checkout", "get", &main])["id"], main);
    let arbitrary = f
        .command(&[
            "--json",
            "rpc",
            "vcs.status",
            &json!({"path":repo}).to_string(),
        ])
        .output()
        .unwrap();
    assert!(!arbitrary.status.success());
}
#[test]
fn bare_storage_can_prepare_its_first_worktree_without_a_fake_checkout_id() {
    let f = Fixture::new();
    let repo = f.repo();
    let bare = f.root.path().join("bare.git");
    f.git(
        f.root.path(),
        &[
            "clone",
            "--bare",
            repo.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
    );
    let registration=f.rpc("checkout.register",json!({"directory":bare,"create":{"name":"first","target":{"kind":"detached","revision":"HEAD"}}}));
    assert!(registration["checkouts"].as_array().unwrap().is_empty());
    assert!(registration["plan"]["checkout_id"].is_null());
    let result = f.execute(&registration["plan"]);
    assert_eq!(result["state"], "completed", "{result}");
    let rows = f.call(&["checkout", "list"]);
    assert_eq!(rows.as_array().unwrap().len(), 1);
    let row = &rows[0];
    assert!(row["branch"].is_null());
    assert!(f.path(row).join("file").exists());
}
#[test]
fn read_credentials_cannot_register_or_mutate_checkouts() {
    let f = Fixture::new();
    let repo = f.repo();
    let row = f.call(&["checkout", "register", repo.to_str().unwrap()])["checkouts"][0].clone();
    let token = f.call(&["token", "issue", "reader", "--scope", "read"])["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let o = f
        .command(&["checkout", "list"])
        .env("TOWER_BEARER_TOKEN", &token)
        .output()
        .unwrap();
    assert!(o.status.success());
    for args in [
        vec!["checkout", "register", repo.to_str().unwrap()],
        vec![
            "vcs",
            "prepare",
            row["id"].as_str().unwrap(),
            "checkout.remove",
        ],
    ] {
        let o = f
            .command(&args)
            .env("TOWER_BEARER_TOKEN", &token)
            .output()
            .unwrap();
        assert!(!o.status.success());
    }
}

#[test]
fn selected_checkout_cannot_launch_into_a_replaced_directory() {
    let f = Fixture::new();
    let repo = f.repo();
    let row = f.call(&["checkout", "register", repo.to_str().unwrap()])["checkouts"][0].clone();
    std::fs::rename(&repo, f.root.path().join("original")).unwrap();
    std::fs::create_dir(&repo).unwrap();
    f.git(&repo, &["init", "-b", "main"]);
    let output = f
        .command(&[
            "--json",
            "launch",
            "codex",
            "--headless",
            "--checkout",
            row["id"].as_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("checkout_unavailable"),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct AgentFixture {
    path: PathBuf,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl AgentFixture {
    fn new(root: &Path, name: &'static str) -> Self {
        use std::io::Write;
        let path = root.join(format!("{name}.sock"));
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        tower_pal::private_socket(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = stop.clone();
        let thread = std::thread::spawn(move || {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.write_all(name.as_bytes());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5))
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        });
        Self {
            path,
            stop,
            thread: Some(thread),
        }
    }
    fn auth(&self) -> Value {
        json!({"kind":"ssh_agent","socket":self.path})
    }
}
impl Drop for AgentFixture {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn vcs_agents_are_per_phase_private_and_never_replayed_after_an_uncertain_push() {
    let f = Fixture::new();
    let repo = f.repo();
    let root = f.root.path();
    let remote = root.join("remote.git");
    f.git(
        root,
        &[
            "clone",
            "--bare",
            repo.to_str().unwrap(),
            remote.to_str().unwrap(),
        ],
    );
    let ssh = root.join("ssh-fixture.mjs");
    std::fs::write(&ssh, include_str!("fixtures/vcs-ssh.mjs")).unwrap();
    let log = root.join("ssh-calls");
    let hold = root.join("hold-push");
    let lose = root.join("lose-agent");
    std::fs::write(
        root.join("ssh-fixture.mjs.json"),
        json!({"remote":remote,"log":log,"hold":hold,"lose":lose}).to_string(),
    )
    .unwrap();
    f.git(
        &repo,
        &[
            "config",
            "core.sshCommand",
            &format!("node '{}'", ssh.display()),
        ],
    );
    f.git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "ssh://fixture.invalid/repository",
        ],
    );
    let id = f.call(&["checkout", "register", repo.to_str().unwrap()])["checkouts"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    std::fs::write(repo.join("file"), "next").unwrap();
    f.git(&repo, &["commit", "-am", "Next"]);
    let a = AgentFixture::new(root, "agent-a");
    let output = f
        .command(&[
            "vcs",
            "prepare",
            &id,
            "push",
            "--options",
            r#"{"remote":"origin","reference":"main"}"#,
            "--auth",
            "ssh-agent",
        ])
        .env("SSH_AUTH_SOCK", &a.path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan["review"]["authentication"], "ssh_agent");
    let persisted = rusqlite::Connection::open(root.join("state/tower/tower.db"))
        .unwrap()
        .query_row(
            "SELECT prepared FROM vcs_operations WHERE id=?1",
            [plan["plan_id"].as_str().unwrap()],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    assert!(persisted.contains("ssh_agent"));
    assert_eq!(
        serde_json::from_str::<Value>(&persisted).unwrap()["format"],
        "vcs.ssh-agent.v1"
    );
    assert!(!persisted.contains(a.path.to_str().unwrap()));
    assert!(!persisted.contains("agent-a.sock"));
    let plan_id = plan["plan_id"].as_str().unwrap();
    let a_path = a.path.clone();
    drop(a);
    f.call(&["daemon", "stop"]);
    let no_auth = f
        .command(&[
            "rpc",
            "vcs.execute",
            &json!({"plan_id":plan_id}).to_string(),
        ])
        .output()
        .unwrap();
    assert!(!no_auth.status.success());
    assert_eq!(
        f.rpc("vcs.operation", json!({"plan_id":plan_id}))["state"],
        "prepared"
    );
    let b = AgentFixture::new(root, "agent-b");
    std::fs::write(&hold, "wait").unwrap();
    let auth = b.auth();
    std::thread::scope(|scope| {
        let execution =
            scope.spawn(|| f.rpc("vcs.execute", json!({"plan_id":plan_id,"auth":auth})));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !std::fs::read_to_string(&log)
            .unwrap_or_default()
            .contains(r#""command":"receive""#)
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(
            f.rpc("vcs.operation", json!({"plan_id":plan_id}))["state"],
            "running"
        );
        std::fs::remove_file(&hold).unwrap();
        assert_eq!(execution.join().unwrap()["state"], "completed");
    });
    let calls: Vec<Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert!(
        calls
            .iter()
            .any(|c| c["agent"] == "agent-a" && c["command"] == "upload")
    );
    assert!(
        calls
            .iter()
            .any(|c| c["agent"] == "agent-b" && c["command"] == "receive")
    );
    let b_path = b.path.clone();
    drop(b);
    assert_eq!(f.call(&["vcs", "execute", plan_id])["state"], "completed");
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().lines().count(),
        calls.len()
    );
    let database = rusqlite::Connection::open(root.join("state/tower/tower.db")).unwrap();
    let dump: String = database
        .prepare("SELECT prepared FROM vcs_operations")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for path in [&a_path, &b_path] {
        assert!(!dump.contains(path.to_str().unwrap()));
    }

    let clone = root.join("another-clone");
    f.git(
        root,
        &["clone", repo.to_str().unwrap(), clone.to_str().unwrap()],
    );
    f.git(
        &clone,
        &[
            "remote",
            "set-url",
            "origin",
            "ssh://fixture.invalid/repository",
        ],
    );
    f.git(
        &clone,
        &[
            "config",
            "core.sshCommand",
            &format!("node '{}'", ssh.display()),
        ],
    );
    let clone_id = f.call(&["checkout", "register", clone.to_str().unwrap()])["checkouts"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let d = AgentFixture::new(root, "agent-d");
    let e = AgentFixture::new(root, "agent-e");
    let request_d = json!({"checkout_id":id,"operation":"push","options":{"remote":"origin","reference":"main"},"auth":{"kind":"ssh_agent","socket":"~/agent-d.sock"}});
    let request_e = json!({"checkout_id":clone_id,"operation":"push","options":{"remote":"origin","reference":"main"},"auth":e.auth()});
    std::thread::scope(|scope| {
        let one = scope.spawn(|| f.rpc("vcs.prepare", request_d));
        let two = scope.spawn(|| f.rpc("vcs.prepare", request_e));
        assert_eq!(one.join().unwrap()["state"], "prepared");
        assert_eq!(two.join().unwrap()["state"], "prepared");
    });
    let trace = std::fs::read_to_string(&log).unwrap();
    for line in trace.lines() {
        let call: Value = serde_json::from_str(line).unwrap();
        if call["agent"] == "agent-d" {
            assert_eq!(call["cwd"], repo.canonicalize().unwrap().to_str().unwrap());
        }
        if call["agent"] == "agent-e" {
            assert_eq!(call["cwd"], clone.canonicalize().unwrap().to_str().unwrap());
        }
    }
    assert!(trace.contains("agent-d") && trace.contains("agent-e"));
    drop((d, e));

    std::fs::write(repo.join("file"), "uncertain next").unwrap();
    f.git(&repo, &["commit", "-am", "Uncertain fixture"]);
    let c = AgentFixture::new(root, "agent-c");
    let second = f.rpc("vcs.prepare", json!({"checkout_id":id,"operation":"push","options":{"remote":"origin","reference":"main"},"auth":c.auth()}));
    let second_id = second["plan_id"].as_str().unwrap();
    std::fs::write(&lose, "lose socket after remote success").unwrap();
    let output = f
        .command(&["vcs", "execute", second_id])
        .env("SSH_AUTH_SOCK", &c.path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let unknown: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(unknown["state"], "unknown", "{unknown}");
    assert_eq!(unknown["error"]["data"]["may_have_effect"], true);
    assert_eq!(
        f.git(&remote, &["rev-parse", "main"]),
        f.git(&repo, &["rev-parse", "main"])
    );
    let before = std::fs::read_to_string(&log).unwrap();
    assert_eq!(
        f.rpc("vcs.execute", json!({"plan_id":second_id}))["state"],
        "unknown"
    );
    assert_eq!(std::fs::read_to_string(&log).unwrap(), before);
    let retained: String = database
        .query_row(
            "SELECT prepared FROM vcs_operations WHERE id=?1",
            [second_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!retained.contains("agent-c.sock"));
}

#[test]
fn agent_activity_refreshes_a_checkout_at_most_once_per_interval() {
    use std::time::{Duration, Instant};
    let f = Fixture::new();
    let repo = f.repo();
    let id = f.call(&["checkout", "register", repo.to_str().unwrap()])["checkouts"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let agent = f.call(&[
        "register",
        "--harness",
        "fixture",
        "--cwd",
        repo.to_str().unwrap(),
    ]);
    let agent = agent["id"].as_str().unwrap();
    let wait = |dirty: bool| {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let row = f.call(&["checkout", "get", &id]);
            if row["dirty"] == dirty {
                return row;
            }
            assert!(Instant::now() < until, "expected dirty={dirty}: {row}");
            std::thread::sleep(Duration::from_millis(40));
        }
    };
    // Editing a file touches nothing Git watches; only the agent's activity
    // tells Tower to look. Let the interval since registration pass first.
    wait(false);
    std::thread::sleep(Duration::from_millis(2200));
    std::fs::write(repo.join("file"), "edited\n").unwrap();
    f.call(&["tags", agent, "--add", "start"]);
    let first = wait(true)["revision"].as_u64().unwrap();
    // A burst of activity while the tree keeps changing: every state would
    // be a new revision if each event refreshed the checkout.
    for n in 0..8 {
        let text = if n % 2 == 0 { "initial\n" } else { "edited\n" };
        std::fs::write(repo.join("file"), text).unwrap();
        f.call(&["tags", agent, "--add", &format!("t{n}")]);
        std::thread::sleep(Duration::from_millis(60));
    }
    std::fs::write(repo.join("file"), "initial\n").unwrap();
    f.call(&["tags", agent, "--add", "last"]);
    // The refresh at the end of the interval sees the final state.
    let last = wait(false)["revision"].as_u64().unwrap();
    assert!(
        last - first <= 2,
        "{} refreshes changed the checkout",
        last - first
    );
}
