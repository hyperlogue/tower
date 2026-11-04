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
        let mut c = Command::new(env!("CARGO_BIN_EXE_tower"));
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
        let mut result = self.call(&[
            "vcs",
            "execute",
            plan["plan_id"].as_str().unwrap(),
            "--wait",
        ]);
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
            "open",
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
