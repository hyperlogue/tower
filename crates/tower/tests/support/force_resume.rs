use super::*;
async fn ready(s: &Server, runtime: &str) -> Value {
    let frame = s
        .until(|v| {
            v["method"] == "session.events"
                && v["params"]["runtime_id"] == runtime
                && v["params"]["events"].as_array().is_some_and(|events| {
                    events
                        .iter()
                        .any(|e| e["kind"] == "runtime_state" && e["payload"]["state"] == "idle")
                })
        })
        .await;
    frame["params"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["payload"]["state"] == "idle")
        .unwrap()["payload"]
        .clone()
}
fn resume(
    f: &Fixture,
    harness: &str,
    protocol: &str,
    old: &str,
    new: &str,
    native: &Value,
) -> Value {
    let mut c = command(
        &format!("resume-{new}"),
        new,
        "resume",
        json!({"harness":harness,"protocol":protocol,"cwd":f.root.path(),"resume_session_id":native,"force":true,"cleanup":[{"runtime_id":old,"generation":format!("generation-{old}")}]}),
    );
    c["session_id"] = json!(format!("logical-{old}"));
    c
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_resume_stops_exact_owners_and_preserves_native_history_and_other_codex_threads() {
    let f = Fixture::new();
    let s = Server::start(&f).await;
    let independent = command(
        "independent",
        "independent",
        "launch",
        json!({"harness":"codex","protocol":"codex-app-server","cwd":f.root.path()}),
    );
    assert!(
        s.call("command.execute", independent)
            .await
            .get("error")
            .is_none()
    );
    let unrelated = ready(&s, "independent").await;
    let unrelated_id = unrelated["tower_session_id"].as_str().unwrap();
    let unrelated_pid = f.rpc("agent.get", json!({"id":unrelated_id}))["pid"].clone();
    for (harness, protocol) in [("codex", "codex-app-server"), ("claude-code", "acp")] {
        let old = format!("old-{harness}");
        let new = format!("new-{harness}");
        let launch = command(
            &format!("launch-{harness}"),
            &old,
            "launch",
            json!({"harness":harness,"protocol":protocol,"cwd":f.root.path()}),
        );
        assert!(
            s.call("command.execute", launch)
                .await
                .get("error")
                .is_none()
        );
        let first = ready(&s, &old).await;
        let native = &first[if protocol == "acp" {
            "acp_session_id"
        } else {
            "native_session_id"
        }];
        let prompt = command(
            &format!("busy-{harness}"),
            &old,
            "prompt",
            json!({"content":[{"type":"text","text":"approve"}]}),
        );
        assert!(
            s.call("command.execute", prompt)
                .await
                .get("error")
                .is_none()
        );
        s.until(|v| {
            v["method"] == "session.request"
                && v["params"]["runtime_id"] == old
                && v["params"]["state"] == "pending"
        })
        .await;
        let force = resume(&f, harness, protocol, &old, &new, native);
        let mut ordinary = force.clone();
        ordinary["command_id"] = json!(format!("ordinary-{harness}"));
        ordinary["payload"].as_object_mut().unwrap().remove("force");
        ordinary["payload"]
            .as_object_mut()
            .unwrap()
            .remove("cleanup");
        assert_eq!(
            s.call("command.execute", ordinary).await["error"]["data"]["code"],
            "resume_unavailable"
        );
        let mut bad = force.clone();
        bad["command_id"] = json!(format!("foreign-{harness}"));
        bad["payload"]["cleanup"] =
            json!([{"runtime_id":"independent","generation":"generation-independent"}]);
        assert_eq!(
            s.call("command.execute", bad).await["error"]["data"]["code"],
            "cleanup_target_conflict"
        );
        let accepted = s.call("command.execute", force.clone()).await;
        assert_eq!(
            accepted["result"]["acceptance"]["level"], "adapter",
            "{accepted}"
        );
        let next = ready(&s, &new).await;
        complete(&s, &format!("resume-{new}")).await;
        assert_eq!(next["tower_session_id"], first["tower_session_id"]);
        assert_eq!(next["native_session_id"], first["native_session_id"]);
        assert_ne!(next["owner_id"], first["owner_id"]);
        s.until(|v| {
            v["method"] == "runtime.lost"
                && v["params"]["runtime_id"] == old
                && v["params"]["termination"] == "confirmed"
        })
        .await;
        assert_eq!(
            s.call("command.execute", force.clone()).await["result"]["dispatch"]["state"],
            "finished"
        );
        let stale = command(
            &format!("stale-{harness}"),
            &old,
            "prompt",
            json!({"content":[{"type":"text","text":"must-not-run"}]}),
        );
        assert_eq!(
            s.call("command.execute", stale).await["error"]["data"]["code"],
            "runtime_fenced"
        );
        let active = f.rpc("agent.get", json!({"id":unrelated_id}));
        assert_eq!(active["pid"], unrelated_pid);
        assert_ne!(active["status"], "exited");
        f.cli(&["kill", next["tower_session_id"].as_str().unwrap()]);
        let log = std::fs::read_to_string(f.root.path().join("dispatches")).unwrap();
        assert!(!log.contains("must-not-run"));
        assert!(
            !log.contains(CONTINUE),
            "Force resume must not continue the stopped turn automatically"
        );
    }
    f.cli(&["kill", unrelated_id]);
}

async fn crash_server(f: &Fixture) {
    let socket = tower_pal::connect(&f.root.path().join("state/tower/run/towerd.sock"))
        .await
        .unwrap();
    let server =
        tower_pal::process_identity(tower_pal::peer(&socket).unwrap().pid.unwrap()).unwrap();
    drop(socket);
    tower_pal::force_terminate_process(server.pid, server.start_time).unwrap();
}
fn controlled_codex(f: &Fixture, phase: &str) {
    let path = f.root.path().join("bin/codex");
    let mut text = std::fs::read_to_string(&path).unwrap();
    let guard = "if (fs.existsSync(path.join(home, 'force-hold'))) { fs.writeFileSync(path.join(home, 'force-blocked'), 'blocked'); return; }";
    text=text.replace("const original = value.method === 'thread/start' ? null : lookup(params.threadId);", "fs.appendFileSync(path.join(home, 'native-opens'), value.method + '\\n'); const original = value.method === 'thread/start' ? null : lookup(params.threadId);");
    text=match phase {
        "cleanup"=>text.replace("case 'thread/goal/get': result =",&format!("case 'thread/goal/get': {guard} result =")),
        "ready"=>text.replace("case 'config/read':",&format!("case 'config/read': {guard}")),
        "dispatched"=>text.replace("const thread = startThread(peer, params, original, value.method === 'thread/fork');",&format!("const thread = startThread(peer, params, original, value.method === 'thread/fork'); if (value.method === 'thread/resume') {{ {guard} }}")),
        _=>panic!(),
    };
    std::fs::write(path, text).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_resume_recovers_cleanup_but_never_replays_a_dispatched_replacement() {
    for phase in ["cleanup", "ready", "dispatched"] {
        let f = Fixture::new();
        controlled_codex(&f, phase);
        let s = Server::start(&f).await;
        assert!(
            s.call(
                "command.execute",
                command(
                    "launch",
                    "old",
                    "launch",
                    json!({"harness":"codex","protocol":"codex-app-server","cwd":f.root.path()})
                )
            )
            .await
            .get("error")
            .is_none()
        );
        let old = ready(&s, "old").await;
        assert!(
            s.call(
                "command.execute",
                command(
                    "busy",
                    "old",
                    "prompt",
                    json!({"content":[{"type":"text","text":"wait"}]})
                )
            )
            .await
            .get("error")
            .is_none()
        );
        s.until(|v| {
            v["method"] == "session.events"
                && v["params"]["events"]
                    .as_array()
                    .is_some_and(|es| es.iter().any(|e| e["kind"] == "user_message"))
        })
        .await;
        std::fs::write(f.root.path().join("force-hold"), "hold").unwrap();
        let force = resume(
            &f,
            "codex",
            "codex-app-server",
            "old",
            "replacement",
            &old["native_session_id"],
        );
        let receipt = s.call("command.execute", force.clone()).await;
        assert_eq!(receipt["result"]["state"], "accepted", "{receipt}");
        tokio::time::timeout(Duration::from_secs(15), async {
            while !f.root.path().join("force-blocked").exists() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        crash_server(&f).await;
        std::fs::remove_file(f.root.path().join("force-hold")).unwrap();
        f.cli(&["daemon", "start"]);
        if phase == "dispatched" {
            s.until(|v| {
                v["method"] == "runtime.lost"
                    && v["params"]["runtime_id"] == "replacement"
                    && v["params"]["termination"] == "unknown"
            })
            .await;
            let status=s.call("command.status",json!({"command_id":"resume-replacement","runtime_id":"replacement","generation":"generation-replacement"})).await;
            assert_eq!(status["result"]["dispatch"]["state"], "unknown", "{status}");
            assert_eq!(
                s.call("command.execute", force).await["result"]["dispatch"]["state"],
                "unknown"
            );
        } else {
            let next = ready(&s, "replacement").await;
            assert_eq!(next["native_session_id"], old["native_session_id"]);
            complete(&s, "resume-replacement").await;
            f.cli(&["kill", next["tower_session_id"].as_str().unwrap()]);
        }
        let opens = std::fs::read_to_string(f.root.path().join("native-opens")).unwrap();
        assert_eq!(opens.lines().count(), 2, "{phase}: {opens}");
        let prompts = std::fs::read_to_string(f.root.path().join("dispatches")).unwrap();
        assert_eq!(prompts.lines().count(), 1, "{phase}: {prompts}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_resume_needs_negotiation_and_expiry_does_not_replace_the_current_binding() {
    for negotiate in [false, true] {
        let f = Fixture::new();
        let s = Server::start_protocols(&f, false, false, negotiate).await;
        assert!(
            s.call(
                "command.execute",
                command(
                    "launch",
                    "old",
                    "launch",
                    json!({"harness":"codex","protocol":"codex-app-server","cwd":f.root.path()})
                )
            )
            .await
            .get("error")
            .is_none()
        );
        let old = ready(&s, "old").await;
        assert!(
            s.call(
                "command.execute",
                command(
                    "materialize",
                    "old",
                    "prompt",
                    json!({"content":[{"type":"text","text":"materialize history"}]})
                )
            )
            .await
            .get("error")
            .is_none()
        );
        complete(&s, "materialize").await;

        let mut c = resume(
            &f,
            "codex",
            "codex-app-server",
            "old",
            "new",
            &old["native_session_id"],
        );
        if negotiate {
            c["expires_at"] = json!("2000-01-01T00:00:00Z");
        }
        assert_eq!(
            s.call("command.execute", c).await["error"]["data"]["code"],
            if negotiate {
                "command_expired"
            } else {
                "unsupported_control"
            }
        );
        let prompt = command(
            "still-live",
            "old",
            "prompt",
            json!({"content":[{"type":"text","text":"unchanged"}]}),
        );
        assert!(
            s.call("command.execute", prompt)
                .await
                .get("error")
                .is_none()
        );
        complete(&s, "still-live").await;
        f.cli(&["kill", old["tower_session_id"].as_str().unwrap()]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_cleanup_keeps_the_old_directory_protected_after_registry_replacement() {
    let f = Fixture::new();
    controlled_codex(&f, "cleanup");
    let base = f.root.path().join("base");
    let old_dir = f.root.path().join("old-worktree");
    let new_dir = f.root.path().join("new-directory");
    std::fs::create_dir(&new_dir).unwrap();
    std::fs::create_dir(f.root.path().join("worktrees")).unwrap();
    let git = |args: &[&str]| {
        let out = f.program("git", args).output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "--initial-branch=main", base.to_str().unwrap()]);
    git(&[
        "-C",
        base.to_str().unwrap(),
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "-m",
        "fixture",
    ]);
    git(&[
        "-C",
        base.to_str().unwrap(),
        "worktree",
        "add",
        "-b",
        "old",
        old_dir.to_str().unwrap(),
    ]);
    let s = Server::start(&f).await;
    assert!(
        s.call(
            "command.execute",
            command(
                "launch",
                "old",
                "launch",
                json!({"harness":"codex","protocol":"codex-app-server","cwd":old_dir})
            )
        )
        .await
        .get("error")
        .is_none()
    );
    let old = ready(&s, "old").await;
    assert!(
        s.call(
            "command.execute",
            command(
                "busy",
                "old",
                "prompt",
                json!({"content":[{"type":"text","text":"wait"}]})
            )
        )
        .await
        .get("error")
        .is_none()
    );
    s.until(|v| {
        v["method"] == "session.events"
            && v["params"]["events"]
                .as_array()
                .is_some_and(|es| es.iter().any(|e| e["kind"] == "user_message"))
    })
    .await;
    std::fs::write(f.root.path().join("force-hold"), "hold").unwrap();
    let mut c = resume(
        &f,
        "codex",
        "codex-app-server",
        "old",
        "replacement",
        &old["native_session_id"],
    );
    c["payload"]["cwd"] = json!(new_dir);
    assert_eq!(
        s.call("command.execute", c).await["result"]["state"],
        "accepted"
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !f.root.path().join("force-blocked").exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    crash_server(&f).await;
    let db =
        rusqlite::Connection::open(f.root.path().join("state/tower/runtime/runtime.db")).unwrap();
    db.execute("UPDATE runtimes SET metadata=json_remove(metadata,'$.pid','$.pid_start_time') WHERE runtime_id='old'",[]).unwrap();
    drop(db);
    std::fs::remove_file(f.root.path().join("force-hold")).unwrap();
    f.cli(&["daemon", "start"]);
    s.until(|v| {
        v["method"] == "command.receipt"
            && v["params"]["command_id"] == "resume-replacement"
            && v["params"]["error"]["code"] == "cleanup_unconfirmed"
    })
    .await;
    let record = f.rpc("agent.get", json!({"id":old["tower_session_id"]}));
    assert_eq!(record["status"], "exited");
    f.cli(&["logout"]);
    f.rpc("checkout.register", json!({"directory":old_dir}));
    let checkouts = f.rpc("checkout.list", json!({}));
    let checkout = checkouts
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["branch"] == "old")
        .unwrap();
    let p = json!({"checkout_id":checkout["id"],"operation":"checkout.move","options":{"name":"moved"}});
    let plan = f.rpc("vcs.prepare", p);
    let out = f
        .command(&[
            "--json",
            "rpc",
            "vcs.execute",
            &json!({"plan_id":plan["plan_id"]}).to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let error: Value = serde_json::from_slice(&out.stderr)
        .or_else(|_| serde_json::from_slice(&out.stdout))
        .unwrap();
    assert_eq!(error["error"]["data"]["code"], "checkout_in_use", "{error}");
    assert!(old_dir.exists());
    assert_eq!(
        std::fs::read_to_string(f.root.path().join("native-opens"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_resume_refuses_ephemeral_history_before_stopping_the_old_execution() {
    let f = Fixture::new();
    let path = f.root.path().join("bin/codex");
    let source = std::fs::read_to_string(&path)
        .unwrap()
        .replace("ephemeral: false", "ephemeral: true");
    std::fs::write(path, source).unwrap();
    let s = Server::start(&f).await;
    assert!(
        s.call(
            "command.execute",
            command(
                "launch",
                "old",
                "launch",
                json!({"harness":"codex","protocol":"codex-app-server","cwd":f.root.path()})
            )
        )
        .await
        .get("error")
        .is_none()
    );
    let old = ready(&s, "old").await;
    assert!(
        s.call(
            "command.execute",
            command(
                "busy",
                "old",
                "prompt",
                json!({"content":[{"type":"text","text":"approve"}]})
            )
        )
        .await
        .get("error")
        .is_none()
    );
    s.until(|v| {
        v["method"] == "session.request"
            && v["params"]["runtime_id"] == "old"
            && v["params"]["state"] == "pending"
    })
    .await;
    let c = resume(
        &f,
        "codex",
        "codex-app-server",
        "old",
        "new",
        &old["native_session_id"],
    );
    let denied = s.call("command.execute", c).await;
    assert_eq!(
        denied["error"]["data"]["code"], "resume_unavailable",
        "{denied}"
    );
    let status = s
        .call(
            "command.status",
            json!({"command_id":"resume-new","runtime_id":"new","generation":"generation-new"}),
        )
        .await;
    assert_eq!(status["result"]["state"], "missing");
    let record = f.rpc("agent.get", json!({"id":old["tower_session_id"]}));
    assert_eq!(record["status"], "waiting_approval");
    assert_eq!(record["caps"]["resume"], false);
    assert!(
        !s.frames
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["method"] == "runtime.lost" && v["params"]["runtime_id"] == "old")
    );
    f.cli(&["kill", old["tower_session_id"].as_str().unwrap()]);
}
