use super::*;
use tower_pool_proto::{self as pool, Reply, Request, Size};

fn install_pi(f: &Fixture) {
    let node = tower_pal::find_program("node", &tower_pal::environment()).unwrap();
    let script = format!(
        r#"#!{}
const fs = require('node:fs');
fs.appendFileSync(process.env.HOME + '/pi-starts', 'start\n');
console.log('TERMINAL-READY');
process.stdin.on('data', b => console.log('echo:' + b));
"#,
        node.display()
    );
    let path = f.root.path().join("bin/pi");
    std::fs::write(&path, script).unwrap();
    tower_pal::make_executable(&path).unwrap();
}
fn consent(f: &Fixture) {
    let state = f.root.path().join("state/tower");
    let socket = std::fs::read_to_string(state.join("allgather-server")).unwrap();
    let origin = socket
        .trim_end_matches("/tower")
        .replacen("ws:", "http:", 1);
    tower_pal::write_private(&state.join("allgather-consent.json"),
        json!({"origin":origin,"machine_id":"fixture-machine","scopes":["read","terminal:control"]}).to_string().as_bytes()).unwrap();
}
async fn launch(f: &Fixture, s: &Server, name: &str, harness: &str) -> (Value, String) {
    let command = command(
        name,
        name,
        "launch",
        json!({"harness":harness,"protocol":"terminal","cwd":f.root.path()}),
    );
    let receipt = s.call("command.execute", command.clone()).await;
    assert_eq!(
        receipt["result"]["acceptance"]["level"], "adapter",
        "{receipt}"
    );
    let event = s
        .until(|v| {
            v["method"] == "session.events"
                && v["params"]["runtime_id"] == name
                && v["params"]["events"].as_array().is_some_and(|es| {
                    es.iter()
                        .any(|e| e["kind"] == "runtime_state" && e["payload"]["state"] == "running")
                })
        })
        .await;
    let id = event["params"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["payload"]["state"] == "running")
        .unwrap()["payload"]["tower_session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    complete(s, name).await;
    (command, id)
}
/// Crashes the server, which owns runtimes, and starts a new one.
async fn kill_owner(f: &Fixture) {
    let socket = tower_pal::connect(&f.root.path().join("state/tower/run/towerd.sock"))
        .await
        .unwrap();
    let identity =
        tower_pal::process_identity(tower_pal::peer(&socket).unwrap().pid.unwrap()).unwrap();
    drop(socket);
    tower_pal::force_terminate_process(identity.pid, identity.start_time).unwrap();
    f.cli(&["daemon", "start"]);
}
fn starts(f: &Fixture) -> usize {
    std::fs::read_to_string(f.root.path().join("pi-starts"))
        .unwrap()
        .lines()
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_launch_attaches_and_recovers_without_replay_or_structured_controls() {
    let f = Fixture::new();
    install_pi(&f);
    let s = Server::start_modes(&f, false, true).await;
    let denied = command(
        "no-consent",
        "no-consent",
        "launch",
        json!({"protocol":"terminal","harness":"pi","cwd":f.root.path()}),
    );
    assert_eq!(
        s.call("command.execute", denied).await["error"]["data"]["code"],
        "denied"
    );
    consent(&f);
    let (launch_command, id) = launch(&f, &s, "terminal-pi", "pi").await;
    let info = f.rpc("session.attach_info", json!({"id":id}));
    let mut stream =
        tower_pal::connect(std::path::Path::new(info["pool_socket"].as_str().unwrap()))
            .await
            .unwrap();
    pool::hello(&mut stream).await.unwrap();
    pool::write(
        &mut stream,
        &Request::Attach {
            name: info["pool_session"].as_str().unwrap().into(),
            size: Size::default(),
            clipboard: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        pool::read(&mut stream)
            .await
            .unwrap()
            .json::<Reply>()
            .unwrap(),
        Reply::Attached { .. }
    ));
    pool::data(&mut stream, b"terminal-input\n").await.unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let snapshot = f.rpc("session.snapshot", json!({"id":id}));
            if snapshot["buffer"]
                .as_str()
                .is_some_and(|v| v.contains("echo:terminal-input"))
            {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(snapshot.to_string().contains("TERMINAL-READY"));
    let pid = f.rpc("agent.get", json!({"id":id}))["pid"].clone();
    for kind in ["prompt", "reply", "configure", "resume"] {
        let unsupported = s
            .call(
                "command.execute",
                command(kind, "terminal-pi", kind, json!({})),
            )
            .await;
        assert_eq!(
            unsupported["error"]["data"]["code"], "unsupported_control",
            "{unsupported}"
        );
        let status = s.call("command.status", json!({"command_id":kind,"runtime_id":"terminal-pi","generation":"generation-terminal-pi"})).await;
        assert_eq!(status["result"]["state"], "missing");
    }
    let structured = s
        .call(
            "command.execute",
            command(
                "structured",
                "structured",
                "launch",
                json!({"protocol":"acp","harness":"claude-code","cwd":f.root.path()}),
            ),
        )
        .await;
    assert_eq!(structured["error"]["data"]["code"], "unsupported_control");
    let mut altered = launch_command.clone();
    altered["payload"]["cwd"] = json!("/tmp");
    assert_eq!(
        s.call("command.execute", altered).await["error"]["data"]["code"],
        "command_conflict"
    );
    assert_eq!(
        s.call("command.execute", launch_command.clone()).await["result"]["dispatch"]["state"],
        "finished"
    );
    f.cli(&["daemon", "stop"]);
    f.cli(&["daemon", "start"]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(f.rpc("agent.get", json!({"id":id}))["pid"], pid);
    kill_owner(&f).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        s.call("command.execute", launch_command.clone()).await["result"]["dispatch"]["state"],
        "finished"
    );
    assert_eq!(f.rpc("agent.get", json!({"id":id}))["pid"], pid);
    assert_eq!(starts(&f), 1);
    assert!(
        !f.command(&["restart", &id])
            .output()
            .unwrap()
            .status
            .success()
    );
    let cancel = command("stop", "terminal-pi", "cancel", json!({}));
    assert_eq!(
        s.call("command.execute", cancel.clone()).await["result"]["state"],
        "accepted"
    );
    let stopped = s
        .until(|v| {
            v["method"] == "command.receipt"
                && v["params"]["command_id"] == "stop"
                && v["params"]["result"]["outcome"] == "stopped"
        })
        .await;
    assert_eq!(stopped["params"]["dispatch"]["state"], "finished");
    assert_eq!(
        s.call("command.execute", cancel).await["result"]["result"]["outcome"],
        "stopped"
    );
    assert_eq!(
        s.call("command.execute", launch_command).await["result"]["dispatch"]["state"],
        "finished"
    );
    assert_eq!(starts(&f), 1);
    for v in s
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|v| v["method"] == "session.events")
    {
        for event in v["params"]["events"].as_array().unwrap() {
            assert_eq!(event["kind"], "runtime_state");
            assert!(event["payload"].get("acp_session_id").is_none());
        }
    }
    drop(stream);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_pool_loss_fences_instead_of_starting_a_replacement() {
    let f = Fixture::new();
    install_pi(&f);
    let s = Server::start_modes(&f, false, true).await;
    consent(&f);
    let (launch_command, id) = launch(&f, &s, "pool-loss", "pi").await;
    let socket = tower_pal::connect(&f.root.path().join("state/tower/run/pool.sock"))
        .await
        .unwrap();
    let identity =
        tower_pal::process_identity(tower_pal::peer(&socket).unwrap().pid.unwrap()).unwrap();
    drop(socket);
    tower_pal::force_terminate_process(identity.pid, identity.start_time).unwrap();
    s.until(|v| v["method"] == "runtime.lost" && v["params"]["runtime_id"] == "pool-loss")
        .await;
    f.cli(&["daemon", "stop"]);
    f.cli(&["daemon", "start"]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        s.call("command.execute", launch_command).await["result"]["state"],
        "accepted"
    );
    assert!(
        !f.command(&["restart", &id])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(starts(&f), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_codex_stop_preserves_the_other_shared_host_thread() {
    let f = Fixture::new();
    let s = Server::start_modes(&f, false, true).await;
    consent(&f);
    let (_, first) = launch(&f, &s, "codex-first", "codex").await;
    let (_, second) = launch(&f, &s, "codex-second", "codex").await;
    let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let a = f.rpc("agent.get", json!({"id":first}));
            let b = f.rpc("agent.get", json!({"id":second}));
            if a["native_session_id"].is_string() && b["native_session_id"].is_string() {
                break (a, b);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(a["pid"], b["pid"]);
    assert_ne!(a["native_session_id"], b["native_session_id"]);
    assert_eq!(a["mode"], "app-server");
    let cancel = command("codex-stop", "codex-first", "cancel", json!({}));
    assert_eq!(
        s.call("command.execute", cancel).await["result"]["state"],
        "accepted"
    );
    s.until(|v| {
        v["method"] == "command.receipt"
            && v["params"]["command_id"] == "codex-stop"
            && v["params"]["result"]["outcome"] == "stopped"
    })
    .await;
    let b_after = f.rpc("agent.get", json!({"id":second}));
    assert_eq!(b_after["pid"], b["pid"]);
    assert_eq!(b_after["native_session_id"], b["native_session_id"]);
    assert_eq!(b_after["caps"]["attach"], true);
    f.cli(&["kill", &second]);
}
