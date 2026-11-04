use super::*;
use crate::runtime::{AgentDetail, AgentSpec, RuntimeState};
use serde_json::{Value, json};
use std::path::Path;

fn spec(harness: Harness, root: &Path) -> AgentSpec {
    AgentSpec {
        harness,
        cwd: root.to_string_lossy().into_owned(),
        args: vec![],
        mode: (harness == Harness::Codex).then(|| "app-server".into()),
        resume: None,
        fork: false,
        native_home: None,
    }
}
#[test]
fn configurations_preserve_native_state_and_limit_owned_changes() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path();
    let env = tower_pal::Environment::from([
        ("HOME".into(), root.to_string_lossy().into_owned()),
        ("PATH".into(), "/usr/bin:/bin".into()),
    ]);
    std::fs::create_dir(root.join(".reasonix")).unwrap();
    std::fs::write(
        root.join(".reasonix/settings.json"),
        r#"{"hooks":{"Stop":[{"command":"user-hook"}]},"user_setting":true}"#,
    )
    .unwrap();
    std::fs::create_dir(root.join(".kimi")).unwrap();
    std::fs::write(
        root.join(".kimi/config.toml"),
        "default_model = 'my-model'\n[[hooks]]\nevent = 'Stop'\ncommand = 'user-hook'\n",
    )
    .unwrap();
    for h in Harness::ALL {
        let dir = root.join(h.id());
        let plan = config::prepare(
            &spec(h, root),
            &dir,
            Path::new("/opt/tower's bin/tower"),
            env.clone(),
        )
        .unwrap();
        assert_eq!(plan.argv[0], h.program());
        assert!(
            !plan
                .argv
                .iter()
                .any(|a| a == "--dangerously-bypass-hook-trust")
        );
        match h {
            Harness::ClaudeCode => {
                let cfg: Value =
                    serde_json::from_slice(&std::fs::read(&plan.argv[2]).unwrap()).unwrap();
                assert!(cfg["hooks"]["PostModelSwitch"].is_array());
                assert!(!cfg.to_string().contains("TOWER_AGENT_TOKEN"));
                assert!(
                    cfg["hooks"]["Stop"][0]["hooks"][0]["command"]
                        .as_str()
                        .unwrap()
                        .contains("'\"'\"'")
                );
            }
            Harness::Codex => {
                assert_eq!(plan.argv, vec!["codex"]);
                assert!(!root.join(".codex/tower.config.toml").exists());
            }
            Harness::Reasonix => {
                let home = Path::new(&plan.env["REASONIX_HOME"]);
                let cfg: Value =
                    serde_json::from_slice(&std::fs::read(home.join("settings.json")).unwrap())
                        .unwrap();
                assert_eq!(cfg["hooks"]["Stop"].as_array().unwrap().len(), 2);
                assert!(home.join("config.toml").is_symlink());
                assert_eq!(
                    plan.env["REASONIX_STATE_HOME"],
                    root.join(".reasonix").to_str().unwrap()
                );
                assert_ne!(plan.env["REASONIX_CACHE_HOME"], plan.env["REASONIX_HOME"]);
            }
            Harness::Kimi => {
                let home = Path::new(&plan.env["KIMI_CODE_HOME"]);
                let cfg: toml::Table = std::fs::read_to_string(home.join("config.toml"))
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(cfg["default_model"].as_str(), Some("my-model"));
                assert!(
                    cfg["hooks"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|h| h.get("matcher").is_none())
                );
                std::fs::write(home.join("session_index.jsonl"), "native history").unwrap();
                assert_eq!(
                    std::fs::read_to_string(root.join(".kimi/session_index.jsonl")).unwrap(),
                    "native history"
                );
            }
            Harness::Grok => {
                let cfg: Value = serde_json::from_slice(
                    &std::fs::read(Path::new(&plan.env["GROK_HOME"]).join("hooks/tower.json"))
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    cfg["hooks"]["Notification"][0]["matcher"],
                    "permission_prompt"
                );
            }
            Harness::Opencode => {
                assert!(
                    Path::new(&plan.env["OPENCODE_CONFIG_DIR"])
                        .join("plugins/tower.ts")
                        .is_file()
                );
            }
            Harness::Pi | Harness::Omp => {
                assert_eq!(plan.argv[1], "-e");
                assert!(Path::new(&plan.argv[2]).is_file());
            }
            Harness::Antigravity => {
                assert_eq!(plan.argv[1], "--gemini_dir");
                let cfg: Value = serde_json::from_slice(
                    &std::fs::read(Path::new(&plan.argv[2]).join("config/hooks.json")).unwrap(),
                )
                .unwrap();
                assert!(
                    cfg["tower"]["Stop"][0]["command"]
                        .as_str()
                        .unwrap()
                        .ends_with("printf '{}'")
                );
            }
        }
    }
    assert!(
        !std::fs::read_to_string(root.join(".reasonix/settings.json"))
            .unwrap()
            .contains("tower")
    );
    assert!(
        !std::fs::read_to_string(root.join(".kimi/config.toml"))
            .unwrap()
            .contains("tower")
    );
}
#[test]
fn codex_rejects_native_mode_and_endpoint_overrides_without_editing_native_config() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path();
    std::fs::create_dir(root.join(".codex")).unwrap();
    std::fs::write(root.join(".codex/config.toml"), "model='mine'").unwrap();
    let env = tower_pal::Environment::from([("HOME".into(), root.to_string_lossy().into_owned())]);
    config::prepare(
        &spec(Harness::Codex, root),
        &root.join("cfg"),
        Path::new("/opt/tower"),
        env,
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join(".codex/config.toml")).unwrap(),
        "model='mine'"
    );
    let mut native = spec(Harness::Codex, root);
    native.mode = Some("native".into());
    assert!(config::validate(&native).is_err());
    for arg in ["--profile=other", "-pother", "--remote=unix:///tmp/other"] {
        let mut selected = spec(Harness::Codex, root);
        selected.args.push(arg.into());
        assert!(config::validate(&selected).is_err());
    }
}
fn state() -> RuntimeState {
    RuntimeState {
        id: "tower-session".into(),
        generation: "one".into(),
        harness: Harness::ClaudeCode,
        status: "starting".into(),
        cwd: "/work".into(),
        native: None,
        display_name: None,
        detail: AgentDetail::default(),
        launcher_pid: 1,
        launcher_start_time: 1,
        boot_id: "boot".into(),
        agent_pid: Some(2),
        agent_start_time: Some(2),
        seq: 0,
        updated_at: 0,
        stop_requested: false,
        exit_code: None,
        private: Default::default(),
    }
}
#[test]
fn native_events_preserve_root_identity_waits_and_background_work() {
    let mut s = state();
    let apply = |s: &mut RuntimeState, h, event, payload: Value| {
        events::apply(
            s,
            events::parse(h, event, &serde_json::to_vec(&payload).unwrap()).unwrap(),
            42,
        )
    };
    apply(
        &mut s,
        Harness::ClaudeCode,
        "SessionStart",
        json!({"session_id":"root"}),
    );
    assert_eq!(s.status, "idle");
    apply(
        &mut s,
        Harness::ClaudeCode,
        "SessionStart",
        json!({"session_id":"child","subagent_type":"Explore"}),
    );
    assert_eq!(s.native.as_ref().unwrap().id, "root");
    apply(
        &mut s,
        Harness::ClaudeCode,
        "PermissionRequest",
        json!({"session_id":"root","tool_name":"Bash","tool_use_id":"tool1","prompt":"secret-body"}),
    );
    assert_eq!(s.status, "waiting_approval");
    apply(
        &mut s,
        Harness::ClaudeCode,
        "PostToolUse",
        json!({"session_id":"root","tool_name":"Read","tool_use_id":"tool2"}),
    );
    assert_eq!(s.status, "waiting_approval");
    apply(
        &mut s,
        Harness::ClaudeCode,
        "PostToolUse",
        json!({"session_id":"root","tool_name":"Bash","tool_use_id":"tool1"}),
    );
    assert_eq!(s.status, "working");
    apply(
        &mut s,
        Harness::Omp,
        "Stop",
        json!({"session_id":"root","will_continue":true}),
    );
    assert_eq!(s.status, "working");
    apply(
        &mut s,
        Harness::Omp,
        "Stop",
        json!({"session_id":"root","background_jobs":1}),
    );
    assert_eq!(s.status, "working");
    apply(
        &mut s,
        Harness::Omp,
        "Stop",
        json!({"session_id":"root","background_jobs":0}),
    );
    assert_eq!(s.status, "idle");
    assert!(!serde_json::to_string(&s).unwrap().contains("secret-body"));
    let event = events::parse(
        Harness::Codex,
        "SessionStart",
        &serde_json::to_vec(&json!({"session_id":"x".repeat(513)})).unwrap(),
    )
    .unwrap();
    assert!(event.native_id.is_none());
}

#[test]
fn generated_extensions_execute_native_callbacks_in_order_without_prompt_data() {
    for h in [Harness::Pi, Harness::Omp, Harness::Opencode] {
        let root = tempfile::tempdir().unwrap();
        let receiver = root.path().join("receive");
        std::fs::write(
            &receiver,
            "#!/bin/sh\n{ printf '%s\\t' \"$3\"; cat; printf '\\n'; } >> \"$TOWER_TEST_EVENTS\"\n",
        )
        .unwrap();
        tower_pal::make_executable(&receiver).unwrap();
        let module = root.path().join("extension.mjs");
        let source = if h == Harness::Opencode {
            extensions::opencode(&receiver)
        } else {
            extensions::extension(&receiver, h)
        };
        std::fs::write(&module, source).unwrap();
        let module = serde_json::to_string(&format!("file://{}", module.display())).unwrap();
        let script = if h == Harness::Opencode {
            format!(
                r#"
const {{Tower}} = await import({module});
const plugin=await Tower({{directory:'/work'}});
const first=plugin.event({{event:{{type:'session.created',properties:{{info:{{id:'first',directory:'/work',title:'A'}}}}}}}});
const second=plugin['chat.message']({{sessionID:'first',prompt:'SECRET-PROMPT'}},{{parts:[{{text:'SECRET-PROMPT'}}]}});
if (await first !== undefined || await second !== undefined) throw Error('callback changed host result');
await plugin.event({{event:{{type:'session.updated',properties:{{info:{{id:'child',parentID:'first',directory:'/work'}}}}}}}});
"#
            )
        } else {
            format!(
                r#"
const {{default:install}}=await import({module});
const handlers=new Map();
install({{on:(key,callback)=>handlers.set(key,callback),getSessionName:()=> 'name'}});
let id='first';
const ctx={{cwd:'/work',sessionManager:{{getSessionId:()=>id,getSessionFile:()=>'/sessions/first.jsonl'}},getContextUsage:()=>({{tokens:42}}),getAsyncJobSnapshot:()=>({{running:[{{secret:'SECRET-PROMPT'}}]}}),model:{{provider:'fixture',id:'model'}}}};
const first=handlers.get('session_start')({{prompt:'SECRET-PROMPT'}},ctx);
id='second';
const second=handlers.get('tool_execution_start')({{toolName:'Read',prompt:'SECRET-PROMPT'}},ctx);
if (await first !== undefined || await second !== undefined) throw Error('callback changed host result');
"#
            )
        };
        let events = root.path().join("events");
        let output = std::process::Command::new("node")
            .args(["--input-type=module", "--eval", &script])
            .env_clear()
            .env("HOME", root.path())
            .env("PATH", std::env::var("PATH").unwrap())
            .env("TOWER_TEST_EVENTS", &events)
            .output()
            .expect("Node is required for generated extension tests; use nix develop");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let captured = std::fs::read_to_string(events).unwrap();
        assert!(!captured.contains("SECRET-PROMPT"));
        let records = captured
            .lines()
            .map(|line| {
                let (name, body) = line.split_once('\t').unwrap();
                (name, serde_json::from_str::<Value>(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(records[0].0, "SessionStart");
        assert_eq!(records[0].1["session_id"], "first");
        if h == Harness::Opencode {
            assert_eq!(records[2].1["is_child"], true);
        } else {
            assert_eq!(records[1].1["session_id"], "second");
            assert_eq!(records[0].1["context_tokens"], 42);
        }
    }
}

#[test]
fn claude_native_activity_corrects_queued_turns_but_preserves_approval_waits() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join(".claude");
    std::fs::create_dir_all(home.join("sessions")).unwrap();
    let mut s = state();
    let env =
        tower_pal::Environment::from([("HOME".into(), root.path().to_string_lossy().into_owned())]);
    let mut activity = activity::Activity::new(Harness::ClaudeCode, &env);
    let path = home.join("sessions/2.json");
    std::fs::write(
        &path,
        r#"{"sessionId":"root","status":"busy","name":"chosen title"}"#,
    )
    .unwrap();
    assert!(activity.refresh(&mut s, 100));
    assert_eq!(s.status, "working");
    assert_eq!(s.display_name.as_deref(), Some("chosen title"));
    events::apply(
        &mut s,
        events::parse(Harness::ClaudeCode, "Stop", b"{}").unwrap(),
        101,
    );
    assert_eq!(s.status, "idle");
    std::thread::sleep(std::time::Duration::from_millis(510));
    assert!(activity.refresh(&mut s, 102));
    assert_eq!(s.status, "working");
    s.status = "waiting_approval".into();
    std::fs::write(&path, r#"{"sessionId":"root","status":"idle"}"#).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(510));
    activity.refresh(&mut s, 103);
    assert_eq!(s.status, "waiting_approval");
    s.status = "working".into();
    std::thread::sleep(std::time::Duration::from_millis(510));
    assert!(activity.refresh(&mut s, 104));
    assert_eq!(s.status, "idle");
}

#[test]
fn resume_preserves_runtime_options_without_replaying_startup_actions() {
    let args = [
        "--model",
        "chosen",
        "--worktree",
        "new-tree",
        "-s",
        "original-session",
        "--fork",
        "prompt",
        "--",
        "--model",
        "prompt-text",
    ]
    .map(str::to_owned);
    assert_eq!(
        config::resume_options(Harness::Opencode, &args).unwrap(),
        vec!["--model", "chosen"]
    );
    let args = [
        "--model",
        "chosen",
        "-s",
        "read-only",
        "resume",
        "original",
        "initial prompt",
    ]
    .map(str::to_owned);
    assert_eq!(
        config::resume_options(Harness::Codex, &args).unwrap(),
        vec!["--model", "chosen", "-s", "read-only"]
    );
}

#[test]
fn restart_keeps_permission_constraints_and_rejects_unknown_options() {
    let args = [
        "--permission-mode",
        "plan",
        "--disallowedTools",
        "Bash",
        "Write",
        "--settings",
        "/config/strict.json",
        "--",
        "old prompt",
    ]
    .map(str::to_owned);
    assert_eq!(
        config::resume_options(Harness::ClaudeCode, &args).unwrap(),
        args[..7]
    );
    assert!(
        config::resume_options(
            Harness::ClaudeCode,
            &["--future-permission".into(), "restrictive".into()]
        )
        .is_err()
    );
    assert_eq!(
        config::resume_options(Harness::Pi, &["-c".into(), "old prompt".into()]).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn native_store_identity_is_stable_when_its_directory_is_created() {
    let root = tempfile::tempdir().unwrap();
    let env =
        tower_pal::Environment::from([("HOME".into(), root.path().to_string_lossy().into_owned())]);
    let before = inventory::namespace(Harness::Codex, &env);
    std::fs::create_dir_all(root.path().join(".codex/sessions")).unwrap();
    assert_eq!(before, inventory::namespace(Harness::Codex, &env));
}
