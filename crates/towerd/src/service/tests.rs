use super::*;
use tower_store::Store;

fn setup() -> (tempfile::TempDir, Arc<Store>, Service) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let paths = Paths {
        run: state.join("run"),
        socket: state.join("run/towerd.sock"),
        database: state.join("tower.db"),
        tokens: state.join("tokens"),
        logs: state.join("logs"),
        config_home: dir.path().join("config"),
        home: dir.path().join("home"),
        state,
    };
    paths.prepare().unwrap();
    tower_pal::private_file(&paths.database, true).unwrap();
    let store = Arc::new(Store::open(&paths.database).unwrap());
    let service = Service::new(
        paths,
        store.clone(),
        "test",
        tower_sessions::Backends::default(),
    )
    .unwrap();
    (dir, store, service)
}
fn call(
    s: &Service,
    method: &str,
    p: serde_json::Value,
    context: CallerContext,
) -> std::result::Result<serde_json::Value, RpcError> {
    let r = Request {
        jsonrpc: "2.0".into(),
        id: Some(RequestId::String("test".into())),
        method: method.into(),
        params: serde_json::from_value(p).unwrap(),
        context,
    };
    s.dispatch(
        &r,
        Peer {
            uid: tower_pal::uid(),
            pid: None,
        },
    )
    .map(|v| serde_json::to_value(v).unwrap())
}
fn human() -> CallerContext {
    CallerContext::default()
}

#[tokio::test]
async fn cancelled_waiter_does_not_release_an_inflight_mutation() {
    let (_dir, _store, service) = setup();
    let worker = crate::worker::ServiceWorker::new(service);
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let task_worker = worker.clone();
    let caller = tokio::spawn(async move {
        task_worker
            .execute(move |_| {
                let _ = started.send(());
                let _ = blocked.recv();
            })
            .await
    });
    entered.await.unwrap();
    caller.abort();
    let _ = caller.await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), worker.drain())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), worker.drain())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn registration_cannot_acquire_another_sessions_credential_or_create_an_alias() {
    let (_dir, store, s) = setup();
    let a = call(
        &s,
        "agent.register",
        serde_json::json!({"harness":"codex","cwd":"~/a"}),
        human(),
    )
    .unwrap();
    let b = call(&s, "agent.register", serde_json::json!({"harness":"codex","cwd":"~/b","pid":tower_pal::pid(),"native_session_id":"b"}), human()).unwrap();
    let before = store.agent(b["id"].as_str().unwrap()).unwrap().record;
    let context = CallerContext {
        agent_id: Some(a["id"].as_str().unwrap().into()),
        agent_token: Some(Secret(a["token"].as_str().unwrap().into())),
        ..Default::default()
    };
    for params in [
        serde_json::json!({"harness":"codex","cwd":"~/changed","display_name":"changed","pid":tower_pal::pid()}),
        serde_json::json!({"harness":"codex","cwd":"~/changed"}),
    ] {
        assert_eq!(
            call(&s, "agent.register", params, context.clone())
                .unwrap_err()
                .data
                .code,
            "denied"
        );
    }
    assert_eq!(store.agent(&before.id).unwrap().record, before);
    assert_eq!(store.agents(&AgentFilter::default()).unwrap().len(), 2);
    assert_eq!(std::fs::read_dir(&s.paths.tokens).unwrap().count(), 2);
}

#[test]
fn failed_reregistration_preserves_metadata_sequence_and_credential_files() {
    let (_dir, store, s) = setup();
    let (id, token) = register(&s, "thread");
    call(
        &s,
        "agent.update",
        serde_json::json!({"id":id,"token":token.0,"seq":8,"status":"working"}),
        human(),
    )
    .unwrap();
    let before = store.agent(&id).unwrap();
    let path = s.paths.token_path(&id).unwrap();
    tower_pal::remove_private(&path).unwrap();
    for corrupt in [false, true] {
        if corrupt {
            tower_pal::write_private(&path, b"wrong-credential").unwrap();
        }
        let error = call(&s, "agent.register", serde_json::json!({"harness":"codex","cwd":"~/changed","native_session_id":"thread","display_name":"changed"}), human()).unwrap_err();
        assert_eq!(error.data.code, "internal_error");
        let after = store.agent(&id).unwrap();
        assert_eq!(after.record, before.record);
        assert_eq!(after.seq, before.seq);
        assert_eq!(after.token_hash, before.token_hash);
        assert_eq!(
            std::fs::read_dir(&s.paths.tokens).unwrap().count(),
            usize::from(corrupt)
        );
    }
}

#[test]
fn reregistration_retains_only_a_verified_live_process_binding() {
    let (_dir, store, s) = setup();
    let original = call(&s, "agent.register", serde_json::json!({"harness":"codex","cwd":"~/work","native_session_id":"thread","pid":tower_pal::pid()}), human()).unwrap();
    let id = original["id"].as_str().unwrap();
    let params = serde_json::json!({"harness":"codex","cwd":"~/work","native_session_id":"thread"});
    assert_eq!(
        call(&s, "agent.register", params.clone(), human()).unwrap(),
        original
    );
    assert_eq!(store.agent(id).unwrap().record.pid, Some(tower_pal::pid()));
    // Model a reused PID: the numeric PID still exists but the recorded start differs.
    let mut stale = store.agent(id).unwrap().record;
    stale.pid_start_time = stale.pid_start_time.map(|v| v + 1);
    let digest = store.agent(id).unwrap().token_hash;
    store.register(stale, &digest, id).unwrap();
    assert_eq!(
        call(&s, "agent.register", params, human()).unwrap(),
        original
    );
    let current = store.agent(id).unwrap();
    assert_eq!(current.record.pid, None);
    assert_eq!(current.record.pid_start_time, None);
    call(
        &s,
        "agent.heartbeat",
        serde_json::json!({"id":id,"token":original["token"]}),
        human(),
    )
    .unwrap();
    s.maintain().unwrap();
    assert_eq!(store.agent(id).unwrap().record.status, "unknown");
}

#[test]
fn directory_filters_normalize_like_registered_paths() {
    let (_dir, _store, s) = setup();
    let (id, _) = register(&s, "thread");
    for prefix in [
        "~/work".to_owned(),
        "/".to_owned(),
        s.paths
            .home
            .parent()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        s.paths.home.join("work").to_string_lossy().into_owned(),
        s.paths
            .home
            .join("unused/../work/")
            .to_string_lossy()
            .into_owned(),
    ] {
        let result = call(
            &s,
            "agent.list",
            serde_json::json!({"filter":{"cwd_prefix":prefix}}),
            human(),
        )
        .unwrap();
        assert_eq!(result[0]["id"], id);
    }
    assert_eq!(
        call(
            &s,
            "agent.list",
            serde_json::json!({"filter":{"cwd_prefix":"~/wor"}}),
            human()
        )
        .unwrap(),
        serde_json::json!([])
    );
}

#[test]
fn named_token_attribution_is_canonical_for_all_rule_mutations() {
    let (_dir, _store, s) = setup();
    let token = call(
        &s,
        "token.issue",
        serde_json::json!({"name":"human","scope":["admin"]}),
        human(),
    )
    .unwrap();
    let context = CallerContext {
        bearer_token: Some(Secret(token["token"].as_str().unwrap().into())),
        ..Default::default()
    };
    assert_eq!(
        call(
            &s,
            "identity.whoami",
            serde_json::json!({}),
            context.clone()
        )
        .unwrap()["id"],
        "token:human"
    );
    let rule = serde_json::json!({"id":"rule","from":"human","to":"*","action":"allow","created_by":"","created_at":0});
    assert_eq!(
        call(
            &s,
            "rules.put",
            serde_json::json!({"rule":rule}),
            context.clone()
        )
        .unwrap()["created_by"],
        "token:human"
    );
    call(
        &s,
        "rules.replace",
        serde_json::json!({"rules":[rule]}),
        context,
    )
    .unwrap();
    assert_eq!(
        call(&s, "rules.list", serde_json::json!({}), human()).unwrap()["local_rules"][0]["created_by"],
        "token:human"
    );
}

#[test]
fn indexed_authentication_preserves_identity_and_rejects_mismatches() {
    let (_dir, _store, s) = setup();
    let (a, token_a) = register(&s, "a");
    let (b, _) = register(&s, "b");
    for agent_id in [None, Some(a.clone())] {
        let context = CallerContext {
            agent_id,
            agent_token: Some(token_a.clone()),
            ..Default::default()
        };
        assert_eq!(
            call(&s, "identity.whoami", serde_json::json!({}), context).unwrap()["id"],
            a
        );
    }
    for id in [b, "missing".into()] {
        let context = CallerContext {
            agent_id: Some(id),
            agent_token: Some(token_a.clone()),
            ..Default::default()
        };
        assert_eq!(
            call(&s, "identity.whoami", serde_json::json!({}), context)
                .unwrap_err()
                .data
                .code,
            "unauthorized"
        );
    }
    let weak = CallerContext {
        agent_id: Some(a),
        ..Default::default()
    };
    assert_eq!(
        call(&s, "identity.whoami", serde_json::json!({}), weak).unwrap()["trust"],
        "weak"
    );
}

#[test]
fn expiry_rejects_unicode_zero_and_overflow_without_panicking() {
    for input in ["💥", "30日", "0d", "18446744073709551615d", "-1h", "", "2"] {
        assert!(parse_duration(input).is_err(), "{input}");
    }
    assert_eq!(parse_duration("30d").unwrap(), 30 * 86_400_000);
}
fn register(s: &Service, native: &str) -> (String, Secret) {
    let r = call(
        s,
        "agent.register",
        serde_json::json!({"harness":"codex","cwd":"~/work","native_session_id":native}),
        human(),
    )
    .unwrap();
    (
        r["id"].as_str().unwrap().into(),
        Secret(r["token"].as_str().unwrap().into()),
    )
}
#[test]
fn bad_credentials_never_fall_back_to_human_and_agents_cannot_manage_rules() {
    let (_dir, _store, s) = setup();
    let (id, token) = register(&s, "thread");
    let agent = CallerContext {
        agent_id: Some(id.clone()),
        agent_token: Some(token.clone()),
        ..Default::default()
    };
    let p = serde_json::json!({"name":"admin","scope":["admin"]});
    assert_eq!(
        call(&s, "token.issue", p.clone(), agent)
            .unwrap_err()
            .data
            .code,
        "denied"
    );
    let invalid = CallerContext {
        agent_token: Some(Secret("invalid".into())),
        ..Default::default()
    };
    assert_eq!(
        call(&s, "token.issue", p.clone(), invalid)
            .unwrap_err()
            .data
            .code,
        "unauthorized"
    );
    let weak = CallerContext {
        agent_id: Some(id),
        ..Default::default()
    };
    assert_eq!(
        call(&s, "token.issue", p.clone(), weak)
            .unwrap_err()
            .data
            .code,
        "denied"
    );
    let unregistered = CallerContext {
        native_session_id: Some("unseen".into()),
        harness: Some("codex".into()),
        ..Default::default()
    };
    assert_eq!(
        call(&s, "token.issue", p, unregistered)
            .unwrap_err()
            .data
            .code,
        "denied"
    );
}
#[test]
fn revoked_and_expired_bearers_fail_on_the_next_request() {
    let (_dir, store, s) = setup();
    let issued = call(
        &s,
        "token.issue",
        serde_json::json!({"name":"reader","scope":["read"],"expires":"30d"}),
        human(),
    )
    .unwrap();
    let context = CallerContext {
        bearer_token: Some(Secret(issued["token"].as_str().unwrap().into())),
        ..Default::default()
    };
    assert!(call(&s, "agent.list", serde_json::json!({}), context.clone()).is_ok());
    assert_eq!(
        call(
            &s,
            "rules.replace",
            serde_json::json!({"rules":[]}),
            context.clone()
        )
        .unwrap_err()
        .data
        .code,
        "denied"
    );
    call(
        &s,
        "token.revoke",
        serde_json::json!({"name":"reader"}),
        human(),
    )
    .unwrap();
    assert_eq!(
        call(&s, "agent.list", serde_json::json!({}), context)
            .unwrap_err()
            .data
            .code,
        "unauthorized"
    );
    let expired = Secret("expired-test-token".into());
    store
        .issue_token(
            TokenInfo {
                name: "expired".into(),
                scope: vec!["admin".into()],
                created_at: 1,
                expires_at: Some(2),
            },
            &hash(&expired),
        )
        .unwrap();
    assert_eq!(
        call(
            &s,
            "token.list",
            serde_json::json!({}),
            CallerContext {
                bearer_token: Some(expired),
                ..Default::default()
            }
        )
        .unwrap_err()
        .data
        .code,
        "unauthorized"
    );
}
#[test]
fn duplicate_registration_preserves_id_and_secret_and_projection_excludes_credentials() {
    let (_dir, store, s) = setup();
    let (id, token) = register(&s, "thread");
    let (again, second) = register(&s, "thread");
    assert_eq!(id, again);
    assert_eq!(token.0, second.0);
    let info = call(&s, "agent.get", serde_json::json!({"id":id}), human()).unwrap();
    let json = info.to_string();
    assert!(!json.contains(&token.0));
    assert!(info.get("token_hash").is_none());
    assert!(info.get("delivery").is_none());
    assert!(info.get("resume_spec").is_none());
    assert_eq!(store.agents(&AgentFilter::default()).unwrap().len(), 1);
}
#[test]
fn update_auth_order_and_prompt_redaction() {
    let (_dir, store, s) = setup();
    let (id, token) = register(&s, "thread");
    let bad = call(
        &s,
        "agent.update",
        serde_json::json!({"id":id,"token":"wrong","seq":1,"status":"idle"}),
        human(),
    )
    .unwrap_err();
    assert_eq!(bad.data.code, "unauthorized");
    let update = |seq, state| serde_json::json!({"id":id,"token":token.0,"seq":seq,"status":state,"detail":{"last_tool":"Bash","last_prompt":"DO_NOT_PERSIST_PROMPTS"}});
    assert_eq!(
        call(&s, "agent.update", update(2, "working"), human()).unwrap()["applied"],
        true
    );
    assert_eq!(
        call(&s, "agent.update", update(1, "idle"), human()).unwrap()["applied"],
        false
    );
    let record = store.agent(&id).unwrap().record;
    assert_eq!(record.status, "working");
    assert!(
        !serde_json::to_string(&record)
            .unwrap()
            .contains("DO_NOT_PERSIST_PROMPTS")
    );
}
#[test]
fn protocol_compatibility_and_null_request_ids() {
    let (_dir, _store, s) = setup();
    assert!(
        call(
            &s,
            "hello",
            serde_json::json!({"protocol_version":{"major":1,"minor":999},"future":true}),
            human()
        )
        .is_ok()
    );
    assert_eq!(
        call(
            &s,
            "hello",
            serde_json::json!({"protocol_version":{"major":2,"minor":0}}),
            human()
        )
        .unwrap_err()
        .data
        .code,
        "protocol_incompatible"
    );
    let request: Request =
        serde_json::from_str(r#"{"jsonrpc":"2.0","id":null,"method":"hello"}"#).unwrap();
    assert_eq!(request.id, Some(RequestId::Null));
    let notification: Request =
        serde_json::from_str(r#"{"jsonrpc":"2.0","method":"hello"}"#).unwrap();
    assert_eq!(notification.id, None);
    let n: Value = serde_json::from_str("18446744073709551615").unwrap();
    assert_eq!(serde_json::to_string(&n).unwrap(), "18446744073709551615");
    for field in [
        "agent_token",
        "bearer_token",
        "agent_id",
        "harness",
        "native_session_id",
    ] {
        let input = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"identity.whoami","context":{field:null}});
        assert!(serde_json::from_value::<Request>(input).is_err());
    }
}
#[test]
fn local_offline_management_does_not_change_received_policy() {
    let (_dir, store, s) = setup();
    store.set_enrollment("allgather", Some("remote")).unwrap();
    let remote = Rule {
        id: "remote".into(),
        from: "*".into(),
        to: "*".into(),
        action: "deny".into(),
        kind: None,
        created_by: "allgather".into(),
        created_at: 1,
    };
    store
        .apply_policy(1, std::slice::from_ref(&remote))
        .unwrap();
    call(
        &s,
        "rules.replace",
        serde_json::json!({"rules":[]}),
        human(),
    )
    .unwrap();
    call(
        &s,
        "token.issue",
        serde_json::json!({"name":"offline","scope":["read"]}),
        human(),
    )
    .unwrap();
    let view = store.rules().unwrap();
    assert!(view.local_rules.is_empty());
    assert_eq!(view.allgather_rules, vec![remote]);
    let info = call(&s, "daemon.info", serde_json::json!({}), human()).unwrap();
    assert_eq!(info["mode"], "allgather");
    assert_eq!(info["allgather"]["connected"], false);
}

mod managed;

mod messaging;

mod enrollment;

#[test]
fn metadata_mutations_require_user_authority_and_attention_ack_has_no_revision() {
    let (_dir, _store, s) = setup();
    let registered = call(
        &s,
        "agent.register",
        serde_json::json!({"harness":"fixture","cwd":"~/work"}),
        human(),
    )
    .unwrap();
    let id = registered["id"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let agent = CallerContext {
        agent_token: Some(Secret(token.into())),
        ..Default::default()
    };
    assert!(
        call(
            &s,
            "session.tags.patch",
            serde_json::json!({"id":id,"add":["pinned"]}),
            agent.clone()
        )
        .is_err()
    );
    let tagged = call(
        &s,
        "session.tags.patch",
        serde_json::json!({"id":id,"add":["pinned"]}),
        human(),
    )
    .unwrap();
    assert_eq!(tagged["tags"], serde_json::json!(["pinned"]));
    for (seq, status) in [(1, "working"), (2, "idle")] {
        call(
            &s,
            "agent.update",
            serde_json::json!({"id":id,"token":token,"seq":seq,"status":status}),
            agent.clone(),
        )
        .unwrap();
    }
    assert!(
        call(
            &s,
            "session.attention.clear",
            serde_json::json!({"id":id}),
            agent
        )
        .is_err()
    );
    let cleared = call(
        &s,
        "session.attention.clear",
        serde_json::json!({"id":id}),
        human(),
    )
    .unwrap();
    assert_eq!(cleared["needs_attention"], false);
    assert_eq!(cleared["tags"], serde_json::json!(["pinned"]));
}
