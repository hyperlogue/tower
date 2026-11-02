use super::*;
use crate::allgather::enrollment::{Poll, Started};
fn request(method: &str, p: serde_json::Value, context: CallerContext) -> Request {
    Request {
        jsonrpc: "2.0".into(),
        id: Some(RequestId::String("enrollment-test".into())),
        method: method.into(),
        params: serde_json::from_value(p).unwrap(),
        context,
    }
}
fn peer(pid: u32) -> Peer {
    Peer {
        uid: tower_pal::uid(),
        pid: Some(pid),
    }
}
fn started(id: &str) -> Started {
    Started {
        enrollment_id: id.into(),
        authorization_url: format!("https://allgather.example/tower/enroll/{id}"),
        polling_token: Secret("private-polling-token-fixture".into()),
        expires_at: "2099-01-01T00:00:00Z".into(),
        interval: 1,
    }
}
#[test]
fn enrollment_grants_no_local_admin_and_only_its_initiator_can_poll() {
    let (_dir, _store, s) = setup();
    let registered = call(
        &s,
        "agent.register",
        serde_json::json!({"harness":"custom","cwd":"/work","native_session_id":"fixture"}),
        human(),
    )
    .unwrap();
    let context = CallerContext {
        agent_token: Some(Secret(registered["token"].as_str().unwrap().into())),
        ..Default::default()
    };
    let r = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        context.clone(),
    );
    let plan = s.enrollment_plan(&r, peer(1)).unwrap();
    assert!(plan.scopes.iter().any(|s| s == "acp:control"));
    let published = s.remember_enrollment(plan, started("first")).unwrap();
    let json = serde_json::to_string(&published).unwrap();
    assert!(!json.contains("private-polling"));
    assert_eq!(
        call(
            &s,
            "allgather.login",
            serde_json::json!({"key":"fixture-key","url":"wss://allgather.example/tower"}),
            context.clone()
        )
        .unwrap_err()
        .data
        .code,
        "denied"
    );
    let query = request(
        "allgather.enrollment.poll",
        serde_json::json!({"id":"first"}),
        human(),
    );
    assert_eq!(
        s.claim_enrollment_poll(&query, peer(2))
            .err()
            .unwrap()
            .data
            .code,
        "denied"
    );
    let query = request(
        "allgather.enrollment.poll",
        serde_json::json!({"id":"first"}),
        context,
    );
    assert!(s.claim_enrollment_poll(&query, peer(3)).unwrap().is_ok());
}
#[test]
fn browser_approval_cannot_widen_scopes_and_logout_invalidates_pending_grants() {
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example","read_only":true}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap();
    assert_eq!(plan.scopes, vec!["read".to_owned()]);
    s.remember_enrollment(plan, started("read-only")).unwrap();
    let approved = Poll {
        state: "approved".into(),
        interval: None,
        machine_id: Some("machine".into()),
        credential_id: Some("credential".into()),
        access_key: Some(Secret("machine-key-fixture".into())),
        node_label: Some("Fixture".into()),
        scopes: Some(vec!["read".into(), "acp:control".into()]),
    };
    assert_eq!(
        s.finish_enrollment_poll("read-only", Ok(approved))
            .err()
            .unwrap()
            .data
            .code,
        "enrollment_failed"
    );
    assert!(!s.paths.state.join("allgather.key").exists());
    assert_eq!(store.host().unwrap().mode, "standalone");
    call(&s, "allgather.logout", serde_json::json!({}), human()).unwrap();
    assert_eq!(
        s.finish_enrollment_poll(
            "read-only",
            Ok(Poll {
                state: "pending".into(),
                interval: None,
                machine_id: None,
                credential_id: None,
                access_key: None,
                node_label: None,
                scopes: None
            })
        )
        .err()
        .unwrap()
        .data
        .code,
        "not_found"
    );
}
#[test]
fn authorized_enrollment_saves_credential_before_confirmation_and_never_returns_it() {
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap();
    s.remember_enrollment(plan, started("authorized")).unwrap();
    let approved = Poll {
        state: "approved".into(),
        interval: None,
        machine_id: Some("machine".into()),
        credential_id: Some("credential".into()),
        access_key: Some(Secret("machine-key-fixture".into())),
        node_label: Some("Fixture".into()),
        scopes: Some(vec!["read".into(), "send".into()]),
    };
    let (result, confirmation) = s
        .finish_enrollment_poll("authorized", Ok(approved))
        .unwrap();
    let text = serde_json::to_string(&result).unwrap();
    assert!(!text.contains("machine-key-fixture"));
    assert!(!text.contains("private-polling"));
    assert_eq!(store.host().unwrap().mode, "allgather");
    assert_eq!(
        tower_pal::read_private(&s.paths.state.join("allgather.key")).unwrap(),
        b"machine-key-fixture"
    );
    assert!(s.paths.state.join("allgather-enrollment.json").exists());
    s.confirm_enrollment(&confirmation.unwrap()).unwrap();
    assert!(!s.paths.state.join("allgather-enrollment.json").exists());
}
#[test]
fn a_rejected_policy_preserves_cache_but_blocks_new_managed_dispatch() {
    let (_dir, store, s) = setup();
    call(
        &s,
        "allgather.login",
        serde_json::json!({"key":"fixture-key","url":"wss://allgather.example/tower"}),
        human(),
    )
    .unwrap();
    let epoch = s.sync_connected(true).unwrap();
    let policy = PolicyPush {
        policy_id: "tower".into(),
        revision: 1,
        rules: Vec::new(),
    };
    s.sync_policy(policy.clone(), epoch).unwrap();
    assert!(s.policy_ready());
    let rejected = s
        .sync_policy(
            PolicyPush {
                policy_id: "wrong-namespace".into(),
                revision: 2,
                rules: Vec::new(),
            },
            epoch,
        )
        .unwrap();
    assert_eq!(rejected.outcome, "rejected");
    assert!(!s.policy_ready());
    assert_eq!(store.rules().unwrap().policy_revision, Some(1));
    s.sync_policy(policy, epoch).unwrap();
    assert!(s.policy_ready());
    s.sync_policy_pending(epoch).unwrap();
    assert!(!s.policy_ready());
}
