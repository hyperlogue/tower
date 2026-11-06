use super::*;
use crate::allgather::enrollment::{Poll, Started};
const PAIRING_CODE: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
fn request(method: &str, mut p: serde_json::Value, context: CallerContext) -> Request {
    if method == "allgather.enrollment.begin" && p.get("pairing_code").is_none() {
        p["pairing_code"] = serde_json::json!(PAIRING_CODE);
    }
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
fn approved() -> Poll {
    Poll {
        state: "approved".into(),
        interval: None,
        machine_id: Some("machine".into()),
        credential_id: Some("credential".into()),
        access_key: Some(Secret("machine-key-fixture".into())),
        node_label: Some("Fixture".into()),
        scopes: Some(vec!["read".into(), "send".into()]),
    }
}
#[test]
fn pairing_request_is_private_durable_and_immutable_across_processes_and_restart() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
    assert!(crate::allgather::enrollment::valid_pairing_secret(
        &plan.request_id
    ));
    let path = s.paths.state.join("allgather-pairing.json");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let saved = crate::allgather::enrollment::load(&s.paths).unwrap();
    assert_eq!(saved[&plan.request_id].plan.body(), plan.body());
    assert_eq!(
        call(
            &s,
            "allgather.login",
            serde_json::json!({"url":"wss://allgather.example/tower","key":""}),
            human()
        )
        .err()
        .unwrap()
        .data
        .code,
        "invalid_params"
    );
    let retried = s.enrollment_plan(&begin, peer(101)).unwrap().unwrap();
    assert_eq!(retried.body(), plan.body());
    let mut older = saved;
    older.get_mut(&plan.request_id).unwrap().plan.tower_version = "previous-fixture-version".into();
    crate::allgather::enrollment::persist(&s.paths, &older).unwrap();
    let paths = s.paths.clone();
    drop(s);
    let s = Service::new(paths, store, "test", tower_sessions::Backends::default()).unwrap();
    let resumed = s.enrollment_plan(&begin, peer(102)).unwrap().unwrap();
    assert_eq!(resumed.request_id, plan.request_id);
    assert_eq!(resumed.tower_version, "previous-fixture-version");
    for options in [
        serde_json::json!({"url":"https://allgather.example","read_only":true}),
        serde_json::json!({"url":"https://allgather.example","node_label":"changed"}),
        serde_json::json!({"url":"https://different.example"}),
    ] {
        let changed = request("allgather.enrollment.begin", options, human());
        assert_eq!(
            s.enrollment_plan(&changed, peer(102))
                .err()
                .unwrap()
                .data
                .code,
            "conflict"
        );
    }
    let visible = s
        .remember_enrollment(resumed.clone(), started("durable"))
        .unwrap();
    assert_eq!(
        s.remember_enrollment(resumed, started("durable")).unwrap(),
        visible
    );
    let paths = s.paths.clone();
    let store = s.store.clone();
    drop(s);
    let s = Service::new(paths, store, "test", tower_sessions::Backends::default()).unwrap();
    assert_eq!(
        s.enrollment_plan(&begin, peer(103)).unwrap().err().unwrap(),
        visible
    );
    let query = request(
        "allgather.enrollment.poll",
        serde_json::json!({"id":"durable"}),
        human(),
    );
    assert!(s.claim_enrollment_poll(&query, peer(104)).unwrap().is_ok());
    let text = serde_json::to_string(&visible).unwrap();
    assert!(!text.contains(PAIRING_CODE));
    assert!(!text.contains("private-polling"));
}
#[test]
fn invalid_or_missing_codes_do_not_allocate_pairing_state() {
    let (_dir, _store, s) = setup();
    for code in [
        None,
        Some(""),
        Some("not-a-code"),
        Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"),
    ] {
        let mut r = request("allgather.enrollment.begin", serde_json::json!({}), human());
        let p = match code {
            None => serde_json::json!({"url":"https://allgather.example"}),
            Some(code) => {
                serde_json::json!({"url":"https://allgather.example","pairing_code":code})
            }
        };
        r.params = serde_json::from_value(p).unwrap();
        assert_eq!(
            s.enrollment_plan(&r, peer(100)).err().unwrap().data.code,
            "invalid_params"
        );
        assert!(!s.paths.state.join("allgather-pairing.json").exists());
    }
}
#[test]
fn pairing_responses_cannot_echo_credentials_as_public_identity() {
    let (_dir, _store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
    s.remember_enrollment(plan, started("redacted")).unwrap();
    for secret in [
        PAIRING_CODE,
        "machine-key-fixture",
        "private-polling-token-fixture",
    ] {
        let mut response = approved();
        response.node_label = Some(secret.into());
        assert_eq!(
            s.finish_enrollment_poll("redacted", Ok(response))
                .err()
                .unwrap()
                .data
                .code,
            "enrollment_failed"
        );
        assert!(!s.paths.state.join("allgather.key").exists());
    }
    let mut response = approved();
    response.machine_id = Some(PAIRING_CODE.into());
    assert_eq!(
        s.finish_enrollment_poll("redacted", Ok(response))
            .err()
            .unwrap()
            .data
            .code,
        "enrollment_failed"
    );
    assert!(!s.paths.state.join("allgather-consent.json").exists());
}
#[test]
fn invalid_pairing_consent_never_falls_back_to_manual_scopes() {
    let (_dir, store, mut s) = setup();
    s.allgather_config.scopes = Some(vec!["read".into(), "send".into(), "acp:control".into()]);
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example","read_only":true}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
    s.remember_enrollment(plan, started("restricted")).unwrap();
    let mut response = approved();
    response.scopes = Some(vec!["read".into()]);
    s.finish_enrollment_poll("restricted", Ok(response))
        .unwrap();
    store.set_enrollment("allgather", Some("machine")).unwrap();
    assert_eq!(s.remote_scopes().unwrap(), ["read"]);
    let consent = s.paths.state.join("allgather-consent.json");
    let original = tower_pal::read_private(&consent).unwrap();
    for bytes in [
        b"invalid json".to_vec(),
        serde_json::to_vec(&serde_json::json!({"origin":"https://other.example","machine_id":"machine","scopes":["read"]})).unwrap(),
        serde_json::to_vec(&serde_json::json!({"origin":"https://allgather.example","machine_id":"other","scopes":["read"]})).unwrap(),
    ] {
        tower_pal::write_private(&consent, &bytes).unwrap();
        assert_eq!(s.remote_scopes().unwrap_err().data.code, "unauthorized");
    }
    std::fs::remove_file(&consent).unwrap();
    std::fs::create_dir(&consent).unwrap();
    assert_eq!(s.remote_scopes().unwrap_err().data.code, "unauthorized");
    std::fs::remove_dir(&consent).unwrap();
    tower_pal::write_private(&consent, &original).unwrap();

    let binding = s.paths.state.join("allgather-server");
    std::fs::remove_file(&binding).unwrap();
    std::fs::create_dir(&binding).unwrap();
    let manual =
        serde_json::json!({"url":"wss://allgather.example/tower","key":"new-manual-fixture-key"});
    assert!(call(&s, "allgather.login", manual.clone(), human()).is_err());
    assert_eq!(tower_pal::read_private(&consent).unwrap(), original);
    assert_eq!(
        tower_pal::read_private(&s.paths.state.join("allgather.key")).unwrap(),
        b"machine-key-fixture"
    );
    assert!(s.remote_scopes().is_err());
    std::fs::remove_dir(&binding).unwrap();
    tower_pal::write_private(&binding, b"wss://allgather.example/tower").unwrap();
    store.set_enrollment("allgather", Some("machine")).unwrap();
    assert_eq!(s.remote_scopes().unwrap(), ["read"]);

    call(&s, "allgather.login", manual, human()).unwrap();
    assert!(!consent.exists());
    assert_eq!(s.remote_scopes().unwrap(), ["read", "send", "acp:control"]);
}
#[test]
fn logout_cancels_create_and_late_responses_across_restart() {
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
    call(&s, "allgather.logout", serde_json::json!({}), human()).unwrap();
    assert_eq!(
        s.remember_enrollment(plan.clone(), started("late"))
            .err()
            .unwrap()
            .data
            .code,
        "not_found"
    );
    let paths = s.paths.clone();
    drop(s);
    let s = Service::new(paths, store, "test", tower_sessions::Backends::default()).unwrap();
    assert_eq!(
        s.remember_enrollment(plan, started("late"))
            .err()
            .unwrap()
            .data
            .code,
        "not_found"
    );
    assert!(
        crate::allgather::enrollment::load(&s.paths)
            .unwrap()
            .is_empty()
    );
    assert!(!s.paths.state.join("allgather.key").exists());
}
#[test]
fn confirmation_recovers_only_after_credential_persistence_and_keeps_completion() {
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example"}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
    s.remember_enrollment(plan, started("saved")).unwrap();
    let key_path = s.paths.state.join("allgather.key");
    std::fs::create_dir(&key_path).unwrap();
    assert!(s.finish_enrollment_poll("saved", Ok(approved())).is_err());
    assert!(s.pending_enrollment_confirmation().unwrap().is_none());
    std::fs::remove_dir(&key_path).unwrap();
    let (authorized, confirmation) = s.finish_enrollment_poll("saved", Ok(approved())).unwrap();
    assert!(
        !serde_json::to_string(&authorized)
            .unwrap()
            .contains("machine-key-fixture")
    );
    let confirmation = confirmation.unwrap();
    let paths = s.paths.clone();
    drop(s);
    let s = Service::new(paths, store, "test", tower_sessions::Backends::default()).unwrap();
    let recovered = s.pending_enrollment_confirmation().unwrap().unwrap();
    assert_eq!(recovered.request_id, confirmation.request_id);
    assert_eq!(recovered.access_key_hash, confirmation.access_key_hash);
    let mut wrong = recovered.clone();
    wrong.machine_id = "other-machine".into();
    assert_eq!(
        s.confirm_enrollment(&wrong).err().unwrap().data.code,
        "conflict"
    );
    let complete = s.confirm_enrollment(&recovered).unwrap();
    assert_eq!(s.confirm_enrollment(&recovered).unwrap(), complete);
    assert!(s.pending_enrollment_confirmation().unwrap().is_none());
    assert!(s.enrollment_plan(&begin, peer(105)).unwrap().is_err());
    let query = request(
        "allgather.enrollment.poll",
        serde_json::json!({"id":"saved"}),
        human(),
    );
    let reply = serde_json::to_value(
        s.claim_enrollment_poll(&query, peer(105))
            .unwrap()
            .err()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reply["state"], "complete");
    let stored = std::fs::read_to_string(s.paths.state.join("allgather-pairing.json")).unwrap();
    assert!(!stored.contains(PAIRING_CODE));
    assert!(!stored.contains("private-polling"));
    assert!(!stored.contains("machine-key-fixture"));
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
    let plan = s.enrollment_plan(&r, peer(1)).unwrap().unwrap();
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
fn pairing_approval_cannot_widen_scopes_and_logout_invalidates_pending_grants() {
    let (_dir, store, s) = setup();
    let begin = request(
        "allgather.enrollment.begin",
        serde_json::json!({"url":"https://allgather.example","read_only":true}),
        human(),
    );
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
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
    let plan = s.enrollment_plan(&begin, peer(100)).unwrap().unwrap();
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
    assert!(s.paths.state.join("allgather-pairing.json").exists());
    s.confirm_enrollment(&confirmation.unwrap()).unwrap();
    let stored = std::fs::read_to_string(s.paths.state.join("allgather-pairing.json")).unwrap();
    assert!(!stored.contains("private-polling"));
    assert!(!stored.contains(PAIRING_CODE));
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
