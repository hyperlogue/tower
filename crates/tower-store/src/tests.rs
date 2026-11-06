use super::*;
use tower_protocol::{Capabilities, Secret};

fn record(id: &str, native: Option<&str>, pid: Option<u32>) -> AgentRecord {
    AgentRecord {
        checkout_id: None,
        id: id.into(),
        display_name: None,
        node: "node".into(),
        harness: "codex".into(),
        mode: None,
        ownership: "registered".into(),
        protocol: None,
        trust: "weak".into(),
        native_session_id: native.map(str::to_owned),
        pid,
        pid_start_time: pid.map(|_| 1),
        shared_process: false,
        cwd: "~/work/x".into(),
        status: "unknown".into(),
        status_detail: None,
        caps: Capabilities::unavailable("not_implemented"),
        pool_session: None,
        attach_count: 0,
        sources: vec!["cli".into()],
        registered_at: 100,
        updated_at: 100,
        last_seen: 100,
        revision: 0,
        labels: Default::default(),
        tags: Default::default(),
        needs_attention: false,
        task_refs: Vec::new(),
        sync_state: "synced".into(),
    }
}
fn setup() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("db")).unwrap();
    store.initialize("host", "node").unwrap();
    (dir, store)
}
fn rule(id: &str, action: &str) -> Rule {
    Rule {
        id: id.into(),
        from: "*".into(),
        to: "*".into(),
        action: action.into(),
        kind: None,
        created_by: "human".into(),
        created_at: 1,
    }
}
fn update(id: &str, seq: u64, status: &str) -> UpdateParams {
    UpdateParams {
        delivery: Patch::Missing,
        id: id.into(),
        token: Secret("unused-by-store".into()),
        seq,
        status: Some(status.into()),
        display_name: Patch::Missing,
        detail: Patch::Missing,
    }
}
#[test]
fn identity_is_stable_and_conflicts_do_not_merge() {
    let (_dir, s) = setup();
    s.register_test(record("first", None, Some(42)), b"hash")
        .unwrap();
    let merged = s
        .register_test(record("discarded", Some("native"), Some(42)), b"other-hash")
        .unwrap();
    assert!(!merged.created);
    assert_eq!(merged.agent.record.id, "first");
    assert_eq!(merged.agent.token_hash, b"hash");
    s.register_test(record("second", Some("other"), Some(43)), b"second")
        .unwrap();
    assert!(
        s.register_test(record("conflict", Some("native"), Some(43)), b"bad")
            .is_err()
    );
    assert_eq!(s.agents(&AgentFilter::default()).unwrap().len(), 2);
    // Reused PID is a new process, never a match for the former identity.
    let mut reused = record("reused", None, Some(42));
    reused.pid_start_time = Some(2);
    assert!(s.register_test(reused, b"new").unwrap().created);
}
#[test]
fn existing_owned_identity_cannot_be_replaced_by_registration() {
    let (_dir, s) = setup();
    let mut owned = record("owned", Some("thread"), Some(42));
    owned.ownership = "owned".into();
    owned.trust = "strong".into();
    owned.sources = vec!["launcher".into()];
    s.register_test(owned, b"owner").unwrap();
    let result = s
        .register_test(record("external", Some("thread"), Some(42)), b"external")
        .unwrap();
    assert_eq!(result.agent.record.id, "owned");
    assert_eq!(result.agent.record.ownership, "owned");
    assert_eq!(result.agent.token_hash, b"owner");
}
#[test]
fn partial_update_and_stale_source_order_preserve_newer_state() {
    let (_dir, s) = setup();
    s.register_test(record("a", Some("thread"), None), b"hash")
        .unwrap();
    let mut u = update("a", 10, "working");
    u.display_name = Patch::Set("name".into());
    u.detail = Patch::Set(Value::Object(
        [("model".into(), Value::String("model".into()))].into(),
    ));
    s.update(&u, 200).unwrap();
    u.seq = 11;
    u.display_name = Patch::Clear;
    u.detail = Patch::Set(Value::Object(
        [("last_tool".into(), Value::String("Bash".into()))].into(),
    ));
    let revision = s.update(&u, 201).unwrap().revision;
    let result = s.update(&update("a", 9, "idle"), 202).unwrap();
    assert!(!result.applied);
    assert_eq!(result.revision, revision);
    let current = s.agent("a").unwrap();
    assert_eq!(current.record.status, "working");
    assert_eq!(current.record.display_name, None);
    let Some(Value::Object(detail)) = current.record.status_detail else {
        panic!("missing detail")
    };
    assert_eq!(detail.len(), 2);
    assert!(
        s.update(&update("a", u64::MAX, "waiting_approval"), 203)
            .unwrap()
            .applied
    );
    s.mark_unconfirmed(204).unwrap();
    assert_eq!(s.agent("a").unwrap().seq, u64::MAX);
    assert_eq!(s.agent("a").unwrap().record.status, "unknown");
}
#[test]
fn policy_snapshot_is_atomic_separate_and_durable() {
    let (dir, s) = setup();
    s.put_rule(rule("local", "allow")).unwrap();
    s.set_enrollment("allgather", Some("remote-node")).unwrap();
    assert!(s.apply_policy(5, &[rule("remote", "deny")]).unwrap());
    assert!(!s.apply_policy(5, &[rule("remote", "deny")]).unwrap());
    assert!(s.apply_policy(4, &[]).is_err());
    assert!(s.apply_policy(5, &[]).is_err());
    assert!(
        s.apply_policy(6, &[rule("duplicate", "allow"), rule("duplicate", "deny")])
            .is_err()
    );
    let before = s.rules().unwrap();
    assert_eq!(before.policy_revision, Some(5));
    assert_eq!(before.allgather_rules[0].action, "deny");
    s.replace_rules(&[]).unwrap();
    drop(s);
    let s = Store::open(&dir.path().join("db")).unwrap();
    let host = s.initialize("host", "new-random-name").unwrap();
    assert_eq!(host.mode, "allgather");
    assert_eq!(host.node_name, "remote-node");
    let rules = s.rules().unwrap();
    assert!(rules.local_rules.is_empty());
    assert_eq!(rules.allgather_rules, before.allgather_rules);
    s.set_enrollment("standalone", None).unwrap();
    assert_eq!(s.host().unwrap().node_name, "node");
}
#[test]
fn expiration_uses_confirmed_end_and_protects_active_and_pending_recovery() {
    let (_dir, s) = setup();
    for id in ["ended", "active", "pending"] {
        let mut r = record(id, None, None);
        r.ownership = "owned".into();
        s.register_test(r, b"hash").unwrap();
    }
    s.unregister("ended", 200).unwrap();
    s.unregister("pending", 200).unwrap();
    s.lock()
        .unwrap()
        .execute(
            "UPDATE agents SET recovery_pending=1 WHERE id='pending'",
            [],
        )
        .unwrap();
    s.unregister("ended", 500).unwrap();
    assert_eq!(s.agent("ended").unwrap().ended_at, Some(200));
    assert!(s.expire(1199, 1000).unwrap().is_empty());
    assert_eq!(s.expire(1200, 1000).unwrap(), vec!["ended"]);
    assert!(s.agent("active").is_ok());
    assert!(s.agent("pending").is_ok());
    // Explicit registration before expiry wins over the cleanup transaction.
    s.register_test(record("candidate", Some("reg"), None), b"hash")
        .unwrap();
    s.unregister("candidate", 100).unwrap();
    s.register_test(record("ignored", Some("reg"), None), b"new")
        .unwrap();
    assert!(s.expire(1_000_000, 1000).unwrap().is_empty());
    assert!(s.agent("candidate").is_ok());
}
#[test]
fn token_names_are_unique_and_revocation_is_immediate() {
    let (_dir, s) = setup();
    let info = TokenInfo {
        name: "test".into(),
        scope: vec!["read".into()],
        created_at: 10,
        expires_at: Some(20),
    };
    s.issue_token(info.clone(), b"digest").unwrap();
    assert!(s.issue_token(info, b"second").is_err());
    assert_eq!(s.tokens().unwrap().len(), 1);
    s.revoke_token("test").unwrap();
    assert!(s.tokens().unwrap().is_empty());
}

#[test]
fn registration_guard_rejects_a_different_resolved_identity_without_mutation() {
    let (_dir, store) = setup();
    store.register_test(record("a", None, None), b"a").unwrap();
    store
        .register_test(record("b", Some("native-b"), Some(42)), b"b")
        .unwrap();
    let before = store.agent("b").unwrap().record;
    let mut candidate = record("new", None, Some(42));
    candidate.display_name = Some("unauthorized change".into());
    assert!(store.register(candidate, b"other", "a").is_err());
    assert_eq!(store.agent("b").unwrap().record, before);
    assert_eq!(store.agent("b").unwrap().token_hash, b"b");
    assert!(
        store
            .register(record("new", None, None), b"other", "a")
            .is_err()
    );
    assert_eq!(store.agents(&AgentFilter::default()).unwrap().len(), 2);
}

#[test]
fn version_one_database_migrates_with_credentials_and_identity_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(include_str!("schema.sql")).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    let agent = record("existing", Some("native"), None);
    conn.execute("INSERT INTO agents(id,harness,native_id,ownership,public_json,token_hash) VALUES(?1,?2,?3,?4,?5,?6)", params![agent.id,agent.harness,agent.native_session_id,agent.ownership,serde_json::to_string(&agent).unwrap(), b"credential".as_slice()]).unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.agent_by_token_hash(b"credential").unwrap().record,
        agent
    );
    assert_eq!(
        store
            .agent_by_native_id("codex", "native")
            .unwrap()
            .record
            .id,
        "existing"
    );
    assert!(matches!(
        store.agent_by_token_hash(b"wrong"),
        Err(Error::NotFound)
    ));
    drop(store);
    assert!(Store::open(&path).is_ok());
}

impl Store {
    fn register_test(&self, candidate: AgentRecord, hash: &[u8]) -> Result<Registration> {
        let target = self.registration_target(&candidate)?;
        let expected = target.map_or_else(|| candidate.id.clone(), |a| a.record.id);
        self.register(candidate, hash, &expected)
    }
}

#[test]
fn version_three_migration_uses_terminal_binding_evidence_and_keeps_launch_intent_separate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(include_str!("schema.sql")).unwrap();
    conn.execute_batch("CREATE TABLE owned_launches(id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,data TEXT NOT NULL); PRAGMA user_version=3;").unwrap();
    let mut terminal = record("terminal-session", None, None);
    terminal.ownership = "owned".into();
    terminal.pool_session = Some("different-resource-name".into());
    let mut unknown = record("managed-unknown", None, None);
    unknown.ownership = "owned".into();
    for r in [&terminal, &unknown] {
        let mut json = serde_json::to_value(r).unwrap();
        json.as_object_mut().unwrap().remove("protocol");
        json["caps"].as_object_mut().unwrap().remove("snapshot");
        conn.execute("INSERT INTO agents(id,harness,ownership,public_json,token_hash) VALUES(?1,?2,?3,?4,?5)",params![r.id,r.harness,r.ownership,json.to_string(),b"hash".as_slice()]).unwrap();
    }
    let launch = serde_json::json!({"harness":"codex","mode":"native","cwd":"/tmp","argv":["codex","--no-daemon"]});
    conn.execute(
        "INSERT INTO owned_launches(id,data) VALUES(?1,?2)",
        params![terminal.id, launch.to_string()],
    )
    .unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    let saved = store.managed_session(&terminal.id).unwrap();
    assert_eq!(saved.protocol, "terminal");
    assert_eq!(
        serde_json::to_value(&saved.binding).unwrap()["name"],
        "different-resource-name"
    );
    assert_eq!(
        serde_json::to_value(saved.launch.as_ref().unwrap()).unwrap()["spec"],
        launch
    );
    assert!(store.agent(&terminal.id).unwrap().record.caps.snapshot);
    assert_eq!(
        store
            .agent(&terminal.id)
            .unwrap()
            .record
            .protocol
            .as_deref(),
        Some("terminal")
    );
    assert!(store.agent(&unknown.id).unwrap().record.protocol.is_none());
    assert!(matches!(
        store.managed_session(&unknown.id),
        Err(Error::NotFound)
    ));
    store.disable_managed_recovery(&terminal.id).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(!store.managed_session(&terminal.id).unwrap().auto_resume);
    assert!(
        store
            .managed_session(&terminal.id)
            .unwrap()
            .launch
            .is_some()
    );
    assert_eq!(
        store.managed_session(&terminal.id).unwrap().binding,
        saved.binding
    );
}

#[test]
fn shared_processes_keep_native_identities_separate_and_reject_ambiguous_pid_registration() {
    let (_dir, store) = setup();
    let mut a = record("shared-a", Some("thread-a"), Some(123));
    a.shared_process = true;
    a.mode = Some("app-server".into());
    let mut b = record("shared-b", Some("thread-b"), Some(123));
    b.shared_process = true;
    b.mode = Some("app-server".into());
    store.register(a.clone(), b"a", &a.id).unwrap();
    store.register(b.clone(), b"b", &b.id).unwrap();
    assert_eq!(
        store
            .agent_by_native_id(&a.harness, "thread-a")
            .unwrap()
            .record
            .id,
        a.id
    );
    assert_eq!(
        store
            .agent_by_native_id(&b.harness, "thread-b")
            .unwrap()
            .record
            .id,
        b.id
    );
    assert_eq!(
        store.registration_target(&b).unwrap().unwrap().record.id,
        b.id
    );
    let unknown = record("unknown", None, Some(123));
    assert!(store.registration_target(&unknown).is_err());
}

#[test]
fn policy_deltas_are_atomic_ordered_and_replay_safe_after_restart() {
    let (dir, store) = setup();
    store.put_rule(rule("local", "allow")).unwrap();
    store
        .apply_policy(7, &[rule("a", "deny"), rule("b", "ask")])
        .unwrap();
    let delta = tower_protocol::PolicyDelta {
        policy_id: "tower".into(),
        base_revision: 7,
        revision: 8,
        upserts: vec![rule("a", "allow")],
        removed: vec!["b".into()],
        order: vec!["a".into()],
    };
    assert!(store.apply_policy_delta(&delta).unwrap());
    let before = store.rules().unwrap();
    assert_eq!(before.allgather_rules, vec![rule("a", "allow")]);
    assert!(before.local_rules.iter().any(|r| r.id == "local"));
    drop(store);
    let store = Store::open(&dir.path().join("db")).unwrap();
    assert!(!store.apply_policy_delta(&delta).unwrap());
    let mut conflict = delta.clone();
    conflict.upserts = vec![rule("a", "deny")];
    assert!(store.apply_policy_delta(&conflict).is_err());
    let mut wrong = delta.clone();
    wrong.revision = 9;
    assert!(store.apply_policy_delta(&wrong).is_err());
    wrong.base_revision = 8;
    wrong.removed.clear();
    wrong.order.clear();
    assert!(store.apply_policy_delta(&wrong).is_err());
    assert_eq!(
        store.rules().unwrap().allgather_rules,
        before.allgather_rules
    );
    let empty = tower_protocol::PolicyDelta {
        policy_id: "tower".into(),
        base_revision: 8,
        revision: 9,
        upserts: vec![],
        removed: vec!["a".into()],
        order: vec![],
    };
    assert!(store.apply_policy_delta(&empty).unwrap());
    assert!(store.rules().unwrap().allgather_rules.is_empty());
    assert_eq!(store.rules().unwrap().local_rules, before.local_rules);
}

#[test]
fn session_tags_and_attention_are_atomic_and_survive_reconstruction() {
    let (dir, s) = setup();
    s.register_test(record("a", None, None), b"token").unwrap();
    s.edit_session(
        "a",
        &["pinned".into(), "review".into(), "pinned".into()],
        &[],
        false,
        101,
    )
    .unwrap();
    assert_eq!(
        s.agent("a")
            .unwrap()
            .record
            .tags
            .into_iter()
            .collect::<Vec<_>>(),
        ["pinned", "review"]
    );
    s.update(&update("a", 1, "working"), 102).unwrap();
    assert!(!s.agent("a").unwrap().record.needs_attention);
    s.update(&update("a", 2, "idle"), 103).unwrap();
    assert!(s.agent("a").unwrap().record.needs_attention);
    let cleared = s
        .edit_session("a", &[], &["review".into()], true, 104)
        .unwrap();
    assert!(!cleared.needs_attention);
    assert_eq!(cleared.tags.into_iter().collect::<Vec<_>>(), ["pinned"]);
    s.mark_unconfirmed(105).unwrap();
    drop(s);
    let s = Store::open(&dir.path().join("db")).unwrap();
    s.update(&update("a", 3, "idle"), 106).unwrap();
    assert!(!s.agent("a").unwrap().record.needs_attention);
    s.update(&update("a", 4, "waiting_approval"), 107).unwrap();
    assert!(s.agent("a").unwrap().record.needs_attention);
    s.edit_session("a", &[], &[], true, 108).unwrap();
    s.mark_unconfirmed(109).unwrap();
    s.update(&update("a", 5, "waiting_approval"), 110).unwrap();
    assert!(!s.agent("a").unwrap().record.needs_attention);
    s.update(&update("a", 6, "working"), 111).unwrap();
    s.update(&update("a", 7, "idle"), 112).unwrap();
    assert!(s.agent("a").unwrap().record.needs_attention);
    assert!(
        s.edit_session("a", &["bad\ntag".into()], &[], false, 113)
            .is_err()
    );
    assert!(
        s.edit_session("a", &["tag".into()], &["tag".into()], false, 113)
            .is_err()
    );
    assert!(
        s.edit_session(
            "a",
            &(0..64).map(|n| n.to_string()).collect::<Vec<_>>(),
            &[],
            false,
            113
        )
        .is_err()
    );
    assert!(s.agent("a").unwrap().record.needs_attention);
}

#[test]
fn expected_stop_does_not_raise_attention_but_lost_process_does() {
    let (_dir, s) = setup();
    for id in ["expected", "unexpected"] {
        s.register_test(record(id, None, None), b"token").unwrap();
        s.update(&update(id, 1, "working"), 101).unwrap();
    }
    s.expect_stop("expected").unwrap();
    for id in ["expected", "unexpected"] {
        s.mark_lost(id, 102).unwrap();
    }
    assert!(!s.agent("expected").unwrap().record.needs_attention);
    assert!(s.agent("unexpected").unwrap().record.needs_attention);
}
