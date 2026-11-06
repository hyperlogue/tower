use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tower_sessions::messaging::RegisteredDelivery;

struct Delivery {
    calls: Arc<AtomicUsize>,
    outcome: usize,
}
impl RegisteredDelivery for Delivery {
    fn capabilities(&self, _: &AgentRecord, endpoint: &DeliveryEndpoint) -> Capabilities {
        if endpoint.thread_id.is_none() && endpoint.messaging_socket.is_none() {
            return Capabilities::unavailable("fixture_no_channel");
        }
        Capabilities {
            message: "native".into(),
            message_reason: None,
            wake_when_idle: true,
            reply_path: true,
            ..Capabilities::unavailable("test")
        }
    }
    fn send(
        &self,
        _: &AgentRecord,
        _: &DeliveryEndpoint,
        _: &str,
    ) -> std::result::Result<DeliveryReceipt, RpcError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.outcome {
            1 => Err(app("delivery_failed", "Known failure")),
            2 => Err(app("delivery_unknown", "Unknown outcome")),
            _ => Ok(DeliveryReceipt {
                method: "native".into(),
                acknowledgement: "harness".into(),
            }),
        }
    }
}
fn messaging(
    outcome: usize,
) -> (
    tempfile::TempDir,
    Arc<Store>,
    Service,
    Arc<AtomicUsize>,
    String,
    String,
    CallerContext,
) {
    let (dir, store, mut s) = setup();
    let calls = Arc::new(AtomicUsize::new(0));
    s.backends = tower_sessions::Backends::default().with_delivery(Arc::new(Delivery {
        calls: calls.clone(),
        outcome,
    }));
    let (a, token) = register(&s, "sender");
    let (b, btoken) = register(&s, "receiver");
    call(
        &s,
        "agent.delivery",
        serde_json::json!({"id":b,"token":btoken,"delivery":{"thread_id":"receiver"}}),
        human(),
    )
    .unwrap();
    let ctx = CallerContext {
        agent_id: Some(a.clone()),
        agent_token: Some(token),
        ..Default::default()
    };
    (dir, store, s, calls, a, b, ctx)
}
fn send(
    s: &Service,
    to: &str,
    body: &str,
    context: CallerContext,
) -> std::result::Result<serde_json::Value, RpcError> {
    s.send_message(
        &Request {
            jsonrpc: "2.0".into(),
            id: None,
            method: "message.send".into(),
            params: value(MessageSendParams {
                to: to.into(),
                body: body.into(),
                ..Default::default()
            })
            .unwrap(),
            context,
        },
        Peer {
            uid: tower_pal::uid(),
            pid: None,
        },
    )
    .map(|v| serde_json::to_value(v).unwrap())
}
#[test]
fn approval_resend_throttle_and_body_privacy() {
    let (_dir, store, s, calls, a, b, ctx) = messaging(0);
    let marker = "private-message-body-482950";
    let e = send(&s, &b, marker, ctx.clone()).unwrap_err();
    assert_eq!(e.data.code, "approval_required");
    let Value::String(id) = &e.data.details["approval_id"] else {
        panic!()
    };
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        call(
            &s,
            "approval.resolve",
            serde_json::json!({"id":id,"allow":true}),
            ctx.clone()
        )
        .unwrap_err()
        .data
        .code,
        "denied"
    );
    call(
        &s,
        "approval.resolve",
        serde_json::json!({"id":id,"allow":true}),
        human(),
    )
    .unwrap();
    assert_eq!(
        send(&s, &b, marker, ctx.clone()).unwrap()["state"],
        "delivered"
    );
    assert_eq!(
        send(&s, &b, marker, ctx.clone()).unwrap_err().data.code,
        "duplicate"
    );
    for i in 1..6 {
        send(&s, &b, &format!("distinct-{i}"), ctx.clone()).unwrap();
    }
    assert_eq!(
        send(&s, &b, "seventh", ctx.clone()).unwrap_err().data.code,
        "rate_limited"
    );
    let approval = store.approvals().unwrap();
    assert_eq!(approval[0].record.from, a);
    for entry in std::fs::read_dir(&s.paths.state).unwrap().flatten() {
        if entry.file_type().unwrap().is_file() {
            let bytes = std::fs::read(entry.path()).unwrap();
            assert!(
                !bytes.windows(marker.len()).any(|w| w == marker.as_bytes()),
                "message body persisted"
            );
        }
    }
}
#[test]
fn one_shot_approval_does_not_allow_a_different_message() {
    let (_dir, _, s, _, _, b, ctx) = messaging(0);
    let e = send(&s, &b, "one", ctx.clone()).unwrap_err();
    let id = &e.data.details["approval_id"];
    call(
        &s,
        "approval.resolve",
        serde_json::json!({"id":id,"allow":true,"remember":false}),
        human(),
    )
    .unwrap();
    send(&s, &b, "one", ctx.clone()).unwrap();
    assert_eq!(
        send(&s, &b, "two", ctx).unwrap_err().data.code,
        "approval_required"
    );
}
#[test]
fn local_approval_cannot_override_remote_policy() {
    let (_dir, store, s, _, _, b, ctx) = messaging(0);
    store.set_enrollment("allgather", None).unwrap();
    assert_eq!(
        send(&s, &b, "one", human()).unwrap_err().data.code,
        "denied"
    );
    let rule = Rule {
        id: "remote".into(),
        from: "*".into(),
        to: "*".into(),
        action: "ask".into(),
        kind: None,
        created_by: "remote".into(),
        created_at: 0,
    };
    store.apply_policy(1, &[rule]).unwrap();
    let e = send(&s, &b, "one", ctx).unwrap_err();
    assert_eq!(e.data.code, "approval_required");
    let resolved = call(
        &s,
        "approval.resolve",
        serde_json::json!({"id":e.data.details["approval_id"],"allow":true}),
        human(),
    )
    .unwrap();
    assert_eq!(resolved["state"], "pending");
    assert_eq!(resolved["required"], serde_json::json!(["allgather"]));
    assert_eq!(
        send(&s, &b, "one", human()).unwrap_err().data.code,
        "approval_required"
    );
    store.apply_policy(2, &[]).unwrap();
    s.refresh_approvals().unwrap();
    let status = call(
        &s,
        "approval.status",
        serde_json::json!({"id":e.data.details["approval_id"]}),
        human(),
    )
    .unwrap();
    assert_eq!(status["state"], "approved");
}
#[test]
fn known_failures_release_budget_but_uncertain_sends_do_not_retry() {
    for outcome in [1, 2] {
        let (_dir, _, s, calls, _, b, _) = messaging(outcome);
        let code = if outcome == 1 {
            "delivery_failed"
        } else {
            "delivery_unknown"
        };
        assert_eq!(
            send(&s, &b, "payload", human()).unwrap_err().data.code,
            code
        );
        assert_eq!(
            send(&s, &b, "payload", human()).unwrap_err().data.code,
            code
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if outcome == 1 { 2 } else { 1 }
        );
    }
}

#[test]
fn shared_process_identity_never_becomes_a_sender_or_a_process_kill_capability() {
    let (_dir, _store, s) = setup();
    for id in ["shared-a", "shared-b"] {
        let registered=call(&s,"agent.register",serde_json::json!({"harness":"codex","native_session_id":id,"pid":tower_pal::pid(),"shared_process":true,"cwd":"~/work"}),human()).unwrap();
        let record = call(
            &s,
            "agent.get",
            serde_json::json!({"id":registered["id"]}),
            human(),
        )
        .unwrap();
        assert_eq!(record["caps"]["kill"], false);
    }
    let actor = s
        .actor(
            &human(),
            Peer {
                uid: tower_pal::uid(),
                pid: Some(tower_pal::pid()),
            },
        )
        .unwrap();
    assert!(matches!(actor, Actor::Unregistered));
}

#[test]
fn explicit_sending_credentials_cannot_fall_back_to_human() {
    let (_dir, _, s, calls, _, b, ctx) = messaging(0);
    let request = |token: &str| Request {
        jsonrpc: "2.0".into(),
        id: None,
        method: "message.send".into(),
        params: value(serde_json::json!({"to":b,"body":"credential fixture","from_token":token}))
            .unwrap(),
        context: human(),
    };
    let peer = Peer {
        uid: tower_pal::uid(),
        pid: None,
    };
    assert_eq!(
        s.send_message(&request("invalid"), peer)
            .unwrap_err()
            .data
            .code,
        "unauthorized"
    );
    assert_eq!(
        s.send_message(&request(&ctx.agent_token.unwrap().0), peer)
            .unwrap_err()
            .data
            .code,
        "approval_required"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn concurrent_identical_sends_reserve_one_submission_without_blocking_registry_reads() {
    struct Block {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl RegisteredDelivery for Block {
        fn capabilities(&self, _: &AgentRecord, _: &DeliveryEndpoint) -> Capabilities {
            Capabilities::unavailable("fixture")
        }
        fn send(
            &self,
            _: &AgentRecord,
            _: &DeliveryEndpoint,
            _: &str,
        ) -> std::result::Result<DeliveryReceipt, RpcError> {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            Ok(DeliveryReceipt {
                method: "native".into(),
                acknowledgement: "harness".into(),
            })
        }
    }
    let (_dir, _, mut s, _, _, b, _) = messaging(0);
    let (entered, ready) = std::sync::mpsc::sync_channel(1);
    let (release, blocked) = std::sync::mpsc::channel();
    s.backends = tower_sessions::Backends::default().with_delivery(Arc::new(Block {
        entered,
        release: Mutex::new(blocked),
    }));
    let s = Arc::new(s);
    let first = s.clone();
    let target = b.clone();
    let first = std::thread::spawn(move || send(&first, &target, "same submission", human()));
    ready
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let (done, result) = std::sync::mpsc::channel();
    let second = s.clone();
    let second = std::thread::spawn(move || {
        let duplicate = send(&second, &b, "same submission", human());
        let registry = call(&second, "agent.list", serde_json::json!({}), human());
        done.send((duplicate, registry)).unwrap();
    });
    let result = result.recv_timeout(std::time::Duration::from_secs(2));
    release.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap();
    let (duplicate, registry) = result.expect("delivery must not hold the registry mutation lock");
    assert_eq!(duplicate.unwrap_err().data.code, "delivery_in_progress");
    assert!(registry.is_ok());
}

#[test]
fn failed_login_storage_keeps_the_restrictive_enrollment_mode() {
    let (_dir, store, mut service) = setup();
    service.allgather_config.url = "wss://allgather.example/tower".into();
    std::fs::create_dir(service.paths.state.join("allgather.key")).unwrap();
    assert!(
        call(
            &service,
            "allgather.login",
            serde_json::json!({"key":"fixture-key"}),
            human(),
        )
        .is_err()
    );
    assert_eq!(store.host().unwrap().mode, "allgather");
    assert_eq!(store.host().unwrap().policy_revision, None);
    assert!(!service.policy_ready());
    assert_eq!(
        tower_pal::read_private(&service.paths.state.join("allgather-server")).unwrap(),
        b"wss://allgather.example/tower"
    );
}

#[test]
fn old_allgather_connections_cannot_act_after_enrollment_rotation() {
    let (_dir, store, s) = setup();
    store.set_enrollment("allgather", None).unwrap();
    tower_pal::write_private(&s.paths.state.join("allgather.key"), b"old-key").unwrap();
    let old = s
        .sync_activate(&Secret("old-key".into()), "old-node")
        .unwrap();
    s.sync_connected(false).unwrap();
    tower_pal::write_private(&s.paths.state.join("allgather.key"), b"new-key").unwrap();
    let new = s
        .sync_activate(&Secret("new-key".into()), "new-node")
        .unwrap();
    assert_ne!(old, new);
    let request = Request {
        jsonrpc: "2.0".into(),
        id: None,
        method: "daemon.info".into(),
        params: value(serde_json::json!({})).unwrap(),
        context: human(),
    };
    assert_eq!(
        s.remote_dispatch(&request, old, [true, false])
            .unwrap_err()
            .data
            .code,
        "unauthorized"
    );
    assert_eq!(store.rules().unwrap().local_rules.len(), 2);
    s.remote_dispatch(&request, new, [true, false]).unwrap();
    assert_eq!(store.rules().unwrap().local_rules.len(), 2);
    assert!(
        s.sync_activate(&Secret("old-key".into()), "stale-node")
            .is_err()
    );
    assert_eq!(store.host().unwrap().node_name, "new-node");
}

#[test]
fn remote_messages_require_scopes_and_both_policy_tables() {
    let (_dir, store, mut service, calls, _, to, _) = messaging(0);
    store.set_enrollment("allgather", None).unwrap();
    tower_pal::write_private(&service.paths.state.join("allgather.key"), b"test-key").unwrap();
    let epoch = service
        .sync_activate(&Secret("test-key".into()), "machine-test")
        .unwrap();
    let request = Request {
        jsonrpc: "2.0".into(),
        id: None,
        method: "message.send".into(),
        params: value(MessageSendParams {
            to,
            body: "remote policy test".into(),
            ..Default::default()
        })
        .unwrap(),
        context: human(),
    };
    let send = |s: &Service| {
        s.send_message_origin(
            &request,
            Peer {
                uid: tower_pal::uid(),
                pid: None,
            },
            Some(epoch),
        )
    };
    assert_eq!(send(&service).unwrap_err().data.code, "denied");
    store.apply_policy(1, &[]).unwrap();
    assert_eq!(send(&service).unwrap_err().data.code, "approval_required");
    assert_eq!(store.approvals().unwrap()[0].record.from, "allgather");
    call(&service,"rules.put",serde_json::json!({"rule":{"id":"local-remote-allow","from":"allgather","to":"*","action":"allow","kind":"message","created_by":"human","created_at":1}}),human()).unwrap();
    store
        .apply_policy(
            2,
            &[Rule {
                id: "deny".into(),
                from: "*".into(),
                to: "*".into(),
                action: "deny".into(),
                kind: None,
                created_by: "allgather".into(),
                created_at: 1,
            }],
        )
        .unwrap();
    assert_eq!(send(&service).unwrap_err().data.code, "denied");
    store.apply_policy(3, &[]).unwrap();
    service.allgather_config.scopes = Some(vec!["read".into()]);
    assert_eq!(send(&service).unwrap_err().data.code, "denied");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    service
        .allgather_config
        .scopes
        .as_mut()
        .unwrap()
        .push("send".into());
    send(&service).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn allgather_cannot_administer_local_state_even_with_admin_scope() {
    let (_dir, store, mut service) = setup();
    service.allgather_config.scopes = Some(vec!["admin".into()]);
    store.set_enrollment("allgather", None).unwrap();
    tower_pal::write_private(&service.paths.state.join("allgather.key"), b"test-key").unwrap();
    let epoch = service
        .sync_activate(&Secret("test-key".into()), "machine-test")
        .unwrap();
    for method in [
        "rules.list",
        "rules.put",
        "rules.remove",
        "rules.replace",
        "token.issue",
        "token.revoke",
        "agent.register",
        "agent.list",
        "approval.list",
        "approval.resolve",
        "session.open",
        "session.kill",
        "session.snapshot",
        "auth.revoke",
        "node.rename",
        "subscribe",
        "allgather.logout",
    ] {
        let request = Request {
            jsonrpc: "2.0".into(),
            id: None,
            method: method.into(),
            params: value(serde_json::json!({"rules":[]})).unwrap(),
            context: human(),
        };
        assert_eq!(
            service
                .remote_dispatch(&request, epoch, [true, false])
                .unwrap_err()
                .data
                .code,
            "method_not_found",
            "{method}"
        );
    }
    assert_eq!(store.rules().unwrap().local_rules.len(), 2);
    assert_eq!(store.host().unwrap().mode, "allgather");
    service
        .sync_policy(
            PolicyPush {
                policy_id: "tower".into(),
                revision: 1,
                rules: vec![Rule {
                    id: "remote-deny".into(),
                    from: "*".into(),
                    to: "*".into(),
                    action: "deny".into(),
                    kind: None,
                    created_by: "allgather".into(),
                    created_at: 1,
                }],
            },
            epoch,
        )
        .unwrap();
    let rules = store.rules().unwrap();
    assert_eq!(rules.local_rules.len(), 2);
    assert_eq!(rules.allgather_rules[0].id, "remote-deny");
    let (id, _) = register(&service, "unpublished");
    assert!(
        service
            .sync_ack(&[], &[(id.clone(), "invalid".into())], epoch)
            .is_err()
    );
    assert_ne!(store.agent(&id).unwrap().record.sync_state, "rejected");
}

#[tokio::test]
async fn webhook_sinks_receive_metadata_without_approval_previews() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let (_dir, _, s, _, _, b, ctx) = messaging(0);
    let paths = s.paths.clone();
    let listener = tower_pal::loopback_listeners(0).await.unwrap().remove(0);
    let port = listener.local_addr().unwrap().port();
    std::fs::create_dir_all(paths.config_home.join("tower")).unwrap();
    std::fs::write(paths.config_home.join("tower/config.toml"),format!("[[event_sinks]]\nkind='webhook'\ntarget='http://127.0.0.1:{port}/events'\ntopics=['approvals']\n")).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut input = BufReader::new(stream);
        let mut length = 0;
        loop {
            let mut line = String::new();
            input.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':')
                && k.eq_ignore_ascii_case("content-length")
            {
                length = v.trim().parse::<usize>().unwrap();
            }
        }
        assert!(length < 65536);
        let mut bytes = vec![0; length];
        input.read_exact(&mut bytes).await.unwrap();
        input
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        bytes
    });
    let worker = crate::worker::ServiceWorker::new(s);
    let tasks = crate::sinks::start(&paths, &worker).unwrap();
    let request = Request {
        jsonrpc: "2.0".into(),
        id: None,
        method: "message.send".into(),
        params: value(serde_json::json!({"to":b,"body":"PRIVATE-PREVIEW-MARKER"})).unwrap(),
        context: ctx,
    };
    let error = worker
        .request(
            request,
            Peer {
                uid: tower_pal::uid(),
                pid: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.data.code, "approval_required");
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("approval.requested"));
    assert!(!text.contains("PRIVATE-PREVIEW-MARKER"));
    assert!(!text.contains("preview"));
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn cancelling_a_command_sink_terminates_its_process_group() {
    let (dir, _, s) = setup();
    let paths = s.paths.clone();
    let script = dir.path().join("handler.sh");
    let marker = dir.path().join("child-pid");
    std::fs::write(&script, "sleep 60 &\nprintf '%s' \"$!\" > \"$1\"\nwait\n").unwrap();
    std::fs::create_dir_all(paths.config_home.join("tower")).unwrap();
    std::fs::write(
        paths.config_home.join("tower/config.toml"),
        format!(
            "[[event_sinks]]\nkind='command'\ntarget='sh'\nargs=[{:?},{:?}]\ntopics=['agents']\n",
            script.to_string_lossy(),
            marker.to_string_lossy()
        ),
    )
    .unwrap();
    let worker = crate::worker::ServiceWorker::new(s);
    let tasks = crate::sinks::start(&paths, &worker).unwrap();
    worker
        .execute(|s| s.emit("agent.added", "fixture", serde_json::json!({})))
        .await
        .unwrap()
        .unwrap();
    let identity = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&marker)
                && let Ok(pid) = text.parse::<u32>()
                && let Ok(identity) = tower_pal::process_identity(pid)
            {
                break identity;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while tower_pal::process_identity(identity.pid)
            .is_ok_and(|p| p.start_time == identity.start_time)
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("event command left a child running");
}

#[test]
fn private_delivery_updates_merge_atomically_and_ignore_stale_sequences() {
    let (_dir, store, s, _, _, b, _) = messaging(0);
    let token = tower_pal::read_private(&s.paths.token_path(&b).unwrap()).unwrap();
    let token = String::from_utf8(token).unwrap();
    call(&s,"agent.delivery",serde_json::json!({"id":b,"token":token,"delivery":{"thread_id":"receiver","app_server":"unix:///fixture.sock","codex_home":"/first"}}),human()).unwrap();
    let registered = call(
        &s,
        "agent.register",
        serde_json::json!({"harness":"codex","native_session_id":"receiver","cwd":"~/refreshed"}),
        human(),
    )
    .unwrap();
    assert_eq!(registered["id"], b);
    assert_eq!(
        store.delivery(&b).unwrap().app_server.as_deref(),
        Some("unix:///fixture.sock")
    );
    let update = |seq, home| serde_json::json!({"id":b,"token":token,"seq":seq,"status":"working","delivery":{"codex_home":home}});
    call(&s, "agent.update", update(2, "/second"), human()).unwrap();
    assert_eq!(
        call(&s, "agent.update", update(1, "/stale"), human()).unwrap()["applied"],
        false
    );
    let endpoint = store.delivery(&b).unwrap();
    assert_eq!(endpoint.codex_home.as_deref(), Some("/second"));
    assert_eq!(endpoint.thread_id.as_deref(), Some("receiver"));
    assert_eq!(endpoint.app_server.as_deref(), Some("unix:///fixture.sock"));
    call(
        &s,
        "agent.update",
        serde_json::json!({"id":b,"token":token,"seq":3,"delivery":null}),
        human(),
    )
    .unwrap();
    assert!(store.delivery(&b).unwrap().thread_id.is_none());
    assert_eq!(store.agent(&b).unwrap().record.caps.message, "none");
}

#[test]
fn message_rules_do_not_authorize_session_operations() {
    let (_dir, store, service) = setup();
    store.set_enrollment("allgather", None).unwrap();
    store.apply_policy(1, &[]).unwrap();
    let actor = Actor::Allgather { scope: Vec::new() };
    let to = crate::policy::Subject {
        id: "*",
        harness: Some("claude"),
        cwd: Some("~/work/repo"),
    };
    for (kind, allowed) in [("message", false), ("session", true)] {
        call(
            &service,
            "rules.replace",
            serde_json::json!({"rules":[{"id":"work","from":"*","to":"cwd:~/work","action":"allow","kind":kind,"created_by":"human","created_at":1}]}),
            human(),
        )
        .unwrap();
        let result = service.session_policy(&actor, to);
        assert_eq!(result.is_ok(), allowed, "{kind}");
        if !allowed {
            assert_eq!(result.unwrap_err().data.code, "approval_required");
        }
    }
    for kind in [serde_json::Value::Null, "launch".into()] {
        let invalid = serde_json::json!({"rule":{"id":"x","from":"*","to":"*","action":"allow","kind":kind,"created_by":"human","created_at":1}});
        assert!(call(&service, "rules.put", invalid, human()).is_err());
    }
}
