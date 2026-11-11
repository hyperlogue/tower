use super::*;

#[derive(Default)]
pub(super) struct Published {
    pub seq: u64,
    pub records: BTreeMap<String, AgentRecord>,
}
impl Service {
    pub(super) fn emit(
        &self,
        method: &str,
        id: &str,
        data: serde_json::Value,
    ) -> std::result::Result<(), RpcError> {
        let Value::Object(data) = value(data)? else {
            return Err(app("internal_error", "Invalid event payload"));
        };
        let mut state = self
            .published
            .lock()
            .map_err(|_| app("internal_error", "Event state unavailable"))?;
        state.seq = state.seq.saturating_add(1);
        let _ = self.events.send(Arc::new(EventNotification {
            jsonrpc: "2.0".into(),
            method: method.into(),
            params: EventParams {
                seq: state.seq,
                id: id.into(),
                record: None,
                data,
            },
        }));
        Ok(())
    }
    pub(crate) fn event_visible(
        &self,
        context: &CallerContext,
        peer: Peer,
        event: &EventNotification,
    ) -> std::result::Result<bool, RpcError> {
        let actor = self.actor(context, peer)?;
        require(&actor, "subscribe")?;
        Ok(!event.method.starts_with("approval.")
            || actor.authorized("approve")
            || event.params.data.get("from") == Some(&Value::String(actor.id())))
    }

    pub(crate) fn events(&self) -> tokio::sync::broadcast::Receiver<Arc<EventNotification>> {
        self.events.subscribe()
    }
    pub(crate) fn check_subscription(
        &self,
        context: &CallerContext,
        peer: Peer,
    ) -> std::result::Result<(), RpcError> {
        require(&self.actor(context, peer)?, "subscribe")
    }
    pub(super) fn publish(&self) -> std::result::Result<(), RpcError> {
        self.wake.replan();
        self.authority.send_modify(|n| *n = n.wrapping_add(1));
        let records: BTreeMap<_, _> = self.all()?.into_iter().map(|r| (r.id.clone(), r)).collect();
        let mut state = self
            .published
            .lock()
            .map_err(|_| app("internal_error", "Event state unavailable"))?;
        for (id, record) in &records {
            let before = state.records.get(id);
            if before.is_some_and(|r| r.revision == record.revision) {
                continue;
            }
            let mut methods = vec![if before.is_some() {
                "agent.changed"
            } else {
                "agent.added"
            }];
            if let Some(old) = before {
                if record.status == "waiting_approval" && old.status != "waiting_approval" {
                    methods.push("agent.waiting_approval");
                }
                if record.ownership == "owned" {
                    if record.status == "exited" && old.status != "exited" {
                        methods.push("session.exited");
                        if matches!(&record.status_detail,Some(Value::Object(d)) if d.get("error")==Some(&Value::String("session_resume_failed".into())))
                        {
                            methods.push("session.resume_failed");
                        }
                    }
                    if record.status == "starting" && old.status != "starting" {
                        methods.push("session.resumed");
                    }
                }
            }
            for method in methods {
                state.seq = state.seq.saturating_add(1);
                let _ = self.events.send(Arc::new(EventNotification {
                    jsonrpc: "2.0".into(),
                    method: method.into(),
                    params: EventParams {
                        seq: state.seq,
                        id: id.clone(),
                        record: Some(record.clone()),
                        data: Default::default(),
                    },
                }));
            }
        }
        let removed: Vec<_> = state
            .records
            .keys()
            .filter(|id| !records.contains_key(*id))
            .cloned()
            .collect();
        for id in removed {
            state.seq = state.seq.saturating_add(1);
            let _ = self.events.send(Arc::new(EventNotification {
                jsonrpc: "2.0".into(),
                method: "agent.removed".into(),
                params: EventParams {
                    seq: state.seq,
                    id,
                    record: None,
                    data: Default::default(),
                },
            }));
        }
        state.records = records;
        Ok(())
    }
}
