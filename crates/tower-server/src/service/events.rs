use super::*;

#[derive(Default)]
pub(super) struct Published {
    pub seq: u64,
    pub records: BTreeMap<String, AgentRecord>,
    /// The registry version `records` was read at.
    pub version: Option<(u64, u64)>,
}
/// Whether a change to a record can change what a connection acting for it,
/// or serving its clipboard, may do. A live session's status, detail and
/// other observations cannot; its identity or its end can.
fn alters_authority(before: &AgentRecord, after: &AgentRecord) -> bool {
    let ended = |record: &AgentRecord| matches!(record.status.as_str(), "exited" | "lost");
    let identity = |record: &AgentRecord| {
        let mut record = record.clone();
        record.status = String::new();
        record.status_detail = None;
        record.display_name = None;
        record.caps = after.caps.clone();
        record.tags = Default::default();
        (record.attach_count, record.needs_attention) = (0, false);
        (record.updated_at, record.last_seen, record.revision) = (0, 0, 0);
        record.sync_state = String::new();
        record
    };
    ended(before) || ended(after) || identity(before) != identity(after)
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
    /// Tells connections to check again what their credentials allow.
    pub(super) fn authority_changed(&self) {
        self.authority.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub(super) fn publish(&self) -> std::result::Result<(), RpcError> {
        self.wake.replan();
        let mut state = self
            .published
            .lock()
            .map_err(|_| app("internal_error", "Event state unavailable"))?;
        // Most calls follow a request that changed no record.
        let version = self.store.agents_version().map_err(storage)?;
        if state.version == Some(version) {
            return Ok(());
        }
        let records: BTreeMap<_, _> = self.all()?.into_iter().map(|r| (r.id.clone(), r)).collect();
        let mut authority = false;
        for (id, record) in &records {
            let before = state.records.get(id);
            if before.is_some_and(|r| r.revision == record.revision) {
                continue;
            }
            authority |= before.is_none_or(|before| alters_authority(before, record));
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
        authority |= !removed.is_empty();
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
        state.version = Some(version);
        drop(state);
        if authority {
            self.authority_changed();
        }
        Ok(())
    }
}
