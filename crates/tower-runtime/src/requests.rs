use crate::{Result, error, internal};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tokio::time::Instant;
use tower_protocol::{RequestId, RuntimeRequest};

pub(crate) struct LiveRequest {
    wire: RuntimeRequest,
    bytes: usize,
    after_sequence: u64,
    method: String,
    deny_decision: String,
    announced: Option<(String, u64)>,
    peer_retired: Option<Instant>,
}
impl LiveRequest {
    pub fn new(wire: RuntimeRequest, method: &str, deny_decision: &str) -> Result<Self> {
        let bytes = serde_json::to_vec(&wire).map_err(internal)?.len();
        if bytes > 256 * 1024 {
            return Err(error(
                "control_capacity",
                "Private request exceeds its size limit",
            ));
        }
        Ok(Self {
            wire,
            bytes,
            method: method.into(),
            deny_decision: deny_decision.into(),
            after_sequence: 0,
            announced: None,
            peer_retired: None,
        })
    }
    fn retire(&mut self, state: &str) -> Result<()> {
        if self.wire.state == "pending" {
            self.wire.state = state.into();
            self.wire.payload = tower_protocol::Value::Null;
            self.wire.request_revision += 1;
            self.bytes = serde_json::to_vec(&self.wire).map_err(internal)?.len();
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Requests {
    items: Mutex<BTreeMap<String, LiveRequest>>,
}
impl Requests {
    pub fn insert(&self, mut request: LiveRequest, after_sequence: u64) -> Result<()> {
        let mut items = self.items.lock().map_err(internal)?;
        if items.values().any(|r| {
            r.wire.state == "pending" && r.wire.native_request_id == request.wire.native_request_id
        }) {
            return Err(error(
                "native_request_conflict",
                "Native request identity was reused before completion",
            ));
        }
        if items.len() >= 32
            || items.values().map(|r| r.bytes).sum::<usize>() + request.bytes > 1024 * 1024
        {
            return Err(error(
                "control_capacity",
                "Too many pending native requests",
            ));
        }
        request.after_sequence = after_sequence;
        items.insert(request.wire.source_request_id.clone(), request);
        tower_store::touch();
        Ok(())
    }
    pub fn validate_reply(&self, params: &Value) -> Result<()> {
        let items = self.items.lock().map_err(internal)?;
        let request = items
            .get(params["source_request_id"].as_str().unwrap_or(""))
            .ok_or_else(|| error("stale_request", "Private request is no longer live"))?;
        validate_reply(request, params)?;
        Ok(())
    }
    pub fn take_reply(&self, params: &Value) -> Result<(RequestId, Value)> {
        let mut items = self.items.lock().map_err(internal)?;
        let key = params["source_request_id"].as_str().unwrap_or("");
        let request = items
            .get(key)
            .ok_or_else(|| error("stale_request", "Private request is no longer live"))?;
        let response = validate_reply(request, params)?;
        let id = request.wire.native_request_id.clone();
        items.remove(key);
        tower_store::touch();
        Ok((id, response))
    }
    pub fn retire_all(&self) -> Result<()> {
        for request in self.items.lock().map_err(internal)?.values_mut() {
            request.retire("stale")?;
        }
        tower_store::touch();
        Ok(())
    }
    pub fn retire_native(&self, id: &Value, state: &str) -> Result<()> {
        let Ok(id) = serde_json::from_value::<RequestId>(id.clone()) else {
            return Ok(());
        };
        for request in self.items.lock().map_err(internal)?.values_mut() {
            if request.wire.native_request_id == id {
                request.retire(state)?;
            }
        }
        tower_store::touch();
        Ok(())
    }
    pub fn updates(
        &self,
        connection: &str,
        committed: u64,
        budget: &mut usize,
    ) -> Result<Vec<RuntimeRequest>> {
        let mut updates = Vec::new();
        for request in self.items.lock().map_err(internal)?.values() {
            if (request.wire.state == "pending" && request.after_sequence > committed)
                || request
                    .announced
                    .as_ref()
                    .is_some_and(|(c, r)| c == connection && *r >= request.wire.request_revision)
                || request.bytes > *budget
            {
                continue;
            }
            *budget -= request.bytes;
            updates.push(request.wire.clone());
        }
        Ok(updates)
    }
    pub fn acknowledge(
        &self,
        id: &str,
        revision: u64,
        connection: &str,
        state: &str,
    ) -> Result<()> {
        let mut items = self.items.lock().map_err(internal)?;
        if items
            .get(id)
            .is_some_and(|r| r.wire.state != "pending" && r.wire.request_revision <= revision)
        {
            items.remove(id);
        } else if let Some(r) = items.get_mut(id)
            && revision <= r.wire.request_revision
        {
            if matches!(state, "answered" | "cancelled" | "stale") {
                r.peer_retired.get_or_insert_with(Instant::now);
            }
            r.announced = Some((connection.into(), revision));
        }
        Ok(())
    }
    pub fn check_reply_timeout(&self) -> Result<()> {
        if self.items.lock().map_err(internal)?.values().any(|r| {
            r.wire.state == "pending"
                && r.peer_retired
                    .is_some_and(|at| at.elapsed() >= Duration::from_secs(30))
        }) {
            return Err(error(
                "private_reply_lost",
                "The peer retired a request without a native reply",
            ));
        }
        Ok(())
    }
}

fn validate_reply(request: &LiveRequest, p: &Value) -> Result<Value> {
    if request.wire.state != "pending"
        || p["source_request_id"] != request.wire.source_request_id
        || p["request_revision"] != request.wire.request_revision
        || p["generation"] != request.wire.generation
    {
        return Err(error("stale_request", "Request is no longer live"));
    }
    let r = &p["response"];
    if request.wire.kind == "elicitation" {
        let payload = serde_json::to_value(&request.wire.payload).map_err(internal)?;
        return crate::controls::answer(&request.method, &payload, r);
    }
    let option = if r.pointer("/outcome/outcome").and_then(Value::as_str) == Some("cancelled") {
        "reject_once"
    } else if r.pointer("/outcome/outcome").and_then(Value::as_str) == Some("selected") {
        r.pointer("/outcome/optionId")
            .and_then(Value::as_str)
            .unwrap_or("")
    } else {
        ""
    };
    let payload = serde_json::to_value(&request.wire.payload).map_err(internal)?;
    if !payload["options"]
        .as_array()
        .is_some_and(|a| a.iter().any(|o| o["optionId"] == option))
    {
        return Err(error(
            "invalid_response",
            "Choose a current offered permission option",
        ));
    }
    let allow = option == "allow_once";
    match request.method.as_str() {
        "session/request_permission" => Ok(r.clone()),
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Ok(json!({"decision":if allow{"accept"}else{&request.deny_decision}}))
        }
        _ => Err(error(
            "unsupported",
            "Native request has no supported reply",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permission() -> LiveRequest {
        let wire = serde_json::from_value(json!({
            "runtime_id":"runtime", "generation":"generation", "owner_id":"owner",
            "source_request_id":"occurrence", "request_revision":1, "native_request_id":42,
            "kind":"permission", "state":"pending", "payload":{
                "options":[{"optionId":"allow_once"},{"optionId":"reject_once"}],
                "private":"PRIVATE-LIVE-REQUEST"
            }
        }))
        .unwrap();
        LiveRequest::new(wire, "session/request_permission", "cancel").unwrap()
    }
    fn updates(requests: &Requests, connection: &str, sequence: u64) -> Vec<RuntimeRequest> {
        let mut budget = usize::MAX;
        requests.updates(connection, sequence, &mut budget).unwrap()
    }

    #[test]
    fn publication_waits_for_events_and_retirement_is_private_and_idempotent() {
        let requests = Requests::default();
        requests.insert(permission(), 10).unwrap();
        assert!(updates(&requests, "connection", 9).is_empty());
        assert_eq!(updates(&requests, "connection", 10).len(), 1);
        requests
            .acknowledge("occurrence", 1, "connection", "pending")
            .unwrap();
        assert!(updates(&requests, "connection", 10).is_empty());
        assert_eq!(updates(&requests, "reconnect", 10).len(), 1);
        requests.retire_native(&json!(42), "cancelled").unwrap();
        requests.retire_all().unwrap();
        requests
            .acknowledge("occurrence", 1, "connection", "pending")
            .unwrap();
        let retired = updates(&requests, "connection", 0);
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].state, "cancelled");
        assert_eq!(retired[0].request_revision, 2);
        assert_eq!(retired[0].payload, tower_protocol::Value::Null);
        assert!(
            !serde_json::to_string(&retired)
                .unwrap()
                .contains("PRIVATE-LIVE-REQUEST")
        );
        requests
            .acknowledge("occurrence", 2, "connection", "cancelled")
            .unwrap();
        assert!(updates(&requests, "reconnect", 10).is_empty());
    }

    #[test]
    fn invalid_reply_does_not_consume_the_live_occurrence() {
        let requests = Requests::default();
        requests.insert(permission(), 1).unwrap();
        let mut reply = json!({"source_request_id":"occurrence","request_revision":2,"generation":"generation",
            "response":{"outcome":{"outcome":"selected","optionId":"allow_once"}}});
        assert_eq!(
            requests.take_reply(&reply).unwrap_err().data.code,
            "stale_request"
        );
        reply["request_revision"] = json!(1);
        requests.validate_reply(&reply).unwrap();
        let (id, response) = requests.take_reply(&reply).unwrap();
        assert_eq!(serde_json::to_value(id).unwrap(), json!(42));
        assert_eq!(response, reply["response"]);
        assert_eq!(
            requests.take_reply(&reply).unwrap_err().data.code,
            "stale_request"
        );
    }
}
