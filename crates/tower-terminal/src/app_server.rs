use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tower_agents::{
    Harness,
    config::Invocation,
    events::{self, AgentEvent},
    runtime::LauncherPlan,
};

pub struct Submission {
    pub thread: String,
    pub text: String,
    pub result:
        tokio::sync::oneshot::Sender<tower_sessions::Result<tower_protocol::DeliveryReceipt>>,
}
pub struct Server {
    pub child: tokio::process::Child,
    relay: tokio::task::JoinHandle<()>,
    pub messages: mpsc::Sender<Submission>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.relay.abort();
    }
}
impl Server {
    pub async fn shutdown(&mut self) {
        self.relay.abort();
        let _ = (&mut self.relay).await;
        if tokio::time::timeout(Duration::from_millis(500), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.start_kill();
            let _ = self.child.wait().await;
        }
    }
}
pub async fn start(
    plan: &LauncherPlan,
    invocation: &mut Invocation,
    events: mpsc::Sender<AgentEvent>,
) -> io::Result<Server> {
    let relay_path = Path::new(&plan.control_socket).with_extension("relay.sock");
    let argv = vec![
        "codex".into(),
        "app-server".into(),
        "--stdio".into(),
        "-c".into(),
        format!(
            "mcp_servers.tower.command={}",
            serde_json::to_string(&tower_pal::executable()?.to_string_lossy())?
        ),
        "-c".into(),
        "mcp_servers.tower.args=[\"mcp\"]".into(),
    ];
    let mut child = tower_pal::spawn_service(&argv, &invocation.env, Path::new(&plan.spec.cwd))?;
    let input = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("missing server input"))?;
    let output = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("missing server output"))?;
    let listener = tower_pal::IpcListener::bind(&relay_path).await?;
    let (messages, submissions) = mpsc::channel(16);
    let relay = tokio::spawn(async move {
        let _ = bridge(listener, input, output, events, submissions).await;
    });
    invocation.argv.splice(
        1..1,
        [
            "--remote".into(),
            format!("unix://{}", relay_path.display()),
        ],
    );
    Ok(Server {
        child,
        relay,
        messages,
    })
}
async fn bridge(
    listener: tower_pal::IpcListener,
    mut input: tokio::process::ChildStdin,
    output: tokio::process::ChildStdout,
    events: mpsc::Sender<AgentEvent>,
    mut submissions: mpsc::Receiver<Submission>,
) -> io::Result<()> {
    let (client, _) = listener.accept().await?;
    let limits = WebSocketConfig::default()
        .max_message_size(Some(16 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
        .max_write_buffer_size(17 * 1024 * 1024);
    let mut client = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::accept_async_with_config(client, Some(limits)),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay handshake timed out"))?
    .map_err(io::Error::other)?;
    let (lines, mut incoming) = mpsc::channel(8);
    let reader = tokio::spawn(async move {
        let mut reader = BufReader::new(output);
        loop {
            let mut bytes = Vec::new();
            match (&mut reader)
                .take(16 * 1024 * 1024 + 1)
                .read_until(b'\n', &mut bytes)
                .await
            {
                Ok(n) if n > 0 && n <= 16 * 1024 * 1024 && bytes.last() == Some(&b'\n') => {
                    if lines.send(bytes).await.is_err() {
                        break;
                    }
                }
                _ => break,
            }
        }
    });
    struct Guard(tokio::task::JoinHandle<()>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _reader = Guard(reader);
    let mut monitor = Monitor::default();
    let mut pending = BTreeMap::new();
    let mut next_id = 0u64;
    loop {
        tokio::select! {
            Some(submission)=submissions.recv()=>{
                pending.retain(|_, reply: &mut tokio::sync::oneshot::Sender<tower_sessions::Result<tower_protocol::DeliveryReceipt>>|!reply.is_closed());
                if monitor.thread.as_deref()!=Some(&submission.thread) || pending.len()>=16 {
                    let _=submission.result.send(Err(tower_protocol::RpcError::application("delivery_failed","Target thread is not available on the relay")));
                    continue;
                }
                let id=format!("tower:message:{next_id}"); next_id=next_id.wrapping_add(1);
                let request=json!({"id":id,"method":"turn/start","params":{"threadId":submission.thread,"input":[{"type":"text","text":submission.text,"text_elements":[]}]}});
                pending.insert(id,submission.result);
                let mut bytes=serde_json::to_vec(&request)?; bytes.push(b'\n');
                tokio::time::timeout(Duration::from_secs(10),input.write_all(&bytes)).await.map_err(|_|io::Error::other("server input timed out"))??;
            }
            frame=client.next()=>{
                let Some(frame)=frame else{return Ok(())};
                match frame.map_err(io::Error::other)? {
                    Message::Text(text)=>{
                        // Each WS message must contain one JSON value. Embedded newlines
                        // are normalized so they cannot become extra stdio requests.
                        let value:Value=serde_json::from_str(&text)?;
                        if value["id"].as_str().is_some_and(|id|id.starts_with("tower:message:")) {return Err(io::Error::other("reserved relay request ID"));}
                        if let Some(event)=monitor.observe(&value,false) {events.try_send(event).map_err(io::Error::other)?;}
                        let mut bytes=serde_json::to_vec(&value)?;bytes.push(b'\n');
                        tokio::time::timeout(Duration::from_secs(10),input.write_all(&bytes)).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"server input timed out"))??;
                    }
                    Message::Close(_)=>return Ok(()),
                    Message::Ping(_)=>client.flush().await.map_err(io::Error::other)?,
                    _=>{},
                }
            }
            line=incoming.recv()=>{
                let Some(line)=line else{return Ok(())};
                let value:Value=serde_json::from_slice(&line)?;
                if value.get("method").is_none() && let Some(reply)=value["id"].as_str().and_then(|id|pending.remove(id)) {
                    let result=if value.get("error").is_some() {Err(tower_protocol::RpcError::application("delivery_failed","Codex rejected the turn"))}else if value.pointer("/result/turn/id").and_then(Value::as_str).is_some() {Ok(tower_protocol::DeliveryReceipt{method:"native".into(),acknowledgement:"harness".into()})}else{Err(tower_protocol::RpcError::application("delivery_unknown","Unexpected Codex turn response; do not automatically retry"))};
                    let _=reply.send(result); continue;
                }
                if let Some(event)=monitor.observe(&value,true) {events.try_send(event).map_err(io::Error::other)?;}
                let text=String::from_utf8(line).map_err(io::Error::other)?;
                tokio::time::timeout(Duration::from_secs(10),client.send(Message::Text(text.into()))).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"client output timed out"))?.map_err(io::Error::other)?;
            }
        }
    }
}
#[derive(Default)]
struct Monitor {
    pending: BTreeMap<String, String>,
    approvals: BTreeSet<String>,
    thread: Option<String>,
}
impl Monitor {
    fn observe(&mut self, value: &Value, server: bool) -> Option<AgentEvent> {
        let method = value["method"].as_str();
        let key = value.get("id").map(Value::to_string);
        if !server {
            if let (Some(method), Some(key)) = (method, key.as_ref()) {
                if matches!(method, "thread/start" | "thread/resume" | "thread/fork")
                    && self.pending.len() < 64
                    && !background(&value["params"])
                    && !(method == "thread/fork"
                        && value["params"]["ephemeral"] == true
                        && value["params"]["threadId"].as_str() == self.thread.as_deref())
                {
                    self.pending.insert(key.clone(), method.into());
                }
            } else if key.as_ref().is_some_and(|key| self.approvals.remove(key))
                && self.approvals.is_empty()
            {
                return self.event("PermissionResult", json!({}));
            }
            return None;
        }
        if method.is_none() {
            self.pending.remove(&key?)?;
            let thread = &value["result"]["thread"];
            let id = thread["id"].as_str().filter(|s| events::native_id(s))?;
            if !thread["parentThreadId"].is_null() || background(thread) {
                return None;
            }
            self.thread = Some(id.into());
            let mut event=self.event("SessionStart",json!({"model":value["result"]["model"],"session_title":thread["name"],"cwd":thread["cwd"]}))?;
            event.authoritative_status = status(&thread["status"]);
            event.resumable = Some(thread["ephemeral"] != true);
            return Some(event);
        }
        let p = &value["params"];
        if self.thread.is_none() || p["threadId"].as_str() != self.thread.as_deref() {
            return None;
        }
        match method? {
            "turn/started" => self.event("UserPromptSubmit", json!({})),
            "turn/completed" => self.event(
                if p["turn"]["status"] == "failed" {
                    "StopFailure"
                } else {
                    "Stop"
                },
                json!({}),
            ),
            "thread/status/changed" => {
                let mut e = self.event("metadata", json!({}))?;
                e.authoritative_status = status(&p["status"]);
                Some(e)
            }
            "thread/name/updated" => {
                self.event("metadata", json!({"session_title":p["threadName"]}))
            }
            "thread/settings/updated" => self.event("metadata", json!({"model":p["model"]})),
            "thread/tokenUsage/updated" => self.event(
                "metadata",
                json!({"context_tokens":p["tokenUsage"]["last"]["totalTokens"]}),
            ),
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                if let Some(key) = key {
                    self.approvals.insert(key);
                }
                self.event("PermissionRequest", json!({}))
            }
            "item/tool/requestUserInput" | "mcpServer/elicitation/request" => {
                if let Some(key) = key {
                    self.approvals.insert(key);
                }
                let mut e = self.event("PreToolUse", json!({"tool_name":"AskUserQuestion"}))?;
                e.authoritative_status = Some("waiting_input".into());
                Some(e)
            }
            "item/started" | "item/completed" => {
                let kind = p["item"]["type"].as_str()?;
                if matches!(kind, "agentMessage" | "userMessage" | "reasoning") {
                    return None;
                }
                self.event(
                    if method == Some("item/started") {
                        "PreToolUse"
                    } else {
                        "PostToolUse"
                    },
                    json!({"tool_name":kind,"tool_call_id":p["item"]["id"]}),
                )
            }
            _ => None,
        }
    }
    fn event(&self, name: &str, mut payload: Value) -> Option<AgentEvent> {
        payload["session_id"] = json!(self.thread);
        events::parse(Harness::Codex, name, &serde_json::to_vec(&payload).ok()?).ok()
    }
}
fn background(value: &Value) -> bool {
    value["threadSource"]
        .as_str()
        .is_some_and(|source| source != "user")
}
fn status(value: &Value) -> Option<String> {
    let status = match value["type"].as_str()? {
        "idle" => "idle",
        "active" => {
            if value["activeFlags"]
                .as_array()
                .is_some_and(|flags| flags.iter().any(|v| v == "waitingOnApproval"))
            {
                "waiting_approval"
            } else if value["activeFlags"]
                .as_array()
                .is_some_and(|flags| flags.iter().any(|v| v == "waitingOnUserInput"))
            {
                "waiting_input"
            } else {
                "working"
            }
        }
        "systemError" => "unknown",
        _ => return None,
    };
    Some(status.into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_successful_foreground_thread_selection_can_change_identity() {
        let mut m = Monitor::default();
        assert!(m.observe(&json!({"method":"thread/status/changed","params":{"threadId":"child","status":{"type":"idle"}}}),true).is_none());
        m.observe(&json!({"id":1,"method":"thread/start"}), false);
        let e=m.observe(&json!({"id":1,"result":{"thread":{"id":"root","cwd":"/work","status":{"type":"idle"}}}}),true).unwrap();
        assert_eq!(e.native_id.as_deref(), Some("root"));
        m.observe(&json!({"id":2,"method":"thread/start","params":{"threadSource":"system","ephemeral":true}}),false);
        assert!(m.observe(&json!({"id":2,"result":{"thread":{"id":"recap","threadSource":"system","ephemeral":true}}}),true).is_none());
        assert_eq!(m.thread.as_deref(), Some("root"));
        assert!(
            m.observe(
                &json!({"method":"turn/started","params":{"threadId":"child"}}),
                true
            )
            .is_none()
        );
        let e=m.observe(&json!({"id":"a","method":"item/commandExecution/requestApproval","params":{"threadId":"root"}}),true).unwrap();
        assert_eq!(e.kind, events::EventKind::Approval);
        assert_eq!(
            m.observe(&json!({"id":"a","result":{}}), false)
                .unwrap()
                .kind,
            events::EventKind::ApprovalResolved
        );
    }
}
