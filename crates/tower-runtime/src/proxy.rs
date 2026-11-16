use crate::{Binding, Result, error, internal};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{mpsc, watch},
};
use tower_protocol::ExecutionResult;

const CAPACITY: usize = 32;
const FRAME_LIMIT: usize = tower_protocol::MAX_FRAME_BYTES;

#[derive(Clone, serde::Deserialize)]
#[serde(default)]
pub(crate) struct Configuration {
    pub agents: BTreeMap<String, Vec<String>>,
}
impl Default for Configuration {
    fn default() -> Self {
        Self {
            agents: BTreeMap::from([
                ("claude-code".into(), vec!["claude-agent-acp".into()]),
                ("codex".into(), vec!["codex-acp".into()]),
            ]),
        }
    }
}
impl Configuration {
    pub fn available(&self) -> BTreeMap<String, tower_protocol::HarnessControls> {
        let env = tower_pal::environment();
        self.agents
            .iter()
            .filter(|(_, argv)| {
                argv.first()
                    .is_some_and(|bin| tower_pal::find_program(bin, &env).is_some())
            })
            .map(|(name, _)| {
                (
                    name.clone(),
                    tower_protocol::HarnessControls {
                        protocol: "acp".into(),
                        capabilities: vec![],
                        cancel_mode: Some("runtime".into()),
                    },
                )
            })
            .collect()
    }
}

#[derive(Default)]
struct Observer {
    requests: BTreeMap<String, (String, Option<String>)>,
    prompts: std::collections::BTreeSet<String>,
    waiting: std::collections::BTreeSet<String>,
    session: Option<String>,
    initialized: bool,
    published: Option<(&'static str, Option<String>)>,
    overflow: bool,
}
impl Observer {
    fn observe(&mut self, raw: &str, from_client: bool) -> Option<(&'static str, Option<String>)> {
        let v: Value = serde_json::from_str(raw).ok()?;
        if self.requests.len() + self.waiting.len() >= 1024 {
            self.requests.clear();
            self.waiting.clear();
            self.prompts.clear();
            self.overflow = true;
        }
        if self.overflow {
            return self.publish("unknown");
        }
        let id = v.get("id").and_then(|id| serde_json::to_string(id).ok());
        if from_client {
            if let (Some(id), Some(method)) = (id.clone(), v["method"].as_str()) {
                if matches!(
                    method,
                    "initialize" | "session/new" | "session/load" | "session/prompt"
                ) {
                    self.requests.insert(
                        id.clone(),
                        (
                            method.into(),
                            v["params"]["sessionId"].as_str().map(str::to_owned),
                        ),
                    );
                    if method == "session/prompt" {
                        self.prompts.insert(id);
                    }
                }
            } else if let Some(id) = id {
                self.waiting.remove(&id);
            }
        } else if let Some(id) = id {
            if let Some(method) = v["method"].as_str() {
                if matches!(method, "session/request_permission" | "elicitation/create") {
                    self.waiting.insert(id);
                }
            } else if let Some((method, requested_session)) = self.requests.remove(&id) {
                if method == "initialize" && v.get("result").is_some() {
                    self.initialized = true;
                }
                if matches!(method.as_str(), "session/new" | "session/load")
                    && v.get("result").is_some()
                    && let Some(session) = v["result"]["sessionId"]
                        .as_str()
                        .map(str::to_owned)
                        .or(requested_session)
                {
                    self.session = Some(session);
                }
                self.prompts.remove(&id);
            }
        }
        self.publish(if !self.waiting.is_empty() {
            "waiting_input"
        } else if !self.prompts.is_empty() {
            "working"
        } else if self.initialized {
            "idle"
        } else {
            "starting"
        })
    }
    fn publish(&mut self, state: &'static str) -> Option<(&'static str, Option<String>)> {
        let snapshot = (state, self.session.clone());
        if self.published.as_ref() == Some(&snapshot) {
            return None;
        }
        self.published = Some(snapshot.clone());
        Some(snapshot)
    }
}

struct Input {
    sequence: u64,
    last: Option<String>,
    sender: mpsc::Sender<String>,
}
struct Output {
    receiver: mpsc::Receiver<(u64, String)>,
    pending: Option<(u64, String)>,
    acknowledged: u64,
}
pub(crate) struct Proxy {
    input: Mutex<Input>,
    output: Mutex<Output>,
    stop: watch::Sender<bool>,
    closed_at: Mutex<Option<tokio::time::Instant>>,
}
impl Proxy {
    pub fn send(&self, sequence: u64, frame: &str) -> Result<()> {
        if frame.is_empty() || frame.len() > FRAME_LIMIT || frame.contains('\n') {
            return Err(error(
                "invalid_frame",
                "ACP requires one bounded line per frame",
            ));
        }
        let mut input = self.input.lock().map_err(internal)?;
        if sequence == input.sequence && input.last.as_deref() == Some(frame) {
            return Ok(());
        }
        if sequence != input.sequence + 1 {
            return Err(error(
                "sequence_conflict",
                "ACP input sequence is not the next frame",
            ));
        }
        input
            .sender
            .try_send(frame.into())
            .map_err(|_| error("acp_backpressure", "ACP input is full or closed"))?;
        input.sequence = sequence;
        input.last = Some(frame.into());
        Ok(())
    }
    pub fn peek(&self) -> Result<Option<(u64, String)>> {
        let mut out = self.output.lock().map_err(internal)?;
        if out.pending.is_none() {
            out.pending = out.receiver.try_recv().ok();
        }
        Ok(out.pending.clone())
    }
    pub fn acknowledge(&self, sequence: u64) -> Result<()> {
        let mut out = self.output.lock().map_err(internal)?;
        if sequence <= out.acknowledged {
            return Ok(());
        }
        if out.pending.as_ref().map(|p| p.0) != Some(sequence) {
            return Err(error(
                "invalid_ack",
                "ACP acknowledgement exceeds the offered frame",
            ));
        }
        out.pending = None;
        out.acknowledged = sequence;
        tower_store::touch();
        Ok(())
    }
    pub fn closed(&self) -> bool {
        self.input
            .lock()
            .is_ok_and(|input| input.sender.is_closed())
    }
    pub fn retired(&self) -> bool {
        if !self.closed() {
            return false;
        }
        let Ok(mut at) = self.closed_at.lock() else {
            return false;
        };
        let at = at.get_or_insert_with(tokio::time::Instant::now);
        at.elapsed() >= Duration::from_secs(300)
            || self
                .output
                .lock()
                .is_ok_and(|out| out.pending.is_none() && out.receiver.is_empty())
    }
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
}

pub(crate) fn spawn(
    ctx: crate::engine::Context,
    b: Binding,
    command: tower_protocol::ExecutionCommand,
    argv: Vec<String>,
) -> Arc<Proxy> {
    let (sender, mut input) = mpsc::channel::<String>(CAPACITY);
    let (output, receiver) = mpsc::channel(CAPACITY);
    let (stop, mut stopped) = watch::channel(false);
    let proxy = Arc::new(Proxy {
        input: Mutex::new(Input {
            sequence: 0,
            last: None,
            sender,
        }),
        output: Mutex::new(Output {
            receiver,
            pending: None,
            acknowledged: 0,
        }),
        stop,
        closed_at: Mutex::new(None),
    });
    tokio::spawn(async move {
        let result: Result<()> = async {
            ctx.authority.check(&b, &command).await?;
            if *stopped.borrow() { return Err(error("acp_closed", "ACP connection closed before launch")); }
            ctx.journal.begin_dispatch(&b, &command.command_id)?;
            let mut env = tower_pal::environment();
            for key in ["TOWER_BEARER_TOKEN", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "CLAUDECODE"] { env.remove(key); }
            let token = tower_pal::read_private(&ctx.paths.token_path(&b.tower_session_id).map_err(internal)?).map_err(internal)?;
            env.insert("TOWER_AGENT_ID".into(), b.tower_session_id.clone());
            env.insert("TOWER_AGENT_TOKEN".into(), String::from_utf8(token).map_err(internal)?);
            env.insert("TOWER_SOCKET".into(), ctx.paths.socket.to_string_lossy().into_owned());
            let mut child = tower_pal::spawn_protocol(&argv, &env, std::path::Path::new(&b.cwd)).map_err(internal)?;
            let identity = tower_pal::process_identity(child.id().ok_or_else(|| error("acp_closed", "ACP process has no identity"))?).map_err(internal)?;
            struct Group(tower_pal::ProcessIdentity);
            impl Drop for Group { fn drop(&mut self) { let _ = tower_pal::kill_protocol_group(self.0); } }
            let _group = Group(identity);
            let mut metadata = json!({"pid":identity.pid,"pid_start_time":identity.start_time,"boot_id":tower_pal::boot_id().map_err(internal)?,"transport":"acp-proxy.v1"});
            ctx.journal.update_metadata(&b, "starting", &metadata)?;
            let receipt = ctx.journal.finish_dispatch(&b, &command.command_id, true, Some(ExecutionResult { tower_session_id:Some(b.tower_session_id.clone()),owner_id:Some(b.owner_id.clone()),..Default::default() }), None)?;
            ctx.journal.acknowledge_receipt(&b.namespace(), &command.command_id, receipt.receipt_revision)?;
            let mut stdin = child.stdin.take().ok_or_else(|| error("acp_closed", "ACP stdin missing"))?;
            let mut stdout = BufReader::new(child.stdout.take().ok_or_else(|| error("acp_closed", "ACP stdout missing"))?);
            let observer = Mutex::new(Observer::default());
            let observe = |frame: &str, client: bool| -> Result<()> {
                if let Some((state, session)) = observer.lock().map_err(internal)?.observe(frame, client) {
                    let mut m = metadata.clone();
                    if let Some(id) = session { m["acp_session_id"] = json!(id); }
                    ctx.journal.update_metadata(&b, state, &m)?;
                }
                Ok(())
            };
            let outcome: Result<()> = tokio::select! {
                biased;
                _ = stopped.changed() => Ok(()),
                result = async {
                    while let Some(frame) = input.recv().await {
                        ctx.authority.check(&b, &command).await?;
                        tokio::time::timeout(Duration::from_secs(5), async { stdin.write_all(frame.as_bytes()).await?; stdin.write_all(b"\n").await?; stdin.flush().await }).await.map_err(|_| error("acp_closed", "ACP write timed out"))?.map_err(internal)?;
                        let _ = observe(&frame, true);
                    }
                    Ok(())
                } => result,
                result = async {
                    let mut sequence = 0;
                    loop {
                        let mut bytes = Vec::new();
                        loop {
                            let available = stdout.fill_buf().await.map_err(internal)?;
                            if available.is_empty() { return if bytes.is_empty() { Ok(()) } else { Err(error("acp_closed", "Truncated ACP frame")) }; }
                            let n = available.iter().position(|b| *b == b'\n').map_or(available.len(), |n| n+1);
                            if bytes.len() + n > FRAME_LIMIT + 1 { return Err(error("frame_too_large", "ACP output exceeds frame limit")); }
                            let done = available[n-1] == b'\n';
                            bytes.extend_from_slice(&available[..n]); stdout.consume(n);
                            if done { bytes.pop(); break; }
                        }
                        let frame = String::from_utf8(bytes).map_err(internal)?;
                        let _ = observe(&frame, false);
                        sequence += 1;
                        output.send((sequence, frame)).await.map_err(internal)?;
                        tower_store::touch();
                    }
                } => result,
            };
            let _ = tower_pal::kill_protocol_group(identity);
            let _ = tokio::time::timeout(Duration::from_secs(5),child.wait()).await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
            while !tower_pal::protocol_group_terminated(identity).unwrap_or(false) && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            metadata = ctx.journal.metadata(&b).unwrap_or(metadata);
            let _ = ctx.journal.update_metadata(&b, "ended", &metadata);
            ctx.journal.fence(&b, if outcome.is_ok() { "acp_closed" } else { "acp_transport_error" }, if tower_pal::protocol_group_terminated(identity).unwrap_or(false) { "confirmed" } else { "unknown" })?;
            outcome
        }.await;
        if let Err(e) = result {
            let _ = ctx.journal.fence(&b, &e.data.code, "unknown");
        }
    });
    proxy
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn load_identity_is_observed_when_the_native_response_has_no_session_id() {
        let mut observer = Observer::default();
        observer.observe(
            r#"{"id":1,"method":"session/load","params":{"sessionId":"restored"}}"#,
            true,
        );
        assert_eq!(
            observer
                .observe(r#"{"id":1,"result":{}}"#, false)
                .unwrap()
                .1
                .as_deref(),
            Some("restored")
        );
    }
    #[test]
    fn observer_does_not_classify_unknown_extensions_as_turns() {
        let mut observer = Observer::default();
        observer.observe(
            r#"{"jsonrpc":"2.0","id":"init","method":"initialize","params":{}}"#,
            true,
        );
        assert_eq!(
            observer
                .observe(r#"{"jsonrpc":"2.0","id":"init","result":{}}"#, false)
                .unwrap()
                .0,
            "idle"
        );
        observer.observe(
            r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{}}"#,
            true,
        );
        assert!(
            observer
                .observe(
                    r#"{"method":"vendor/future","params":{"arbitrary":true}}"#,
                    false
                )
                .is_none()
        );
        assert_eq!(
            observer
                .observe(r#"{"id":1,"result":{"stopReason":"end_turn"}}"#, false)
                .unwrap()
                .0,
            "idle"
        );
    }
}
