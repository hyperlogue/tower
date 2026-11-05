use crate::{Result, error, internal, random_id, wire};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::{
    io::BufReader,
    process::{Child, ChildStdin},
    sync::mpsc,
};

fn response(id: &Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn failure(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn uuid() -> Result<String> {
    let id = random_id("")?;
    Ok(format!(
        "{}-{}-4{}-a{}-{}",
        &id[..8],
        &id[8..12],
        &id[13..16],
        &id[17..20],
        &id[20..]
    ))
}
struct Native {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Result<Value>>,
    reader: tokio::task::JoinHandle<()>,
}
impl Drop for Native {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
impl Native {
    async fn start(harness: &str, cwd: &str, native_id: &str, resume: bool) -> Result<Self> {
        let mut argv: Vec<String> = if harness == "claude-code" {
            [
                "claude",
                "--print",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--verbose",
                "--permission-prompt-tool",
                "stdio",
                "--permission-prompts",
                "host",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        } else {
            [
                "agy",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--disable-slash-commands",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        };
        if harness == "claude-code" {
            argv.extend(["--mcp-config".into(),json!({"mcpServers":{"tower":{"command":tower_pal::Program::Cli.path().map_err(internal)?,"args":["mcp"]}}}).to_string()]);
            argv.extend([
                if resume { "--resume" } else { "--session-id" }.into(),
                native_id.into(),
            ]);
        } else if resume {
            argv.extend(["--conversation".into(), native_id.into()]);
        }
        let mut env = tower_pal::environment();
        for key in ["TOWER_BEARER_TOKEN", "CLAUDECODE"] {
            env.remove(key);
        }
        let mut child = tower_pal::spawn_protocol_child(&argv, &env, std::path::Path::new(cwd))
            .map_err(|_| error("native_unavailable", "Cannot start native CLI"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| error("native_protocol", "Missing native stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| error("native_protocol", "Missing native stdout"))?;
        let (tx, output) = mpsc::channel(32);
        let reader = tokio::spawn(async move {
            let mut r = BufReader::new(stdout);
            loop {
                match wire::line(&mut r).await {
                    Ok(Some(v)) => {
                        if tx.send(Ok(v)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        Ok(Self {
            child,
            input,
            output,
            reader,
        })
    }
    async fn send(&mut self, value: Value) -> Result<()> {
        wire::json_line(&mut self.input, &value).await
    }
}

pub async fn run(harness: &str) -> Result<()> {
    if !matches!(harness, "claude-code" | "antigravity") {
        return Err(error("unsupported", "No ACP bridge for this harness"));
    }
    let (tx, mut input) = mpsc::channel(32);
    let reader = tokio::spawn(async move {
        let mut r = BufReader::new(tokio::io::stdin());
        loop {
            match wire::line(&mut r).await {
                Ok(Some(v)) => {
                    if tx.send(Ok(v)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });
    let mut out = tokio::io::stdout();
    let mut initialized = false;
    let mut forms = false;
    let mut native: Option<Native> = None;
    let mut session = String::new();
    let mut native_id = String::new();
    let mut creation: Option<(Value, bool)> = None;
    let mut prompt: Option<Value> = None;
    let mut permissions: BTreeMap<String, (String, Value, Option<Value>)> = BTreeMap::new();
    let mut cancelled = false;
    let mut text_seen = false;
    let result:Result<()>=async {loop {
        tokio::select! {
            biased;
            message=input.recv()=>{
                let Some(message) = message else { break Ok(()) };
                let message = message?;
                let id = message.get("id").cloned().unwrap_or(Value::Null);
                if message.get("method").is_none() {
                    if let Some((request_id, mut original, form)) =
                        id.as_str().and_then(|s| permissions.remove(s))
                    {
                        let answer = if let Some(form) = form {
                            let result =
                                crate::controls::answer("elicitation/create", &form, &message["result"])?;
                            if result["action"] == "accept" {
                                let mut answers = serde_json::Map::new();
                                for (index, q) in original["questions"]
                                    .as_array()
                                    .ok_or_else(|| error("native_protocol", "Question state is unavailable"))?
                                    .iter()
                                    .enumerate()
                                {
                                    let value = &result["content"][format!("question_{index}")];
                                    let text = if let Some(values) = value.as_array() {
                                        values
                                            .iter()
                                            .filter_map(Value::as_str)
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    } else {
                                        value.as_str().unwrap_or("").into()
                                    };
                                    answers.insert(q["question"].as_str().unwrap_or("").into(), json!(text));
                                }
                                original["answers"] = Value::Object(answers);
                                json!({"behavior":"allow","updatedInput":original})
                            } else {
                                json!({"behavior":"deny","message":"The operator declined this question"})
                            }
                        } else {
                            let allow = message
                                .pointer("/result/outcome/optionId")
                                .and_then(Value::as_str)
                                == Some("allow_once")
                                && message
                                    .pointer("/result/outcome/outcome")
                                    .and_then(Value::as_str)
                                    == Some("selected");
                            if allow {
                                json!({"behavior":"allow","updatedInput":original})
                            } else {
                                json!({"behavior":"deny","message":"Permission denied by the operator"})
                            }
                        };
                        if let Some(n) = native.as_mut() {
                            n.send(json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":answer}})).await?;
                        }
                    }
                    continue;
                }
                let p = &message["params"];
                let method = message["method"].as_str().unwrap_or("");
                let reply = match method {
                    "initialize" if !initialized => {
                        if p["protocolVersion"] != 1 {
                            Some(failure(&id, -32602, "Only ACP version 1 is supported"))
                        } else {
                            initialized = true;
                            forms = p
                                .pointer("/clientCapabilities/elicitation/form")
                                .is_some_and(|v| !v.is_null());
                            Some(response(
                                &id,
                                json!({"protocolVersion":1,"agentInfo":{"name":format!("tower-{harness}"),"version":env!("CARGO_PKG_VERSION")},"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false}},"authMethods":[]}),
                            ))
                        }
                    }
                    "session/new" | "session/load" if initialized && native.is_none() => {
                        if !p["mcpServers"].as_array().is_some_and(Vec::is_empty) {
                            Some(failure(
                                &id,
                                -32602,
                                "This bridge uses the harness's installed MCP configuration",
                            ))
                        } else {
                            let cwd = p["cwd"]
                                .as_str()
                                .filter(|s| std::path::Path::new(s).is_absolute())
                                .ok_or_else(|| error("invalid_params", "Absolute cwd required"))?;
                            let resume = method == "session/load";
                            session = if resume {
                                p["sessionId"]
                                    .as_str()
                                    .filter(|s| crate::identifier(s))
                                    .ok_or_else(|| error("invalid_params", "Invalid ACP session"))?
                                    .into()
                            } else {
                                random_id("acp_")?
                            };
                            native_id = if resume {
                                p.pointer("/_meta/tower.nativeSessionId")
                                    .and_then(Value::as_str)
                                    .filter(|s| crate::identifier(s))
                                    .ok_or_else(|| error("invalid_params", "Native resume identity required"))?
                                    .into()
                            } else if harness == "claude-code" {
                                uuid()?
                            } else {
                                String::new()
                            };
                            let mut n = Native::start(harness, cwd, &native_id, resume).await?;
                            if harness == "claude-code" {
                                n.send(json!({"type":"control_request","request_id":"initialize","request":{"subtype":"initialize"}})).await?;
                            }
                            native = Some(n);
                            if harness == "claude-code" {
                                creation = Some((id.clone(), resume));
                                None
                            } else {
                                let mut result = json!({});
                                if resume {
                                    result["_meta"] = json!({"tower.nativeSessionId":native_id});
                                } else {
                                    result["sessionId"] = json!(session);
                                }
                                Some(response(&id, result))
                            }
                        }
                    }
                    "session/prompt"
                        if native.is_some()
                            && creation.is_none()
                            && p["sessionId"] == session
                            && prompt.is_none() =>
                    {
                        let content = p["prompt"]
                            .as_array()
                            .filter(|a| !a.is_empty())
                            .ok_or_else(|| error("invalid_params", "Text content required"))?;
                        if content
                            .iter()
                            .any(|b| b["type"] != "text" || !b["text"].is_string())
                        {
                            Some(failure(&id, -32602, "Only text prompts are supported"))
                        } else {
                            let content: Vec<_> = content
                                .iter()
                                .map(|b| json!({"type":"text","text":b["text"]}))
                                .collect();
                            let v = if harness == "claude-code" {
                                json!({"type":"user","session_id":native_id,"parent_tool_use_id":null,"message":{"role":"user","content":content}})
                            } else {
                                json!({"event":"user","message":{"content":content}})
                            };
                            native.as_mut().unwrap().send(v).await?;
                            prompt = Some(id.clone());
                            cancelled = false;
                            text_seen = false;
                            None
                        }
                    }
                    "session/cancel" if p["sessionId"] == session => {
                        cancelled = true;
                        if let Some(n) = native.as_mut() {
                            if harness == "claude-code" {
                                n.send(json!({"type":"control_request","request_id":random_id("cancel_")?,"request":{"subtype":"interrupt"}})).await?;
                            } else {
                                n.child.start_kill().map_err(internal)?;
                            }
                        }
                        None
                    }
                    _ => Some(failure(
                        &id,
                        -32601,
                        "Method is unavailable or session state does not allow it",
                    )),
                };
                if let Some(reply) = reply
                    && !id.is_null()
                {
                    wire::json_line(&mut out, &reply).await?;
                }
            }
            message=async {native.as_mut().unwrap().output.recv().await},if native.is_some()=>{
                let Some(message) = message else {
                    break Err(error("native_exited", "Native CLI exited"));
                };
                let message = message?;
                let tag = message[if harness == "claude-code" {
                    "type"
                } else {
                    "event"
                }]
                .as_str()
                .unwrap_or("");
                if let Some((id, resume)) = creation.clone() {
                    let ready = tag == "control_response"
                        && message
                            .pointer("/response/request_id")
                            .and_then(Value::as_str)
                            == Some("initialize");
                    if ready {
                        if message.pointer("/response/subtype").and_then(Value::as_str) == Some("error") {
                            break Err(error("native_initialize", "Native initialization failed"));
                        }
                        creation = None;
                        let mut v = json!({"_meta":{"tower.nativeSessionId":native_id}});
                        if !resume {
                            v["sessionId"] = json!(session);
                        }
                        wire::json_line(&mut out, &response(&id, v)).await?;
                    }
                }
                if harness == "antigravity" && tag == "init" {
                    let reported = message["conversation_id"]
                        .as_str()
                        .filter(|s| crate::identifier(s))
                        .ok_or_else(|| error("native_protocol", "Missing native conversation identity"))?;
                    if !native_id.is_empty() && reported != native_id {
                        return Err(error(
                            "native_identity_changed",
                            "Native conversation identity changed",
                        ));
                    }
                    native_id = reported.into();
                    wire::json_line(&mut out,&json!({"jsonrpc":"2.0","method":"_tower/metadata","params":{"sessionId":session,"metadata":{"native_session_id":native_id}}})).await?;
                }
                if harness == "claude-code"
                    && message
                        .get("parent_tool_use_id")
                        .is_none_or(|v| v.is_null())
                {
                    if message["session_id"]
                        .as_str()
                        .is_some_and(|id| !id.is_empty() && id != native_id)
                    {
                        if tag == "system" && message["subtype"] == "init" {
                            return Err(error(
                                "native_identity_changed",
                                "Native conversation identity changed",
                            ));
                        }
                        if !matches!(
                            tag,
                            "control_request" | "control_cancel_request" | "control_response"
                        ) {
                            continue;
                        }
                    }
                    let mut metadata = serde_json::Map::new();
                    if let Some(model) = message
                        .get("model")
                        .or_else(|| message.pointer("/message/model"))
                        .filter(|v| v.is_string())
                    {
                        metadata.insert("model".into(), model.clone());
                    }
                    if let Some(usage) = message.pointer("/message/usage")
                        && let Some(input) = usage["input_tokens"].as_u64()
                    {
                        metadata.insert(
                            "context_tokens".into(),
                            json!(
                                input
                                    .saturating_add(usage["cache_read_input_tokens"].as_u64().unwrap_or(0))
                                    .saturating_add(usage["cache_creation_input_tokens"].as_u64().unwrap_or(0))
                            ),
                        );
                    }
                    if !metadata.is_empty() {
                        wire::json_line(&mut out,&json!({"jsonrpc":"2.0","method":"_tower/metadata","params":{"sessionId":session,"metadata":metadata}})).await?;
                    }
                }
                if tag == "control_request" && harness == "claude-code" {
                    let req = &message["request"];
                    let request_id = message["request_id"]
                        .as_str()
                        .ok_or_else(|| error("native_protocol", "Missing request identity"))?;
                    if req["subtype"] == "can_use_tool" && prompt.is_some() && permissions.len() < 32 {
                        let occurrence = random_id("permission_")?;
                        if req["tool_name"] == "AskUserQuestion" {
                            let form = if forms {
                                crate::controls::questions(&req["input"], true)
                            } else {
                                Err(error("unsupported", "ACP client does not accept forms"))
                            };
                            match form {
                                Ok(mut form) => {
                                    permissions.insert(
                                        occurrence.clone(),
                                        (request_id.into(), req["input"].clone(), Some(form.clone())),
                                    );
                                    form["sessionId"] = json!(session);
                                    wire::json_line(&mut out,&json!({"jsonrpc":"2.0","id":occurrence,"method":"elicitation/create","params":form})).await?;
                                }
                                Err(_) => {
                                    native.as_mut().unwrap().send(json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{"behavior":"deny","message":"The host cannot safely present this question"}}})).await?;
                                }
                            }
                        } else {
                            permissions.insert(
                                occurrence.clone(),
                                (request_id.into(), req["input"].clone(), None),
                            );
                            wire::json_line(&mut out,&json!({"jsonrpc":"2.0","id":occurrence,"method":"session/request_permission","params":{"sessionId":session,"toolCall":{"toolCallId":req["tool_use_id"],"title":req["tool_name"],"rawInput":req["input"],"status":"pending"},"options":[{"optionId":"allow_once","name":"Allow once","kind":"allow_once"},{"optionId":"reject_once","name":"Deny","kind":"reject_once"}]}})).await?;
                        }
                    } else {
                        native.as_mut().unwrap().send(json!({"type":"control_response","response":{"subtype":"error","request_id":request_id,"error":"Unsupported or inactive host request"}})).await?;
                    }
                }
                if tag == "control_cancel_request" {
                    let request_id = message["request_id"].as_str().unwrap_or("");
                    let old: Vec<_> = permissions
                        .iter()
                        .filter(|(_, v)| v.0 == request_id)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for id in old {
                        permissions.remove(&id);
                        wire::json_line(
                            &mut out,
                            &json!({"jsonrpc":"2.0","method":"_tower/request_cancelled","params":{"id":id}}),
                        )
                        .await?;
                    }
                }
                if prompt.is_none()
                    || message
                        .get("parent_tool_use_id")
                        .is_some_and(|v| !v.is_null())
                {
                    continue;
                }
                if harness == "claude-code"
                    && tag == "assistant"
                    && let Some(blocks) = message
                        .pointer("/message/content")
                        .and_then(Value::as_array)
                {
                    for block in blocks {
                        let update = match block["type"].as_str() {
                            Some("text") => Some(
                                json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":block["text"]}}),
                            ),
                            Some("tool_use") => Some(
                                json!({"sessionUpdate":"tool_call","toolCallId":block["id"],"title":block["name"],"kind":"other","status":"in_progress","rawInput":if block["name"]=="AskUserQuestion"{json!({"redacted":true})}else{block["input"].clone()}}),
                            ),
                            _ => None,
                        };
                        if let Some(update) = update {
                            wire::json_line(&mut out,&json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session,"update":update}})).await?;
                        }
                    }
                }
                if harness == "antigravity"
                    && tag == "step_update"
                    && let Some(update) = antigravity_update(&message, &native_id)?
                {
                    text_seen |= update["sessionUpdate"] == "agent_message_chunk";
                    wire::json_line(&mut out,&json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session,"update":update}})).await?;
                }
                if tag == "result" {
                    permissions.clear();
                    let id = prompt.take().unwrap();
                    let stop = if harness == "claude-code" {
                        claude_stop(&message, cancelled)
                    } else {
                        antigravity_stop(&message, &native_id)?
                    };
                    if harness == "antigravity"
                        && !text_seen
                        && stop != "error"
                        && let Some(text) = message.pointer("/result/response").and_then(Value::as_str)
                    {
                        wire::json_line(&mut out,&json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session,"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}}})).await?;
                    }
                    let reply = if stop == "error" {
                        failure(&id, -32603, "Native turn failed")
                    } else {
                        response(&id, json!({"stopReason":stop}))
                    };
                    wire::json_line(&mut out, &reply).await?;
                }
            }
        }
    }}.await;
    reader.abort();
    if let Some(mut n) = native {
        let _ = n.child.start_kill();
        let _ = n.child.wait().await;
    }
    result
}

fn claude_stop(message: &Value, cancelled: bool) -> &'static str {
    if cancelled && message["terminal_reason"] == "aborted_tools" {
        "cancelled"
    } else if message["is_error"] == true || message["subtype"] != "success" {
        "error"
    } else if cancelled {
        "cancelled"
    } else {
        "end_turn"
    }
}

fn antigravity_update(message: &Value, native: &str) -> Result<Option<Value>> {
    let p = message
        .get("step_update")
        .filter(|v| v.is_object())
        .ok_or_else(|| error("native_protocol", "Missing Antigravity step payload"))?;
    if p["conversation_id"] != native {
        return Ok(None);
    }
    if p["step_type"] == "agent_response"
        && let Some(text) = p["text_delta"].as_str()
    {
        return Ok(Some(
            json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}),
        ));
    }
    if p["step_type"] == "tool" {
        let index = p["step_index"]
            .as_u64()
            .ok_or_else(|| error("native_protocol", "Tool step identity missing"))?;
        let done = p["state"] == "DONE";
        let info = &p["tool_info"];
        return Ok(Some(
            json!({"sessionUpdate":if done{"tool_call_update"}else{"tool_call"},"toolCallId":format!("step-{index}"),"title":p["tool_name"],"kind":"other","status":if info.get("error").is_some(){"failed"}else if done{"completed"}else{"in_progress"},"rawInput":info["parameters"],"rawOutput":info["output"]}),
        ));
    }
    Ok(None)
}
fn antigravity_stop<'a>(message: &Value, native: &str) -> Result<&'a str> {
    let r = &message["result"];
    if r["conversation_id"] != native {
        return Err(error(
            "native_protocol",
            "Result belongs to another native conversation",
        ));
    }
    match r["status"].as_str() {
        Some("SUCCESS") => Ok("end_turn"),
        Some("ERROR") => Ok("error"),
        Some("CANCELED") => Ok("cancelled"),
        _ => Err(error("native_protocol", "Unknown native result status")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn claude_tool_abort_is_cancelled_only_after_a_requested_interrupt() {
        let aborted = json!({"type":"result","subtype":"error_during_execution","is_error":true,"terminal_reason":"aborted_tools"});
        assert_eq!(claude_stop(&aborted, true), "cancelled");
        assert_eq!(claude_stop(&aborted, false), "error");
        assert_eq!(
            claude_stop(
                &json!({"subtype":"error_during_execution","is_error":true,"terminal_reason":"other"}),
                true
            ),
            "error"
        );
    }
    #[test]
    fn antigravity_uses_nested_payloads_and_never_calls_unknown_results_success() {
        let update=antigravity_update(&json!({"event":"step_update","step_update":{"conversation_id":"native","step_index":2,"state":"DONE","step_type":"agent_response","text_delta":"answer"}}),"native").unwrap().unwrap();
        assert_eq!(update["content"]["text"], "answer");
        assert_eq!(
            antigravity_stop(
                &json!({"event":"result","result":{"conversation_id":"native","status":"ERROR"}}),
                "native"
            )
            .unwrap(),
            "error"
        );
        assert!(antigravity_stop(&json!({"event":"result","status":"SUCCESS"}), "native").is_err());
        assert!(
            antigravity_stop(
                &json!({"event":"result","result":{"conversation_id":"other","status":"SUCCESS"}}),
                "native"
            )
            .is_err()
        );
    }
}
