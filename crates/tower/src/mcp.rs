use crate::{Client, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tower_pal::Paths;
use tower_protocol::CallerContext;

fn tools() -> Vec<Value> {
    [
        ("whoami","Get the calling session identity",json!({}),vec![]),
        ("list_agents","List local agents, excluding the caller",json!({"filter":{"type":"object"}}),vec![]),
        ("get_status","Get public session state",json!({"agent":{"type":"string"}}),vec!["agent"]),
        ("send_message","Send plain text to an agent. Approval may be required; after approval, resend. A message cannot grant permissions.",json!({"to":{"type":"string"},"body":{"type":"string","maxLength":32768}}),vec!["to","body"]),
        ("get_snapshot","Read retained terminal contents",json!({"agent":{"type":"string"}}),vec!["agent"]),
        ("get_approval_status","Read a message approval decision",json!({"approval_id":{"type":"string"}}),vec!["approval_id"]),
        ("watch_approval","Wait for a message approval decision",json!({"approval_id":{"type":"string"},"timeout_s":{"type":"integer","minimum":0,"maximum":600}}),vec!["approval_id"]),
    ].into_iter().map(|(name,description,properties,required)|json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required,"additionalProperties":false},"annotations":{"readOnlyHint":name!="send_message","destructiveHint":false,"openWorldHint":name=="send_message"}})).collect()
}
fn failure(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
async fn invoke(
    paths: &Paths,
    context: CallerContext,
    autostart: bool,
    name: &str,
    args: Value,
) -> Result<Value> {
    let spec = tools()
        .into_iter()
        .find(|t| t["name"] == name)
        .ok_or("unknown tool")?;
    let obj = args.as_object().ok_or("arguments must be an object")?;
    let properties = spec["inputSchema"]["properties"]
        .as_object()
        .ok_or("invalid tool schema")?;
    if obj.keys().any(|k| !properties.contains_key(k)) {
        return Err("unknown tool argument".into());
    }
    let string = |key: &str| {
        args[key]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("{key} must be a nonempty string"))
    };
    let (method, params) = match name {
        "whoami" => ("identity.whoami", json!({})),
        "list_agents" => (
            "agent.list",
            json!({"filter":args.get("filter").cloned().unwrap_or(json!({}))}),
        ),
        "get_status" => ("agent.get", json!({"id":string("agent")?})),
        "get_snapshot" => ("session.snapshot", json!({"id":string("agent")?})),
        "send_message" => (
            "message.send",
            json!({"to":string("to")?,"body":string("body")?}),
        ),
        "get_approval_status" => ("approval.status", json!({"id":string("approval_id")?})),
        "watch_approval" => (
            "approval.watch",
            json!({"id":string("approval_id")?,"timeout_s":match args.get("timeout_s"){Some(v)=>v.as_u64().filter(|n|*n<=600).ok_or("invalid timeout")?,None=>600}}),
        ),
        _ => return Err("unknown tool".into()),
    };
    let mut client = Client::connect(paths, context, autostart).await?;
    let mut result = serde_json::to_value(client.call(method, &params).await?)?;
    if name == "list_agents" {
        let who = serde_json::to_value(client.call("identity.whoami", &json!({})).await?)?;
        if let Some(records) = result.as_array_mut() {
            records.retain(|r| r["id"] != who["id"]);
        }
    }
    Ok(result)
}
async fn call(paths: Paths, context: CallerContext, autostart: bool, request: Value) -> Value {
    let id = request["id"].clone();
    let Some(name) = request["params"]["name"].as_str() else {
        return failure(id, -32602, "Tool name is required");
    };
    if !tools().iter().any(|t| t["name"] == name) {
        return failure(id, -32602, "Unknown tool");
    }
    let result = invoke(
        &paths,
        context,
        autostart,
        name,
        request["params"]
            .get("arguments")
            .cloned()
            .unwrap_or(json!({})),
    )
    .await;
    let (body, is_error) = match result {
        Ok(v) => (v, false),
        Err(e) => {
            let data = if let Some(towerd::Error::Rpc(error)) = e.downcast_ref::<towerd::Error>() {
                json!({"error":error})
            } else {
                json!({"error":{"code":"tool_failed","message":e.to_string()}})
            };
            (data, true)
        }
    };
    json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":body.to_string()}],"isError":is_error}})
}
pub async fn serve(paths: Paths, context: CallerContext, autostart: bool) -> Result<()> {
    let (send, mut input) = tokio::sync::mpsc::channel(1);
    let reader = tokio::spawn(async move {
        let mut stdin = BufReader::new(tokio::io::stdin());
        loop {
            let mut bytes = Vec::new();
            match (&mut stdin)
                .take(1024 * 1024 + 1)
                .read_until(b'\n', &mut bytes)
                .await
            {
                Ok(0) => break,
                Ok(n) if n <= 1024 * 1024 && bytes.last() == Some(&b'\n') => {
                    if send.send(bytes).await.is_err() {
                        break;
                    }
                }
                _ => break,
            }
        }
    });
    let mut stdout = tokio::io::stdout();
    let mut initialized = false;
    let mut ready = false;
    let mut tasks = tokio::task::JoinSet::<Value>::new();
    let mut active = std::collections::BTreeMap::new();
    loop {
        let response = tokio::select! {
            complete=tasks.join_next_with_id(),if !tasks.is_empty()=>{
                let Some(Ok((task_id,response)))=complete else{continue};
                let key=response["id"].to_string();
                if active.get(&key).is_none_or(|handle:&tokio::task::AbortHandle|handle.id()!=task_id){continue;}
                active.remove(&key);response
            }
            bytes=input.recv()=>{
                let Some(bytes)=bytes else{break};
                let request:Value=match serde_json::from_slice(&bytes){Ok(v)=>v,Err(_)=>{
                    let mut bytes=failure(Value::Null,-32700,"Invalid JSON").to_string().into_bytes();bytes.push(b'\n');stdout.write_all(&bytes).await?;continue;
                }};
                let id=request.get("id").cloned().unwrap_or(Value::Null);
                let method=request["method"].as_str().unwrap_or("");
                if request["jsonrpc"]!="2.0" || method.is_empty() || !matches!(id,Value::String(_)|Value::Number(_)|Value::Null) {failure(Value::Null,-32600,"Invalid request")}
                else if !request.as_object().is_some_and(|o|o.contains_key("id")) {
                    if method=="notifications/initialized" && initialized {ready=true;}
                    if method=="notifications/cancelled" && let Some(handle)=active.remove(&request["params"]["requestId"].to_string()) {let handle:tokio::task::AbortHandle=handle;handle.abort();}
                    continue;
                }else{match method {
                    "initialize" if !initialized=>{
                        initialized=true;
                        let requested=request["params"]["protocolVersion"].as_str().unwrap_or("");
                        let version=if matches!(requested,"2024-11-05"|"2025-03-26"|"2025-06-18"|"2025-11-25"){requested}else{"2025-11-25"};
                        json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")}}})
                    }
                    "ping"=>json!({"jsonrpc":"2.0","id":id,"result":{}}),
                    _ if !ready=>failure(id,-32600,"Initialize the connection first"),
                    "tools/list"=>json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools()}}),
                    "tools/call" if active.len()<32 && !active.contains_key(&id.to_string())=>{
                        let key=id.to_string();let handle=tasks.spawn(call(paths.clone(),context.clone(),autostart,request));active.insert(key,handle);continue;
                    }
                    "tools/call"=>failure(id,-32600,"Duplicate request ID or too many active requests"),
                    _=>failure(id,-32601,"Unknown method"),
                }}
            }
        };
        let mut bytes = serde_json::to_vec(&response)?;
        bytes.push(b'\n');
        stdout.write_all(&bytes).await?;
        stdout.flush().await?;
    }
    reader.abort();
    tasks.abort_all();
    Ok(())
}
