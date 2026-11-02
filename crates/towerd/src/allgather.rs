pub(crate) mod enrollment;
mod execution;
mod status;
use crate::{Error, Result, worker::ServiceWorker};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, protocol::WebSocketConfig,
};
use tower_protocol::{AllgatherHelloAck, PolicyPush, Request, Response, ResponseBody, Secret};

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub managed: bool,
    pub url: String,
    pub policy_id: String,
    pub scopes: Option<Vec<String>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            managed: true,
            url: String::new(),
            policy_id: "tower".into(),
            scopes: None,
        }
    }
}
pub(crate) fn safe_url(input: &str, websocket: bool) -> Result<()> {
    let url =
        reqwest::Url::parse(input).map_err(|_| Error::Other("Invalid Allgather URL".into()))?;
    let secure = url.scheme() == if websocket { "wss" } else { "https" };
    let local = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if input.len() > 4096
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || !(secure || (local && url.scheme() == if websocket { "ws" } else { "http" }))
    {
        return Err(Error::Other(
            "Allgather requires TLS except for loopback test endpoints".into(),
        ));
    }
    Ok(())
}
impl Config {
    pub(crate) fn load(paths: &tower_pal::Paths) -> Result<Self> {
        #[derive(Default, Deserialize)]
        #[serde(default)]
        struct Root {
            allgather: Config,
        }
        let text = tower_pal::read_config(&paths.config_home.join("tower/config.toml"))?;
        let root: Root = text
            .map(|s| toml::from_str(&s))
            .transpose()
            .map_err(|_| Error::Other("Invalid Allgather configuration".into()))?
            .unwrap_or_default();
        Ok(root.allgather)
    }
    pub(crate) fn permits_scope(&self, scope: &str) -> bool {
        self.scopes.as_ref().is_none_or(|configured| {
            configured
                .iter()
                .any(|s| s == scope || (scope != "acp:control" && s == "admin"))
        })
    }

    pub(crate) fn enrollment_scopes(&self, read_only: bool) -> Vec<String> {
        ["read", "send", "acp:control"]
            .into_iter()
            .filter(|scope| (!read_only || *scope == "read") && self.permits_scope(scope))
            .map(str::to_owned)
            .collect()
    }
    pub(crate) fn validate(&self) -> Result<()> {
        safe_url(&self.url, true)?;
        if self.policy_id.is_empty()
            || self.policy_id.len() > 128
            || self.scopes.iter().flatten().any(|s| {
                !matches!(
                    s.as_str(),
                    "read" | "send" | "subscribe" | "approve" | "admin" | "acp:control"
                )
            })
        {
            return Err(Error::Other(
                "Invalid Allgather policy namespace or scopes".into(),
            ));
        }
        Ok(())
    }
}
fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| Error::Other("Cannot create authorization client".into()))
}
async fn json_response(response: reqwest::Response) -> Result<Value> {
    if response.content_length().is_some_and(|n| n > 65536) {
        return Err(Error::Other("Authorization response is too large".into()));
    }
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::Other("Authorization response failed".into()))?
    {
        if bytes.len() + chunk.len() > 65536 {
            return Err(Error::Other("Authorization response is too large".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Other("Invalid authorization response".into()))
}
pub fn configured_url(paths: &tower_pal::Paths) -> Result<String> {
    Ok(enrollment::socket_url(&enrollment::configured_origin(
        paths, None,
    )?))
}
pub fn enrollment_url(input: &str) -> Result<String> {
    Ok(enrollment::socket_url(&enrollment::origin(input)?))
}
pub(super) fn start(
    paths: tower_pal::Paths,
    worker: ServiceWorker,
) -> Result<tokio::task::JoinHandle<()>> {
    let config = Config::load(&paths)?;
    Ok(tokio::spawn(async move {
        let mut delay = Duration::from_millis(500);
        let mut blocked_key = None;
        loop {
            let _ = enrollment::recover_confirmation(&paths).await;
            let mut config = config.clone();
            if let Ok(binding) = tower_pal::read_private(&paths.state.join("allgather-server"))
                && let Ok(url) = String::from_utf8(binding)
            {
                config.url = url;
            }
            let key = tower_pal::read_private(&paths.state.join("allgather.key"))
                .ok()
                .and_then(|b| String::from_utf8(b).ok());
            let hello = worker
                .execute(|s| s.sync_hello())
                .await
                .ok()
                .and_then(std::result::Result::ok)
                .flatten();
            if let (Some(key), Some(hello)) = (key, hello)
                && blocked_key.as_ref() != Some(&key)
                && config.validate().is_ok()
                && tower_pal::read_private(&paths.state.join("allgather-server"))
                    .is_ok_and(|b| b == config.url.as_bytes())
            {
                let outcome = connection(&paths, &config, &key, hello, worker.clone()).await;
                let _ = worker.execute(|s| s.sync_connected(false)).await;
                if let Err(error) = &outcome {
                    let code = match error {
                        Error::Rpc(e) if e.data.code == "protocol_incompatible" => {
                            "protocol_incompatible"
                        }
                        Error::Rpc(e) if e.data.code == "sync_frame_too_large" => {
                            "sync_frame_too_large"
                        }
                        Error::Rpc(e) if e.data.code == "sync_record_limit" => "sync_record_limit",
                        Error::Rpc(e) if e.data.code == "sync_ack_timeout" => "sync_ack_timeout",
                        _ => "connection_failed",
                    };
                    let failed_key = Secret(key.clone());
                    let _ = worker
                        .execute(move |s| s.sync_error(&failed_key, code))
                        .await;
                }
                if outcome.as_ref().is_err_and(
                    |e| matches!(e,Error::Rpc(e) if e.data.code=="protocol_incompatible"),
                ) {
                    blocked_key = Some(key);
                }
                if outcome.is_ok() {
                    delay = Duration::from_millis(500);
                } else {
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
                tokio::time::sleep(delay).await;
                continue;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }))
}
fn encode<T: Serialize>(value: &T) -> Result<String> {
    let text = serde_json::to_string(value)?;
    if text.len() > tower_protocol::MAX_FRAME_BYTES {
        return Err(tower_protocol::RpcError::application(
            "sync_frame_too_large",
            "Sync frame exceeds 1 MiB; no partial frame was sent",
        )
        .into());
    }
    Ok(text)
}
async fn write<S, T>(socket: &mut S, value: &T) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Debug,
    T: Serialize,
{
    let text = encode(value)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(text.into())),
    )
    .await
    .map_err(|_| Error::Other("Sync write timed out".into()))?
    .map_err(|_| Error::Other("Sync write failed".into()))
}
async fn reply<S>(
    socket: &mut S,
    id: Option<tower_protocol::RequestId>,
    result: std::result::Result<tower_protocol::Value, tower_protocol::RpcError>,
) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Debug,
{
    if let Some(id) = id {
        write(
            socket,
            &Response {
                jsonrpc: "2.0".into(),
                id,
                body: match result {
                    Ok(result) => ResponseBody::Success { result },
                    Err(error) => ResponseBody::Failure { error },
                },
            },
        )
        .await?;
    }
    Ok(())
}
fn public(value: Value) -> Result<tower_protocol::Value> {
    Ok(serde_json::from_value(value)?)
}
async fn connection(
    paths: &tower_pal::Paths,
    config: &Config,
    key: &str,
    hello: tower_protocol::AllgatherHello,
    worker: ServiceWorker,
) -> Result<()> {
    let mut request = config
        .url
        .clone()
        .into_client_request()
        .map_err(|_| Error::Other("Invalid sync endpoint".into()))?;
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {key}")
            .parse()
            .map_err(|_| Error::Other("Invalid access key".into()))?,
    );
    let limits = WebSocketConfig::default()
        .max_message_size(Some(tower_protocol::MAX_FRAME_BYTES))
        .max_frame_size(Some(tower_protocol::MAX_FRAME_BYTES))
        .max_write_buffer_size(tower_protocol::MAX_FRAME_BYTES + 1024);
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_with_config(request, Some(limits), false),
    )
    .await
    .map_err(|_| Error::Other("Sync connection timed out".into()))?
    .map_err(|_| Error::Other("Sync connection failed".into()))?;
    write(
        &mut socket,
        &json!({"jsonrpc":"2.0","id":"hello","method":"hello","params":hello}),
    )
    .await?;
    let first = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .map_err(|_| Error::Other("Sync handshake timed out".into()))?
        .ok_or_else(|| Error::Other("Sync handshake closed".into()))?
        .map_err(|_| Error::Other("Sync handshake failed".into()))?;
    let first: Value = serde_json::from_slice(&first.into_data())?;
    if first.pointer("/error/data/code").and_then(Value::as_str) == Some("protocol_incompatible") {
        return Err(tower_protocol::RpcError::application(
            "protocol_incompatible",
            "Allgather protocol is incompatible",
        )
        .into());
    }
    let ack: AllgatherHelloAck = serde_json::from_value(if first["id"] == "hello" {
        first["result"].clone()
    } else if first["method"] == "hello_ack" {
        first["params"].clone()
    } else {
        return Err(Error::Other("Invalid sync handshake identity".into()));
    })?;
    if ack.protocol_version.major != tower_protocol::PROTOCOL_VERSION.major
        || ["policy-sync.v1", "session-status.v1"]
            .iter()
            .any(|required| !ack.capabilities.iter().any(|c| c == required))
    {
        return Err(tower_protocol::RpcError::application(
            "protocol_incompatible",
            "Allgather must support the current major version, policy-sync.v1, and session-status.v1",
        )
        .into());
    }
    let handshake_key = Secret(key.into());
    let epoch = worker
        .execute(move |s| s.sync_activate(&handshake_key, &ack.node_name))
        .await??;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last_ping = tokio::time::Instant::now();
    let mut last_response = last_ping;
    let mut publisher = status::Publisher::default();
    let mut previous_cursor = None;
    let delta_enabled = ack
        .capabilities
        .iter()
        .any(|c| c == "session-status-delta.v1");
    let managed_enabled = ["managed-acp.v1", "command-receipts.v1", "session-stream.v1"]
        .iter()
        .all(|c| {
            hello.capabilities.iter().any(|v| v == c) && ack.capabilities.iter().any(|v| v == c)
        });
    let mut execution = execution::Publisher::default();
    let mut pending_since: Option<tokio::time::Instant> = None;
    let mut replies = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _=tick.tick()=>{
                if tower_pal::read_private(&paths.state.join("allgather.key")).ok().as_deref()!=Some(key.as_bytes()){return Ok(());}
                worker.execute(move|s|s.check_remote(epoch)).await??;
                if last_response.elapsed()>Duration::from_secs(35){return Err(Error::Other("Sync peer stopped responding".into()));}
                if last_ping.elapsed()>=Duration::from_secs(10){socket.send(Message::Ping(Vec::new().into())).await.map_err(|_|Error::Other("Sync ping failed".into()))?;last_ping=tokio::time::Instant::now();}
                if pending_since.is_some_and(|sent|sent.elapsed()>=Duration::from_secs(30)) {
                    return Err(tower_protocol::RpcError::application("sync_ack_timeout", "Status projection was not acknowledged").into());
                }
                let cursor=worker.execute(|s|s.sync_cursor()).await??;
                if !publisher.waiting() && previous_cursor!=Some(cursor) {
                    let records=worker.execute(|s|s.sync_state()).await??;
                    if let Some(frame)=publisher.prepare(&records,delta_enabled)? {
                        write(&mut socket,&frame).await?;
                        pending_since=Some(tokio::time::Instant::now());
                    }
                    previous_cursor=Some(cursor);
                }
                if managed_enabled {for frame in execution.prepare(&worker,epoch).await? {write(&mut socket,&frame).await?;}}
            }
            Some(reply)=replies.join_next(),if !replies.is_empty()=>{if let Ok(Some(reply))=reply {worker.execute(move|s|s.check_remote(epoch)).await??;write(&mut socket,&reply).await?;}}
            incoming=socket.next()=>{
                let Some(incoming)=incoming else{return Ok(())};
                let incoming=incoming.map_err(|_|Error::Other("Sync read failed".into()))?;
                match incoming {
                    Message::Pong(_)=>{last_response=tokio::time::Instant::now();continue;}
                    Message::Ping(_)=>{socket.flush().await.map_err(|_|Error::Other("Sync pong failed".into()))?;continue;}
                    Message::Close(_)=>return Ok(()),
                    Message::Text(text)=>{
                        last_response=tokio::time::Instant::now();
                        worker.execute(move|s|s.check_remote(epoch)).await??;
                        let frame:Value=serde_json::from_str(&text)?;
                        if frame.get("method").is_none() {
                            if managed_enabled {execution.response(frame,&worker,epoch).await?;}
                            continue;
                        }
                        let mut request:Request=serde_json::from_value(frame)?;
                        if request.jsonrpc!="2.0"{return Err(Error::Other("Invalid sync RPC".into()));}
                        request.context=Default::default();
                        let p=serde_json::to_value(&request.params)?;
                        match request.method.as_str(){
                            "policy.push"|"policy.delta"=>{
                                worker.execute(move|s|s.sync_policy_pending(epoch)).await??;
                                let result=if request.method=="policy.delta" {
                                    if !ack.capabilities.iter().any(|c|c=="policy-delta.v1") {
                                        Err(tower_protocol::RpcError::application("unsupported","Policy deltas were not negotiated"))
                                    } else {
                                        match serde_json::from_value::<tower_protocol::PolicyDelta>(p) {
                                            Ok(policy)=>worker.execute(move|s|s.sync_policy_delta(policy,epoch)).await?,
                                            Err(_)=>Err(tower_protocol::RpcError::application("invalid_params","Invalid policy delta")),
                                        }
                                    }
                                } else { match serde_json::from_value::<PolicyPush>(p) {
                                    Ok(policy)=>worker.execute(move|s|s.sync_policy(policy,epoch)).await?,
                                    Err(_)=>Err(tower_protocol::RpcError::application("invalid_params","Invalid policy snapshot")),
                                }};
                                match result {
                                    Ok(ack)=>{
                                        reply(&mut socket,request.id,Ok(public(serde_json::to_value(&ack)?)?)).await?;
                                        write(&mut socket,&json!({"jsonrpc":"2.0","method":"policy.ack","params":ack})).await?;
                                    }
                                    Err(error)=>{
                                        reply(&mut socket,request.id,Err(error.clone())).await?;
                                        let current=worker.execute(|s|s.sync_hello()).await??.and_then(|hello|hello.policy_revision);
                                        let ack=tower_protocol::PolicyAck{policy_id:config.policy_id.clone(),previous_revision:current,applied_revision:current,changed_rule_ids:Vec::new(),outcome:"rejected".into(),reason:Some(error.data.code),received_at:std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis().min(u64::MAX as u128) as u64};
                                        write(&mut socket,&json!({"jsonrpc":"2.0","method":"policy.ack","params":ack})).await?;
                                    }
                                }
                                continue;
                            }
                            "sync.ack"=>{
                                let ack:tower_protocol::SyncAck=serde_json::from_value(p)?;
                                if let Some(ids)=publisher.acknowledge(&ack)? {
                                    pending_since=None;
                                    let rejected=ack.rejected.into_iter().map(|r|(r.id,r.reason)).collect::<Vec<_>>();
                                    worker.execute(move|s|s.sync_ack(&ids,&rejected,epoch)).await??;
                                }
                                continue;
                            },
                            "sync.resync"=>{publisher.resync();pending_since=None;previous_cursor=None;reply(&mut socket,request.id,Ok(public(json!({"scheduled":true}))?)).await?;continue;}
                            "health"=>request.method="daemon.info".into(),
                            "command.execute"|"command.status" if !managed_enabled=>{reply(&mut socket,request.id,Err(tower_protocol::RpcError::application("unsupported","Managed execution was not negotiated"))).await?;continue;}
                            _=>{},
                        }
                        if replies.len()>=32 {
                            if let Some(id)=request.id {write(&mut socket,&Response{jsonrpc:"2.0".into(),id,body:ResponseBody::Failure{error:tower_protocol::RpcError::application("rate_limited","Too many remote requests")}}).await?;}
                        }else{
                            let worker=worker.clone();replies.spawn(async move{let id=request.id.clone();let result=worker.remote(request,epoch).await;id.map(|id|Response{jsonrpc:"2.0".into(),id,body:match result{Ok(result)=>ResponseBody::Success{result},Err(error)=>ResponseBody::Failure{error}}})});
                        }
                    }
                    _=>return Err(Error::Other("Unsupported sync frame".into())),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn enrollment_respects_explicit_scope_restrictions() {
        let default: super::Config = toml::from_str("").unwrap();
        assert_eq!(
            default.enrollment_scopes(false),
            ["read", "send", "acp:control"]
        );
        assert_eq!(default.enrollment_scopes(true), ["read"]);
        let empty: super::Config = toml::from_str("scopes=[]").unwrap();
        assert!(empty.enrollment_scopes(false).is_empty());
        let admin: super::Config = toml::from_str("scopes=['admin']").unwrap();
        assert_eq!(admin.enrollment_scopes(false), ["read", "send"]);
        let read: super::Config = toml::from_str("scopes=['read']").unwrap();
        assert_eq!(read.enrollment_scopes(false), ["read"]);
    }

    use super::*;

    #[tokio::test]
    async fn oversized_sync_frame_is_rejected_before_any_bytes_are_sent() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let sent = Arc::new(AtomicUsize::new(0));
        let mut socket = Box::pin(futures_util::sink::unfold(
            sent.clone(),
            |sent, _: Message| async move {
                sent.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(sent)
            },
        ));
        let result = write(
            &mut socket,
            &json!({"records":[{"id":"large","cwd":"x".repeat(tower_protocol::MAX_FRAME_BYTES)}]}),
        )
        .await;
        assert!(
            matches!(result, Err(Error::Rpc(error)) if error.data.code=="sync_frame_too_large")
        );
        assert_eq!(sent.load(Ordering::SeqCst), 0);
    }
}
