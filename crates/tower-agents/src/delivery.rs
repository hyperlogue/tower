use std::{path::Path, time::Duration};
use tokio::io::AsyncWriteExt;
use tower_pal::Environment;
use tower_protocol::{AgentRecord, Capabilities, DeliveryEndpoint, DeliveryReceipt, RpcError};
use tower_sessions::{Result, messaging::RegisteredDelivery};

pub struct NativeDelivery;

pub fn validate(endpoint: &DeliveryEndpoint) -> Result<()> {
    validate_inner(endpoint, true)
}
fn validate_inner(endpoint: &DeliveryEndpoint, check_sockets: bool) -> Result<()> {
    if let Some(endpoint) = &endpoint.app_server {
        let path = endpoint
            .strip_prefix("unix://")
            .filter(|s| Path::new(s).is_absolute())
            .ok_or_else(|| {
                error(
                    "invalid_params",
                    "External app-server requires an absolute unix:// socket endpoint",
                )
            })?;
        if check_sockets {
            tower_pal::owned_socket(Path::new(path)).map_err(|_| {
                error(
                    "invalid_params",
                    "App-server socket must belong to this user",
                )
            })?;
        }
    }
    if let Some(path) = &endpoint.messaging_socket {
        let path = Path::new(path);
        let parent = path.parent().and_then(Path::to_str).unwrap_or("");
        let uid = tower_pal::uid();
        if ![
            format!("/run/user/{uid}/cc-socks"),
            "/tmp/cc-socks".into(),
            format!("/tmp/cc-socks-{uid}"),
            "/private/tmp/cc-socks".into(),
            format!("/private/tmp/cc-socks-{uid}"),
        ]
        .contains(&parent.to_owned())
            || path.extension().is_none_or(|s| s != "sock")
            || path.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(error(
                "invalid_params",
                "Invalid Claude inbox socket namespace",
            ));
        }
        if check_sockets {
            tower_pal::owned_socket(path).map_err(|_| {
                error(
                    "invalid_params",
                    "Claude inbox must be a socket owned by this user",
                )
            })?;
        }
    }
    if endpoint
        .messaging_token
        .as_ref()
        .is_some_and(|s| s.0.is_empty() || s.0.len() > 4096 || s.0.contains('\0'))
    {
        return Err(error("invalid_params", "Invalid inbox credential"));
    }
    if endpoint.thread_id.as_ref().is_some_and(|s| {
        s.is_empty()
            || s.len() > 512
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
    }) {
        return Err(error("invalid_params", "Invalid Codex thread ID"));
    }
    if endpoint
        .codex_home
        .as_ref()
        .is_some_and(|s| !Path::new(s).is_absolute() || s.len() > 4096 || s.contains('\0'))
    {
        return Err(error(
            "invalid_params",
            "Codex home must be an absolute path",
        ));
    }
    Ok(())
}
fn error(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}

pub async fn queue_supported(env: &Environment, cwd: &Path) -> bool {
    tower_pal::capture(
        &["codex".into(), "queue".into(), "--help".into()],
        env,
        cwd,
        Duration::from_secs(2),
        65536,
    )
    .await
    .is_ok_and(|(code, bytes)| code == 0 && String::from_utf8_lossy(&bytes).contains("--thread"))
}

pub fn capabilities(harness: &str, endpoint: &DeliveryEndpoint, queue: bool) -> Capabilities {
    let (method, reason) = match harness {
        "claude-code" if endpoint.messaging_socket.is_none() => ("none", Some("no_socket")),
        "claude-code" if endpoint.messaging_token.is_none() => ("none", Some("no_messaging_token")),
        "claude-code" => ("native", None),
        "codex" if endpoint.thread_id.is_none() => ("none", Some("no_thread_id")),
        "codex" if endpoint.app_server.is_some() => ("native", None),
        "codex" if !queue => ("none", Some("codex_too_old")),
        "codex" => ("queue", None),
        _ => ("none", Some("harness_unsupported")),
    };
    Capabilities {
        message: method.into(),
        message_reason: reason.map(Into::into),
        wake_when_idle: method != "none",
        reply_path: method != "none",
        ..Capabilities::unavailable("harness_unsupported")
    }
}

pub async fn send(
    harness: &str,
    endpoint: &DeliveryEndpoint,
    text: &str,
    env: &Environment,
    cwd: &Path,
) -> Result<DeliveryReceipt> {
    validate_inner(endpoint, false)?;
    match harness {
        "claude-code" => {
            let path = endpoint
                .messaging_socket
                .as_ref()
                .ok_or_else(|| error("unsupported", "no_socket"))?;
            let token = endpoint
                .messaging_token
                .as_ref()
                .ok_or_else(|| error("unsupported", "no_messaging_token"))?;
            let mut stream =
                tokio::time::timeout(Duration::from_secs(2), tower_pal::connect(Path::new(path)))
                    .await
                    .map_err(|_| {
                        error(
                            "channel_unavailable",
                            "Inbox connection timed out before submission",
                        )
                    })?
                    .map_err(|_| {
                        error(
                            "channel_unavailable",
                            "Inbox connection failed before submission",
                        )
                    })?;
            let mut bytes = serde_json::to_vec(&serde_json::json!({"type":"auth","token":token.0}))
                .map_err(|_| error("internal_error", "Cannot encode delivery"))?;
            bytes.push(b'\n');
            bytes.extend(
                serde_json::to_vec(
                    &serde_json::json!({"type":"user","message":{"role":"user","content":text}}),
                )
                .map_err(|_| error("internal_error", "Cannot encode delivery"))?,
            );
            bytes.push(b'\n');
            tokio::time::timeout(Duration::from_secs(5), async {
                stream.write_all(&bytes).await?;
                stream.shutdown().await
            })
            .await
            .map_err(|_| {
                error(
                    "delivery_unknown",
                    "Inbox submission timed out; do not automatically retry",
                )
            })?
            .map_err(|_| {
                error(
                    "delivery_unknown",
                    "Inbox write failed after connection; do not automatically retry",
                )
            })?;
            Ok(DeliveryReceipt {
                method: "native".into(),
                acknowledgement: "transport".into(),
            })
        }
        "codex" => {
            if endpoint.app_server.is_some() {
                match app_server(endpoint, text).await {
                    Ok(receipt) => return Ok(receipt),
                    Err(e) if e.data.code == "channel_unavailable" => {}
                    Err(e) => return Err(e),
                }
            }
            let thread = endpoint
                .thread_id
                .as_ref()
                .ok_or_else(|| error("unsupported", "no_thread_id"))?;
            let mut env = env.clone();
            if let Some(home) = &endpoint.codex_home {
                env.insert("CODEX_HOME".into(), home.clone());
            }
            if !queue_supported(&env, cwd).await {
                return Err(error("unsupported", "codex_too_old"));
            }
            let result = tower_pal::capture(
                &[
                    "codex".into(),
                    "queue".into(),
                    "--thread".into(),
                    thread.clone(),
                    "--message".into(),
                    text.into(),
                ],
                &env,
                cwd,
                Duration::from_secs(10),
                65536,
            )
            .await;
            match result {
                Ok((0, _)) => Ok(DeliveryReceipt {
                    method: "queue".into(),
                    acknowledgement: "harness".into(),
                }),
                Ok(_) => Err(error(
                    "delivery_failed",
                    "Codex rejected the queued message",
                )),
                Err(_) => Err(error(
                    "delivery_unknown",
                    "Codex queue outcome is unknown; do not automatically retry",
                )),
            }
        }
        _ => Err(error(
            "unsupported",
            "Harness has no native message channel",
        )),
    }
}
fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| error("internal_error", "Cannot start delivery worker"))
}
impl RegisteredDelivery for NativeDelivery {
    fn validate(&self, endpoint: &DeliveryEndpoint) -> Result<()> {
        validate(endpoint)
    }
    fn capabilities(&self, agent: &AgentRecord, endpoint: &DeliveryEndpoint) -> Capabilities {
        let mut env = tower_pal::environment();
        if let Some(home) = &endpoint.codex_home {
            env.insert("CODEX_HOME".into(), home.clone());
        }
        let queue = agent.harness == "codex"
            && runtime().is_ok_and(|r| r.block_on(queue_supported(&env, Path::new("/"))));
        capabilities(&agent.harness, endpoint, queue)
    }
    fn send(
        &self,
        agent: &AgentRecord,
        endpoint: &DeliveryEndpoint,
        text: &str,
    ) -> Result<DeliveryReceipt> {
        runtime()?
            .block_on(send(
                &agent.harness,
                endpoint,
                text,
                &tower_pal::environment(),
                Path::new("/"),
            ))
            .map_err(|mut e| {
                if e.data.code == "channel_unavailable" {
                    e.data.code = "delivery_failed".into();
                }
                e
            })
    }
}

async fn app_server(endpoint: &DeliveryEndpoint, text: &str) -> Result<DeliveryReceipt> {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
    let path = endpoint
        .app_server
        .as_ref()
        .and_then(|s| s.strip_prefix("unix://"))
        .ok_or_else(|| error("channel_unavailable", "App-server endpoint is unavailable"))?;
    let thread = endpoint
        .thread_id
        .as_ref()
        .ok_or_else(|| error("unsupported", "no_thread_id"))?;
    let mut submitted = false;
    let result=tokio::time::timeout(Duration::from_secs(10),async {
        let stream=tower_pal::connect(Path::new(path)).await.map_err(|_|error("channel_unavailable","App-server connection failed"))?;
        let limits=WebSocketConfig::default().max_message_size(Some(16*1024*1024)).max_frame_size(Some(16*1024*1024));
        let (mut socket,_)=tokio_tungstenite::client_async_with_config("ws://localhost/",stream,Some(limits)).await.map_err(|_|error("channel_unavailable","App-server handshake failed"))?;
        for (id,method,params) in [
            (1,"initialize",json!({"clientInfo":{"name":"tower","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})),

            (3,"turn/start",json!({"threadId":thread,"input":[{"type":"text","text":text,"text_elements":[]}]})),
        ] {
            if id==3 {submitted=true;}
            socket.send(Message::Text(json!({"id":id,"method":method,"params":params}).to_string().into())).await.map_err(|_|error(if submitted{"delivery_unknown"}else{"channel_unavailable"},"App-server request write failed"))?;
            loop {
                let frame=socket.next().await.ok_or_else(||error(if submitted{"delivery_unknown"}else{"channel_unavailable"},"App-server connection closed"))?.map_err(|_|error(if submitted{"delivery_unknown"}else{"channel_unavailable"},"App-server read failed"))?;
                if let Message::Text(text)=frame {
                    let value:Value=serde_json::from_str(&text).map_err(|_|error(if submitted{"delivery_unknown"}else{"channel_unavailable"},"Invalid app-server response"))?;
                    if value["id"]==id && value.get("method").is_none() {
                        if value.get("error").is_some(){return Err(error("delivery_failed","App-server rejected the request"));}
                        if id==3 && value.pointer("/result/turn/id").and_then(Value::as_str).is_none(){return Err(error("delivery_unknown","App-server returned no turn identity"));}
                        break;
                    }
                }else if matches!(frame,Message::Ping(_)){socket.flush().await.map_err(|_|error(if submitted{"delivery_unknown"}else{"channel_unavailable"},"App-server keepalive failed"))?;}
            }
            if id==1 {socket.send(Message::Text(json!({"method":"initialized"}).to_string().into())).await.map_err(|_|error("channel_unavailable","App-server initialization failed"))?;}
        }
        Ok(DeliveryReceipt{method:"native".into(),acknowledgement:"harness".into()})
    }).await;
    result.unwrap_or_else(|_| {
        Err(error(
            if submitted {
                "delivery_unknown"
            } else {
                "channel_unavailable"
            },
            "App-server operation timed out",
        ))
    })
}
