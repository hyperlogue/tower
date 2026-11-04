//! Local daemon and client. OS-specific operations are delegated to tower-pal.

pub mod allgather;
pub mod client;
mod clipboard;
mod http;
mod http_io;
mod metrics;
pub mod policy;
mod service;
mod sinks;
mod vcs;
mod wire;
mod worker;
use service::Service;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, task::JoinSet};
use tower_pal::{IpcListener, Paths, SingletonLock};
use tower_protocol::{Request, RequestId, Response, ResponseBody, RpcError};
use worker::ServiceWorker;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Store(#[from] tower_store::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Rpc(#[from] RpcError),
    #[error("{0}")]
    Other(String),
}
pub type Result<T> = std::result::Result<T, Error>;

pub async fn run(paths: Paths, startup: &str, backends: tower_sessions::Backends) -> Result<()> {
    paths.prepare()?;
    let _lock = SingletonLock::acquire(&paths.run.join("towerd.lock"))?;
    tower_pal::ignore_hangup()?;
    tower_pal::private_file(&paths.database, true)?;
    let store = Arc::new(tower_store::Store::open(&paths.database)?);
    let service = Service::new(paths.clone(), store, startup, backends)?;
    service.recover_registry()?;
    let service = ServiceWorker::new(service);
    let listener = IpcListener::bind(&paths.socket).await?;
    let http = http::start(&paths, service.clone()).await?;
    let allgather = allgather::start(paths.clone(), service.clone())?;
    let sinks = sinks::start(&paths, &service)?;
    let shutdown = Arc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let maintenance_service = service.clone();
    let maintenance = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if !matches!(
                maintenance_service.execute(Service::maintain).await,
                Ok(Ok(()))
            ) {
                eprintln!("towerd: registry maintenance failed");
            }
        }
    });
    let vcs = service.vcs();
    let checkout_refresh = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            vcs.refresh_all().await;
        }
    });
    let signals = tower_pal::shutdown_signal();
    tokio::pin!(signals);
    let result = loop {
        tokio::select! {
            _=shutdown.notified()=>break Ok(()),
            result=&mut signals=>break result.map_err(Error::from),
            Some(result)=tasks.join_next(),if !tasks.is_empty()=>{
                if result.is_err() { eprintln!("towerd: client task failed"); }
            },
            connection=listener.accept(),if tasks.len()<128=>{
                match connection {
                    Ok((stream,peer))=>{
                        let service=service.clone();let shutdown=Arc::clone(&shutdown);
                        tasks.spawn(async move { let _=serve_connection(stream,peer,service,shutdown).await; });
                    },
                    Err(e) if e.kind()==std::io::ErrorKind::PermissionDenied=>{},
                    Err(e)=>break Err(e.into()),
                }
            }
        }
    };
    // Finish an in-flight shutdown reply; idle clients cannot delay termination.
    for sink in sinks {
        sink.abort();
        let _ = sink.await;
    }
    allgather.abort();
    let _ = allgather.await;
    http.shutdown().await;
    tasks.abort_all();
    maintenance.abort();
    checkout_refresh.abort();
    let _ = checkout_refresh.await;
    while tasks.join_next().await.is_some() {}
    let _ = maintenance.await;
    service.drain().await?;
    drop(listener);
    result
}

async fn serve_connection(
    stream: tower_pal::IpcStream,
    peer: tower_pal::Peer,
    service: ServiceWorker,
    shutdown: Arc<Notify>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let (read, mut write) = tokio::io::split(stream);
    let (lines, mut incoming) = tokio::sync::mpsc::channel(1);
    let reader = tokio::spawn(async move {
        let mut reader = BufReader::new(read);
        loop {
            let result = async {
                if reader.fill_buf().await?.is_empty() {
                    return Ok(None);
                }
                tokio::time::timeout(Duration::from_secs(60), wire::read_frame(&mut reader))
                    .await
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "incomplete request timed out",
                        )
                    })?
            }
            .await;
            let end = !matches!(&result, Ok(Some(_)));
            if lines.send(result).await.is_err() || end {
                break;
            }
        }
    });
    struct ReaderGuard(tokio::task::JoinHandle<()>);
    impl Drop for ReaderGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _reader = ReaderGuard(reader);
    let mut clipboard = clipboard::Connection::new(service.clipboard());
    let mut events = service.events();
    let mut subscription: Option<(
        tower_protocol::CallerContext,
        tower_protocol::SubscribeResult,
        Vec<String>,
        Option<String>,
    )> = None;
    let mut check = tokio::time::interval(Duration::from_secs(1));
    loop {
        let line = tokio::select! {
            line=incoming.recv()=>match line {Some(Ok(Some(line)))=>line,Some(Err(e))=>return Err(e.into()),_=>return Ok(())},
            event=clipboard.next()=>{
                clipboard.check(&service,peer).await?;
                if let Some(event)=event {let mut v=serde_json::to_value(event)?;v["jsonrpc"]="2.0".into();wire::write_frame(&mut write,&v).await?;}
                continue;
            }
            event=events.recv(),if subscription.is_some()=>{
                let event=event.map_err(|_|Error::Other("event stream lost data; reconnect and load current state".into()))?;
                let (context,state,topics,approval_id)=subscription.as_ref().expect("guarded");
                if event.params.seq<=state.cursor {continue;}
                let topic=match event.method.split('.').next().unwrap_or(""){"agent"=>"agents","message"=>"messages","approval"=>"approvals","session"=>"sessions",_=>"sync"};
                if approval_id.as_ref().is_some_and(|id|&event.params.id!=id){continue;}
                if !topics.iter().any(|t|t==topic) {continue;}
                let context=context.clone();
                let copy=event.clone();
                if !service.execute(move |s|s.event_visible(&context,peer,&copy)).await?? {continue;}
                wire::write_frame(&mut write,&*event).await?;
                continue;
            }
            _=check.tick(),if subscription.is_some()||clipboard.context().is_some()=>{
                if let Some(subscription)=&subscription {let context=subscription.0.clone();service.execute(move |s|s.check_subscription(&context,peer)).await??;}
                clipboard.check(&service,peer).await?;
                continue;
            }
        };
        let parsed = serde_json::from_slice::<Request>(&line);
        let request = match parsed {
            Ok(r) if r.jsonrpc == "2.0" => r,
            _ => {
                let code = if serde_json::from_slice::<serde_json::Value>(&line).is_err() {
                    -32700
                } else {
                    -32600
                };
                let name = if code == -32700 {
                    "parse_error"
                } else {
                    "invalid_request"
                };
                wire::write_frame(
                    &mut write,
                    &Response {
                        jsonrpc: "2.0".into(),
                        id: RequestId::Null,
                        body: ResponseBody::Failure {
                            error: RpcError::protocol(code, name, "Invalid JSON-RPC request"),
                        },
                    },
                )
                .await?;
                continue;
            }
        };
        let stop = request.method == "daemon.shutdown";
        let subscribing = request.method == "subscribe";
        let unsubscribing = request.method == "unsubscribe";
        let context = request.context.clone();
        let approval_id = if subscribing {
            serde_json::to_value(&request.params)?
                .get("approval_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        } else {
            None
        };
        let topics = if subscribing {
            serde_json::from_value::<tower_protocol::SubscribeParams>(serde_json::to_value(
                &request.params,
            )?)
            .ok()
            .map(|p| p.topics)
        } else {
            None
        };
        if subscribing {
            events = service.events();
        }
        let unsubscribe_valid = if unsubscribing {
            matches!(&request.params,tower_protocol::Value::Object(p) if p.get("subscription_id").and_then(|v|if let tower_protocol::Value::String(s)=v{Some(s)}else{None})==subscription.as_ref().map(|s|&s.1.subscription_id))
        } else {
            true
        };
        let id = request.id.clone();
        let result = if let Some(result) = clipboard.request(&request, peer, &service).await {
            result
        } else if unsubscribe_valid {
            service.request(request, peer).await
        } else {
            Err(RpcError::application(
                "not_found",
                "Subscription does not belong to this connection",
            ))
        };
        if let Ok(value) = &result {
            if subscribing {
                let state = serde_json::from_value::<tower_protocol::SubscribeResult>(
                    serde_json::to_value(value)?,
                )?;
                subscription = Some((context, state, topics.unwrap_or_default(), approval_id));
            } else if unsubscribing {
                subscription = None;
            }
        }
        let stopped = stop && result.is_ok();
        if let Some(id) = id {
            let body = match result {
                Ok(result) => ResponseBody::Success { result },
                Err(error) => ResponseBody::Failure { error },
            };
            wire::write_frame(
                &mut write,
                &Response {
                    jsonrpc: "2.0".into(),
                    id,
                    body,
                },
            )
            .await?;
        }
        if stopped {
            shutdown.notify_one();
            return Ok(());
        }
    }
}
