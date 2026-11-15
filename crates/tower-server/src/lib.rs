//! Local daemon and client. OS-specific operations are delegated to tower-pal.

pub mod allgather;
pub mod client;
mod clipboard;
mod discovery;
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

/// A killed predecessor can refuse connections before the kernel releases its
/// lock. Wait for it, but give up while the client is still polling for us.
async fn acquire_singleton(paths: &Paths) -> std::io::Result<SingletonLock> {
    let path = paths.run.join("towerd.lock");
    let mut attempts = 0;
    loop {
        match SingletonLock::acquire(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && attempts < 60 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            result => return result,
        }
    }
}

/// Runs the server. `backends` is built only after the singleton lock is
/// held, so recovery of a previous server's sessions never runs twice.
pub async fn run<F, B>(paths: Paths, startup: &str, backends: B) -> Result<()>
where
    B: FnOnce(Arc<tower_store::Database>) -> F,
    F: std::future::Future<Output = Result<tower_sessions::Backends>>,
{
    paths.prepare()?;
    let _lock = acquire_singleton(&paths).await?;
    tower_pal::ignore_hangup()?;
    let database = Arc::new(tower_store::Database::open(&paths.database)?);
    let backends = backends(database.clone()).await?;
    let execution = backends.execution().cloned();
    let store = Arc::new(tower_store::Store::new(database));
    let service = Service::new(paths.clone(), store, startup, backends)?;
    service.recover_registry()?;
    let service = ServiceWorker::new(service);
    if let Some(execution) = &execution {
        let worker = service.clone();
        execution.bind(Arc::new(move |request| {
            let worker = worker.clone();
            Box::pin(async move {
                worker
                    .execute(move |s| s.authorize_execution(&request))
                    .await
                    .unwrap_or_else(|_| {
                        Err(RpcError::application(
                            "authority_unavailable",
                            "Tower authorization is unavailable",
                        ))
                    })
            })
        }));
    }
    let listener = IpcListener::bind(&paths.socket).await?;
    let http = http::start(&paths, service.clone()).await?;
    let allgather = allgather::start(paths.clone(), service.clone())?;
    let sinks = sinks::start(&paths, &service)?;
    let shutdown = Arc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let maintenance = tokio::spawn(maintain(service.clone()));
    let discovery = discovery::start(service.clone(), &paths.home);
    let checkout_refresh = tokio::spawn(service.vcs().observe_changes(service.events()));
    let signals = tower_pal::shutdown_signal();
    tokio::pin!(signals);
    let result = loop {
        tokio::select! {
            _=shutdown.notified()=>break Ok(()),
            result=&mut signals=>break result.map_err(Error::from),
            Some(result)=tasks.join_next(),if !tasks.is_empty()=>{
                if result.is_err() { eprintln!("tower: client task failed"); }
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
    for task in &discovery {
        task.abort();
    }
    checkout_refresh.abort();
    let _ = checkout_refresh.await;
    while tasks.join_next().await.is_some() {}
    let _ = maintenance.await;
    if let Some(execution) = &execution {
        execution.shutdown().await;
    }
    service.drain().await?;
    drop(listener);
    result
}

/// Runs maintenance when a backend reports a change, a watched process exits,
/// or a stored deadline falls due; otherwise it sleeps.
async fn maintain(service: ServiceWorker) {
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;
    let wake = service.wake();
    let pool = wake.clone();
    service.watch_backends(std::sync::Arc::new(move || pool.run()));
    let mut exits: BTreeMap<(u32, u64), tokio::task::JoinHandle<()>> = BTreeMap::new();
    let mut managed = true;
    let mut due = true;
    loop {
        // A process exit may end a managed session, so it re-reads managed state too.
        managed |= wake.run.swap(false, Ordering::SeqCst);
        if (managed || due)
            && !matches!(
                service.execute(move |s| s.maintain_with(managed)).await,
                Ok(Ok(()))
            )
        {
            eprintln!("tower: registry maintenance failed");
            // A failed expiry can leave the same deadline in the past. Keep
            // the pending work, but do not retry it in a tight loop.
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        (managed, due) = (false, false);
        let (deadline, processes) = match service.execute(|s| s.maintenance_plan()).await {
            Ok(Ok(plan)) => plan,
            _ => {
                eprintln!("tower: maintenance planning failed");
                (None, Vec::new())
            }
        };
        // A fired watcher stays until its record changes, so an exit is reported once.
        exits.retain(|key, task| {
            let keep = processes.contains(key);
            if !keep {
                task.abort();
            }
            keep
        });
        for (pid, start) in processes {
            exits.entry((pid, start)).or_insert_with(|| {
                let wake = wake.clone();
                tokio::spawn(async move {
                    if let Ok(exit) = tower_pal::ProcessExit::watch(pid, start) {
                        exit.exited().await;
                    }
                    wake.run();
                })
            });
        }
        let sleep = deadline.map(|at| {
            let wait = at.saturating_add(1).saturating_sub(service::now_ms());
            tokio::time::Instant::now() + Duration::from_millis(wait)
        });
        tokio::select! {
            _ = wake.notify.notified() => {}
            _ = async { match sleep { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => due = true,
        }
    }
}

/// The earliest expiry among the credentials a connection is acting on.
async fn credential_deadline(
    service: &ServiceWorker,
    subscription: Option<&tower_protocol::CallerContext>,
    clipboard: Option<tower_protocol::CallerContext>,
) -> Result<Option<tokio::time::Instant>> {
    let contexts: Vec<_> = subscription.cloned().into_iter().chain(clipboard).collect();
    if contexts.iter().all(|c| c.bearer_token.is_none()) {
        return Ok(None);
    }
    let at = service
        .execute(move |s| contexts.iter().filter_map(|c| s.credential_expiry(c)).min())
        .await?;
    Ok(at.map(|at| {
        tokio::time::Instant::now()
            + Duration::from_millis(at.saturating_sub(service::now_ms()).saturating_add(1))
    }))
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
    let mut authority = service.authority_changes();
    let mut expiry: Option<tokio::time::Instant> = None;
    loop {
        let line = tokio::select! {
            line=incoming.recv()=>match line {Some(Ok(Some(line)))=>line,Some(Err(e))=>return Err(e.into()),_=>return Ok(())},
            event=clipboard.next()=>{
                clipboard.check(&service,peer).await?;
                expiry=credential_deadline(&service,subscription.as_ref().map(|s|&s.0),clipboard.context()).await?;
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
            _=async {
                tokio::select! {
                    _=authority.changed()=>{},
                    _=async { match expiry { Some(at)=>tokio::time::sleep_until(at).await, None=>std::future::pending().await } }=>{},
                }
            },if subscription.is_some()||clipboard.context().is_some()=>{
                if let Some(subscription)=&subscription {let context=subscription.0.clone();service.execute(move |s|s.check_subscription(&context,peer)).await??;}
                clipboard.check(&service,peer).await?;
                expiry=credential_deadline(&service,subscription.as_ref().map(|s|&s.0),clipboard.context()).await?;
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
        expiry = credential_deadline(
            &service,
            subscription.as_ref().map(|s| &s.0),
            clipboard.context(),
        )
        .await?;
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

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn failed_maintenance_does_not_spin_on_an_overdue_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let paths = Paths {
            run: state.join("run"),
            socket: state.join("run/towerd.sock"),
            database: state.join("tower.db"),
            tokens: state.join("tokens"),
            logs: state.join("logs"),
            config_home: dir.path().join("config"),
            home: dir.path().join("home"),
            state,
        };
        paths.prepare().unwrap();
        let config = paths.config_home.join("tower");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("config.toml"), "[scan]\nenabled=false\n").unwrap();
        let database = Arc::new(tower_store::Database::open(&paths.database).unwrap());
        let store = Arc::new(tower_store::Store::new(database.clone()));
        let service =
            Service::new(paths, store, "test", tower_sessions::Backends::default()).unwrap();
        database.lock().unwrap().execute(
            "INSERT INTO vcs_operations(id,actor,state,prepared,record) VALUES('expired','human','prepared','{}','{\"expires_at\":0}')", [],
        ).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = attempts.clone();
        database
            .lock()
            .unwrap()
            .authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
                if matches!(context.action, rusqlite::hooks::AuthAction::Delete { .. }) {
                    count.fetch_add(1, Ordering::SeqCst);
                    rusqlite::hooks::Authorization::Deny
                } else {
                    rusqlite::hooks::Authorization::Allow
                }
            }))
            .unwrap();
        let worker = ServiceWorker::new(service);
        let task = tokio::spawn(maintain(worker.clone()));
        tokio::time::sleep(Duration::from_millis(150)).await;
        task.abort();
        let _ = task.await;
        worker.execute(|_| ()).await.unwrap();
        let attempts = attempts.load(Ordering::SeqCst);
        assert!(attempts > 0, "maintenance never ran");
        assert!(
            attempts <= 2,
            "failed maintenance retried {attempts} times in 150ms"
        );
    }
}
