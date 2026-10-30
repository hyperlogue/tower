//! Local daemon and client. OS-specific operations are delegated to tower-pal.

pub mod client;
pub mod policy;
mod service;
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

pub async fn run(paths: Paths, startup: &str) -> Result<()> {
    paths.prepare()?;
    let _lock = SingletonLock::acquire(&paths.run.join("towerd.lock"))?;
    tower_pal::ignore_hangup()?;
    tower_pal::private_file(&paths.database, true)?;
    let store = Arc::new(tower_store::LocalStore::open(&paths.database)?);
    let service = Service::new(paths.clone(), store, startup)?;
    service.recover_registry()?;
    let service = ServiceWorker::new(service);
    let listener = IpcListener::bind(&paths.socket).await?;
    let shutdown = Arc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let maintenance_service = service.clone();
    let maintenance = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
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
    tasks.abort_all();
    maintenance.abort();
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
    use tokio::io::BufReader;
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = BufReader::new(read);
    loop {
        let line = match tokio::time::timeout(
            Duration::from_secs(60),
            wire::read_frame(&mut reader),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Ok(()),
        };
        let Some(line) = line else { return Ok(()) };
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
        let id = request.id.clone();
        let result = service
            .execute(move |service| service.dispatch(&request, peer))
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
