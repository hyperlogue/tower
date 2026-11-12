mod clipboard;
mod input;
mod preferences;
mod session;
mod terminal;
use session::{Command, Handle, Out};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tower_pal::{IpcStream, Paths, SingletonLock};
use tower_pool_proto::{self as wire, Frame, Launch, Reply, Request};

type Sessions = Arc<Mutex<BTreeMap<String, Handle>>>;
pub async fn run(paths: Paths) -> io::Result<()> {
    paths.prepare()?;
    let _lock = SingletonLock::acquire(&paths.run.join("pool.lock"))?;
    tower_pal::ignore_hangup()?;
    let listener = tower_pal::IpcListener::bind(&paths.pool_socket()).await?;
    let sessions: Sessions = Arc::new(Mutex::new(BTreeMap::new()));
    let changes: session::Changes = Arc::new(tokio::sync::watch::channel(0).0);
    let shutdown = Arc::new(Notify::new());
    let preferences = Arc::new(Mutex::new(preferences::Preferences::open(
        paths.state.join("terminal.json"),
    )?));
    let mut clients = tokio::task::JoinSet::new();
    let signals = tower_pal::shutdown_signal();
    tokio::pin!(signals);
    let result = loop {
        tokio::select! {
         _=shutdown.notified()=>break Ok(()),r=&mut signals=>break r,
         Some(_)=clients.join_next(),if !clients.is_empty()=>{},
         accepted=listener.accept(),if clients.len()<128=>match accepted {
          Ok((stream,_))=>{let sessions=Arc::clone(&sessions);let shutdown=Arc::clone(&shutdown);let preferences=Arc::clone(&preferences);let changes=Arc::clone(&changes);clients.spawn(async move {let _=connection(stream,sessions,shutdown,preferences,changes).await;});},
          Err(e) if e.kind()==io::ErrorKind::PermissionDenied=>{},Err(e)=>break Err(e)
         }
        }
    };
    clients.abort_all();
    while clients.join_next().await.is_some() {}
    drop(listener);
    let handles: Vec<_> = sessions.lock().unwrap().values().cloned().collect();
    for h in handles {
        let (tx, rx) = oneshot::channel();
        if h.tx.send(Command::Stop(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }
    result
}
fn find(sessions: &Sessions, name: &str) -> Option<Handle> {
    sessions.lock().unwrap().get(name).cloned()
}
fn validate(launch: &mut Launch) -> io::Result<()> {
    if launch.name.is_empty()
        || launch.name.len() > 128
        || !launch
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(io::Error::other("invalid session name"));
    }
    if launch.argv.is_empty()
        || launch.argv.len() > 256
        || !std::path::Path::new(&launch.cwd).is_absolute()
    {
        return Err(io::Error::other("invalid launch specification"));
    }
    if let Some(size) = launch.size {
        launch.size = Some(size.bounded()?);
    }
    if launch
        .term
        .as_deref()
        .is_some_and(|term| !wire::valid_term(term))
    {
        return Err(io::Error::other("invalid terminal type"));
    }
    Ok(())
}
async fn reply(stream: &mut IpcStream, response: &Reply) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), wire::write(stream, response))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}
async fn connection(
    mut stream: IpcStream,
    sessions: Sessions,
    shutdown: Arc<Notify>,
    preferences: Arc<Mutex<preferences::Preferences>>,
    changes: session::Changes,
) -> io::Result<()> {
    let first = tokio::time::timeout(Duration::from_secs(5), wire::read(&mut stream))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    // Frozen core: answer Hello from every client version, then serve only
    // List and Shutdown when versions differ (see tower_pool_proto).
    let Request::Hello { version } = first.json::<Request>()? else {
        return reply(
            &mut stream,
            &Reply::error("invalid_request", "expected hello"),
        )
        .await;
    };
    let core_only = version != wire::VERSION;
    reply(
        &mut stream,
        &Reply::Hello {
            version: wire::VERSION,
            capabilities: vec![
                "attach.multi.v1".into(),
                "snapshot.v1".into(),
                "inject_if_idle.v1".into(),
                "terminal.kitty.v1".into(),
                "clipboard.images.v1".into(),
                "watch.v1".into(),
            ],
        },
    )
    .await?;
    let request = tokio::time::timeout(Duration::from_secs(5), wire::read(&mut stream))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??
        .json::<Request>()?;
    if core_only && !matches!(request, Request::List | Request::Shutdown) {
        return reply(
            &mut stream,
            &Reply::error("protocol_incompatible", "pool version differs"),
        )
        .await;
    }
    match request {
        Request::Watch => {
            let mut seen = changes.subscribe();
            reply(&mut stream, &Reply::Ok).await?;
            while seen.changed().await.is_ok() {
                seen.borrow_and_update();
                reply(&mut stream, &Reply::Changed).await?;
            }
            Ok(())
        }
        Request::Inject { name, proof, text } => {
            let Some(handle) = find(&sessions, &name) else {
                return reply(&mut stream, &Reply::error("not_found", "Session not found")).await;
            };
            let (answer, wait) = oneshot::channel();
            handle
                .tx
                .send(Command::Inject {
                    proof,
                    text,
                    reply: answer,
                })
                .await
                .map_err(io::Error::other)?;
            let result = tokio::time::timeout(Duration::from_secs(3), wait).await;
            let response = match result {
                Ok(Ok(Ok(()))) => Reply::Ok,
                Ok(Ok(Err(code))) => Reply::error(&code, "PTY submission was not confirmed"),
                _ => Reply::error(
                    "delivery_unknown",
                    "PTY submission outcome is unknown; do not automatically retry",
                ),
            };
            reply(&mut stream, &response).await
        }
        Request::List => {
            let records = sessions
                .lock()
                .unwrap()
                .values()
                .map(|s| s.info.borrow().clone())
                .collect();
            reply(&mut stream, &Reply::Sessions { sessions: records }).await
        }
        Request::Create { mut launch } => {
            if let Err(error) = validate(&mut launch) {
                return reply(
                    &mut stream,
                    &Reply::error("invalid_params", error.to_string()),
                )
                .await;
            }
            let reserved = {
                let mut map = sessions.lock().unwrap();
                if let Some(existing) = map.get(&launch.name) {
                    Err(if existing.launch == launch {
                        Reply::Created {
                            session: existing.info.borrow().clone(),
                        }
                    } else {
                        Reply::error("conflict", "session name already has a different launch")
                    })
                } else {
                    if map.len() >= 64 {
                        let ended = map
                            .iter()
                            .find(|(_, h)| h.info.borrow().state == "exited")
                            .map(|(name, _)| name.clone());
                        if let Some(name) = ended {
                            map.remove(&name);
                        }
                    }
                    if map.len() >= 64 {
                        Err(Reply::error("limit", "session limit reached"))
                    } else {
                        let handle = session::reserve(
                            launch.clone(),
                            preferences.lock().unwrap().current(),
                            Arc::clone(&changes),
                        );
                        map.insert(launch.name.clone(), handle.clone());
                        changes.send_modify(|revision| *revision = revision.wrapping_add(1));
                        Ok(handle)
                    }
                }
            };
            let handle = match reserved {
                Ok(handle) => handle,
                Err(response) => return reply(&mut stream, &response).await,
            };
            if !launch.defer {
                let size = handle.info.borrow().size;
                let (answer, wait) = oneshot::channel();
                handle
                    .tx
                    .send(Command::Start {
                        size,
                        reply: answer,
                    })
                    .await
                    .map_err(io::Error::other)?;
                if let Err(error) = wait.await.map_err(io::Error::other)? {
                    sessions.lock().unwrap().remove(&launch.name);
                    return reply(&mut stream, &Reply::error("launch_failed", error)).await;
                }
            }
            let session = handle.info.borrow().clone();
            reply(&mut stream, &Reply::Created { session }).await
        }
        Request::Remove { name } => {
            let handle = {
                let mut map = sessions.lock().unwrap();
                if map
                    .get(&name)
                    .is_some_and(|h| h.info.borrow().state != "exited")
                {
                    None
                } else {
                    map.remove(&name)
                }
            };
            if let Some(h) = handle {
                let (tx, rx) = oneshot::channel();
                let _ = h.tx.send(Command::Stop(tx)).await;
                let _ = rx.await;
                changes.send_modify(|revision| *revision = revision.wrapping_add(1));
                reply(&mut stream, &Reply::Ok).await
            } else {
                reply(
                    &mut stream,
                    &Reply::error("not_exited", "Only exited sessions can be removed"),
                )
                .await
            }
        }
        Request::Attach {
            name,
            term,
            size,
            clipboard,
        } => {
            let size = size.bounded()?;
            if !wire::valid_term(&term) {
                return reply(
                    &mut stream,
                    &Reply::error("invalid_params", "Invalid terminal type"),
                )
                .await;
            }
            let Some(handle) = find(&sessions, &name) else {
                return reply(&mut stream, &Reply::error("not_found", "session not found")).await;
            };
            let (tx, mut output) = mpsc::channel(32);
            let (answer, wait) = oneshot::channel();
            handle
                .tx
                .send(Command::Attach {
                    size,
                    term: term.clone(),
                    out: tx,
                    reply: answer,
                })
                .await
                .map_err(io::Error::other)?;
            let id = match wait.await.map_err(io::Error::other)? {
                Ok(id) => id,
                Err(error) => {
                    return reply(&mut stream, &Reply::error("attach_failed", error)).await;
                }
            };
            if let Some(ticket) = clipboard {
                let Some(config) = &handle.launch.clipboard else {
                    let _ = handle.tx.send(Command::Detach { id }).await;
                    return reply(
                        &mut stream,
                        &Reply::error("unsupported", "Session does not support clipboard input"),
                    )
                    .await;
                };
                let params = tower_protocol::ClipboardAttach {
                    id: name.clone(),
                    generation: config.generation.clone(),
                    client_id: ticket.client_id,
                    ticket: ticket.ticket,
                };
                match tower_clipboard::client::attach(std::path::Path::new(&config.socket), &params)
                    .await
                {
                    Ok((lease, connection)) => {
                        handle
                            .tx
                            .send(Command::Clipboard {
                                id,
                                lease,
                                connection,
                            })
                            .await
                            .map_err(io::Error::other)?;
                    }
                    Err(e) => {
                        let _ = handle.tx.send(Command::Detach { id }).await;
                        return reply(&mut stream, &Reply::error(&e.data.code, e.message)).await;
                    }
                }
            }
            let remembered = preferences.lock().unwrap().remember(term, size);
            if let Err(error) = remembered {
                let _ = handle.tx.send(Command::Detach { id }).await;
                return reply(
                    &mut stream,
                    &Reply::error("preferences_unavailable", error.to_string()),
                )
                .await;
            }
            let (read, mut write) = stream.split();
            let mut read = read;
            let reader = async {
                loop {
                    match wire::read(&mut read).await? {
                        Frame::Data(bytes) => handle
                            .tx
                            .send(Command::Input { id, bytes })
                            .await
                            .map_err(io::Error::other)?,
                        Frame::Json(bytes) => match serde_json::from_slice::<Request>(&bytes)
                            .map_err(io::Error::other)?
                        {
                            Request::Resize { size } => handle
                                .tx
                                .send(Command::Resize {
                                    id,
                                    size: size.bounded()?,
                                })
                                .await
                                .map_err(io::Error::other)?,
                            Request::Detach => return Ok::<_, io::Error>(()),
                            _ => {}
                        },
                        Frame::Unknown => {}
                    }
                }
            };
            let writer = async {
                while let Some(item) = output.recv().await {
                    let ended = matches!(item, Out::Control(Reply::Exited { .. }));
                    let send = async {
                        match item {
                            Out::Control(r) => wire::write(&mut write, &r).await,
                            Out::Data(bytes) => wire::data(&mut write, &bytes).await,
                        }
                    };
                    tokio::time::timeout(Duration::from_secs(5), send)
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
                    if ended {
                        break;
                    }
                }
                Ok::<_, io::Error>(())
            };
            tokio::select! {_=reader=>{},_=writer=>{}}
            let _ = handle.tx.send(Command::Detach { id }).await;
            Ok(())
        }
        Request::Kill { name } => {
            let Some(h) = find(&sessions, &name) else {
                return reply(&mut stream, &Reply::error("not_found", "session not found")).await;
            };
            let (tx, rx) = oneshot::channel();
            h.tx.send(Command::Kill(tx))
                .await
                .map_err(io::Error::other)?;
            rx.await.map_err(io::Error::other)?;
            reply(&mut stream, &Reply::Ok).await
        }
        Request::Snapshot { name } => {
            let Some(h) = find(&sessions, &name) else {
                return reply(&mut stream, &Reply::error("not_found", "session not found")).await;
            };
            let (tx, rx) = oneshot::channel();
            h.tx.send(Command::Snapshot(tx))
                .await
                .map_err(io::Error::other)?;
            let s = rx.await.map_err(io::Error::other)?;
            reply(
                &mut stream,
                &Reply::Snapshot {
                    size: s.size,
                    truncated: s.truncated,
                    limited: s.limited,
                    bytes: s.buffer.len(),
                },
            )
            .await?;
            tokio::time::timeout(Duration::from_secs(5), wire::data(&mut stream, &s.buffer))
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
            reply(&mut stream, &Reply::End).await
        }
        Request::Shutdown => {
            reply(&mut stream, &Reply::Ok).await?;
            shutdown.notify_one();
            Ok(())
        }
        _ => {
            reply(
                &mut stream,
                &Reply::error("unsupported", "unsupported pool request"),
            )
            .await
        }
    }
}
