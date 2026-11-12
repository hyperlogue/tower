use std::io::{Error, Result};
use std::{io, time::Duration};
use tower_pal::{Paths, SingletonLock};
use tower_pool_proto::{self as wire, Frame, Reply, Request, Session};

fn connect(paths: &Paths) -> Result<tower_pal::BlockingIpcStream> {
    let mut stream = tower_pal::connect_blocking(&paths.pool_socket())?;
    wire::hello_sync(&mut stream)?;
    Ok(stream)
}
/// Sends a frozen core request, which every pool version answers.
fn core(paths: &Paths, request: &Request) -> Result<(u16, Reply)> {
    let mut stream = tower_pal::connect_blocking(&paths.pool_socket())?;
    let version = wire::core_hello_sync(&mut stream)?;
    wire::write_sync(&mut stream, request)?;
    Ok((
        version,
        wire::read_sync(&mut stream)?.json::<Reply>()?.checked()?,
    ))
}
pub(crate) fn request(paths: &Paths, request: &Request) -> Result<Reply> {
    let mut stream = connect(paths)?;
    wire::write_sync(&mut stream, request)?;
    wire::read_sync(&mut stream)?.json::<Reply>()?.checked()
}
fn listing(paths: &Paths) -> Result<(u16, Vec<Session>)> {
    match core(paths, &Request::List)? {
        (version, Reply::Sessions { sessions }) => Ok((version, sessions)),
        _ => Err(Error::other("invalid pool list reply")),
    }
}
pub(crate) fn list(paths: &Paths) -> Result<Vec<Session>> {
    listing(paths).map(|(_, sessions)| sessions)
}
/// The running pool's protocol version, or `None` when no pool is running.
pub(crate) fn version(paths: &Paths) -> Result<Option<u16>> {
    match listing(paths) {
        Ok((version, _)) => Ok(Some(version)),
        Err(e) if absent(&e) => Ok(None),
        Err(e) => Err(e),
    }
}
fn absent(e: &Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    )
}
/// Starts a pool if none is running and replaces one with another version.
pub(crate) fn ensure(paths: &Paths) -> Result<()> {
    if version(paths)? == Some(wire::VERSION) {
        return Ok(());
    }
    let _lock = SingletonLock::acquire(&paths.run.join("pool-start.lock"))?;
    match version(paths)? {
        Some(wire::VERSION) => return Ok(()),
        Some(_) => shutdown(paths)?,
        None => {}
    }
    start(paths)
}
/// Replaces the running pool, ending its sessions.
pub(crate) fn restart(paths: &Paths) -> Result<()> {
    let _lock = SingletonLock::acquire(&paths.run.join("pool-start.lock"))?;
    if version(paths)?.is_some() {
        shutdown(paths)?;
    }
    start(paths)
}
fn shutdown(paths: &Paths) -> Result<()> {
    core(paths, &Request::Shutdown)?;
    // The pool holds its lock until its sessions have stopped.
    for _ in 0..400 {
        if SingletonLock::acquire(&paths.run.join("pool.lock")).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Err(Error::other("pool did not stop"))
}
fn start(paths: &Paths) -> Result<()> {
    tower_pal::start_pool()?;
    for _ in 0..100 {
        match version(paths)? {
            Some(wire::VERSION) => return Ok(()),
            Some(_) => return Err(Error::other("started pool has another version")),
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    }
    Err(Error::other("pool did not become ready"))
}
pub(crate) fn snapshot(paths: &Paths, id: &str) -> Result<tower_protocol::SnapshotResult> {
    let mut stream = connect(paths)?;
    wire::write_sync(&mut stream, &Request::Snapshot { name: id.into() })?;
    let Reply::Snapshot {
        size,
        truncated,
        limited,
        bytes,
    } = wire::read_sync(&mut stream)?.json::<Reply>()?.checked()?
    else {
        return Err(Error::other("invalid snapshot reply"));
    };
    if bytes > wire::MAX_SNAPSHOT {
        return Err(Error::other("snapshot exceeds limit"));
    }
    let mut buffer = Vec::with_capacity(bytes);
    loop {
        match wire::read_sync(&mut stream)? {
            Frame::Data(data) => {
                if buffer.len() + data.len() > bytes {
                    return Err(Error::other("invalid snapshot size"));
                }
                buffer.extend(data)
            }
            Frame::Json(data) => {
                let reply: Reply = serde_json::from_slice(&data)?;
                if !matches!(reply.checked()?, Reply::End) || buffer.len() != bytes {
                    return Err(Error::other("incomplete snapshot"));
                }
                break;
            }
            Frame::Unknown => {}
        }
    }
    Ok(tower_protocol::SnapshotResult {
        id: id.into(),
        rows: size.rows,
        cols: size.cols,
        buffer: String::from_utf8(buffer).map_err(|_| Error::other("invalid terminal snapshot"))?,
        truncated,
        limited,
    })
}

/// Calls `wake` on every pool session change, reconnecting when the pool
/// socket is replaced. Waits for the socket rather than retrying on a timer.
pub(crate) async fn watch(paths: Paths, wake: std::sync::Arc<dyn Fn() + Send + Sync>) {
    let socket = paths.pool_socket();
    loop {
        let mut replaced = match tower_pal::FileWatch::new(std::slice::from_ref(&socket)) {
            Ok(watch) => watch,
            Err(e) => {
                eprintln!("tower: terminal pool changes are unobservable: {e}");
                return;
            }
        };
        if let Ok(mut stream) = tower_pal::connect(&socket).await {
            let subscribed = tokio::time::timeout(Duration::from_secs(5), async {
                let capabilities = wire::hello_capabilities(&mut stream).await?;
                if !capabilities.iter().any(|c| c == "watch.v1") {
                    return Err(Error::other("pool cannot report changes"));
                }
                wire::write(&mut stream, &Request::Watch).await?;
                wire::read(&mut stream).await?.json::<Reply>()?.checked()
            })
            .await;
            if matches!(subscribed, Ok(Ok(Reply::Ok))) {
                // Changes made while disconnected are read on the first wake.
                wake();
                while let Ok(frame) = wire::read(&mut stream).await {
                    if let Frame::Json(_) = frame {
                        wake();
                    }
                }
                wake();
            }
        }
        replaced.changed().await;
    }
}
