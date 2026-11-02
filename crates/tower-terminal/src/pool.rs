use std::io::{Error, Result};
use std::{io, time::Duration};
use tower_pal::{Paths, SingletonLock};
use tower_pool_proto::{self as wire, Frame, Reply, Request, Session};

fn connect(paths: &Paths) -> Result<tower_pal::BlockingIpcStream> {
    let mut stream = tower_pal::connect_blocking(&paths.pool_socket())?;
    wire::hello_sync(&mut stream)?;
    Ok(stream)
}
pub(crate) fn request(paths: &Paths, request: &Request) -> Result<Reply> {
    let mut stream = connect(paths)?;
    wire::write_sync(&mut stream, request)?;
    wire::read_sync(&mut stream)?.json::<Reply>()?.checked()
}
pub(crate) fn list(paths: &Paths) -> Result<Vec<Session>> {
    match request(paths, &Request::List)? {
        Reply::Sessions { sessions } => Ok(sessions),
        _ => Err(Error::other("invalid pool list reply")),
    }
}
pub(crate) fn ensure(paths: &Paths) -> Result<()> {
    match tower_pal::connect_blocking(&paths.pool_socket()) {
        Ok(_) => {
            list(paths)?;
            return Ok(());
        }
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) => {}
        Err(e) => return Err(e),
    }
    let _lock = SingletonLock::acquire(&paths.run.join("pool-start.lock"))?;
    match tower_pal::connect_blocking(&paths.pool_socket()) {
        Ok(_) => {
            list(paths)?;
            return Ok(());
        }
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) => {}
        Err(e) => return Err(e),
    }
    tower_pal::start_pool()?;
    for _ in 0..100 {
        match list(paths) {
            Ok(_) => return Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(e) => return Err(e),
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
