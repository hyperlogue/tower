use crate::{Error, Result, wire};
use std::{io, time::Duration};
use tokio::io::BufReader;
use tower_pal::{Paths, SingletonLock};
use tower_protocol::{
    CallerContext, HelloParams, PROTOCOL_VERSION, Request, RequestId, Response, ResponseBody, Value,
};

pub struct Client {
    reader: BufReader<tower_pal::IpcStream>,
    context: CallerContext,
    next: u64,
}
impl Client {
    pub async fn connect(paths: &Paths, context: CallerContext, autostart: bool) -> Result<Self> {
        let stream = match connect(&paths.socket).await {
            Ok(s) => s,
            Err(e)
                if autostart
                    && matches!(
                        e.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) =>
            {
                paths.prepare()?;
                // Keep the OS lock across startup to serialize independent CLI processes.
                let mut lock = None;
                for _ in 0..100 {
                    match SingletonLock::acquire(&paths.run.join("startup.lock")) {
                        Ok(l) => {
                            lock = Some(l);
                            break;
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            tokio::time::sleep(Duration::from_millis(50)).await
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                let _lock = lock
                    .ok_or_else(|| Error::Other("another daemon startup did not finish".into()))?;
                match connect(&paths.socket).await {
                    Ok(stream) => stream,
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                        ) =>
                    {
                        let spawn_paths = paths.clone();
                        tokio::task::spawn_blocking(move || tower_pal::start_daemon(&spawn_paths))
                            .await
                            .map_err(|e| Error::Other(e.to_string()))??;
                        let mut stream = None;
                        for _ in 0..100 {
                            match connect(&paths.socket).await {
                                Ok(s) => {
                                    stream = Some(s);
                                    break;
                                }
                                Err(e)
                                    if matches!(
                                        e.kind(),
                                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                                    ) =>
                                {
                                    tokio::time::sleep(Duration::from_millis(50)).await
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                        stream.ok_or_else(|| {
                            Error::Other(
                                "daemon did not become ready; inspect the private towerd log"
                                    .into(),
                            )
                        })?
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) => return Err(e.into()),
        };
        let mut client = Self {
            reader: BufReader::new(stream),
            context,
            next: 0,
        };
        client
            .call(
                "hello",
                &HelloParams {
                    protocol_version: PROTOCOL_VERSION,
                },
            )
            .await?;
        Ok(client)
    }
    pub async fn call<T: serde::Serialize>(&mut self, method: &str, params: &T) -> Result<Value> {
        self.next += 1;
        let id = RequestId::String(self.next.to_string());
        let request = Request {
            jsonrpc: "2.0".into(),
            id: Some(id.clone()),
            method: method.into(),
            params: serde_json::from_value(serde_json::to_value(params)?)?,
            context: self.context.clone(),
        };
        wire::write_frame(self.reader.get_mut(), &request).await?;
        let frame =
            tokio::time::timeout(Duration::from_secs(10), wire::read_frame(&mut self.reader))
                .await
                .map_err(|_| Error::Other("daemon request timed out".into()))??
                .ok_or_else(|| Error::Other("daemon disconnected".into()))?;
        let response: Response = serde_json::from_slice(&frame)?;
        if response.jsonrpc != "2.0" || response.id != id {
            return Err(Error::Other("invalid RPC response identity".into()));
        }
        match response.body {
            ResponseBody::Success { result } => Ok(result),
            ResponseBody::Failure { error } => Err(error.into()),
        }
    }
}
async fn connect(path: &std::path::Path) -> io::Result<tower_pal::IpcStream> {
    tokio::time::timeout(Duration::from_millis(500), tower_pal::connect(path))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket connection timed out"))?
}
