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
    events: std::collections::VecDeque<tower_protocol::EventNotification>,
    failed: bool,
}
impl Client {
    pub async fn connect(paths: &Paths, context: CallerContext, autostart: bool) -> Result<Self> {
        let hello = HelloParams {
            protocol_version: PROTOCOL_VERSION,
        };
        Ok(
            Self::connect_calling(paths, context, autostart, "hello", &hello)
                .await?
                .0,
        )
    }
    /// Connects and makes `method` the first request in place of the version
    /// handshake, for a caller that cannot afford both. The request may be
    /// sent more than once, so repeating it must be harmless.
    pub async fn connect_calling<T: serde::Serialize>(
        paths: &Paths,
        context: CallerContext,
        autostart: bool,
        method: &str,
        params: &T,
    ) -> Result<(Self, Value)> {
        let mut retries = 0;
        loop {
            match Self::connect_once(paths, context.clone(), autostart, method, params).await {
                Err(Error::Io(error))
                    if autostart
                        && retries < 2
                        && matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionReset
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::UnexpectedEof
                        ) =>
                {
                    // The old listener can accept a socket while its process exits.
                    // Only the first request is retried, and it is safe to repeat.
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                result => return result,
            }
        }
    }
    async fn connect_once<T: serde::Serialize>(
        paths: &Paths,
        context: CallerContext,
        autostart: bool,
        method: &str,
        params: &T,
    ) -> Result<(Self, Value)> {
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
                                "daemon did not become ready; inspect the private server log"
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
            events: Default::default(),
            failed: false,
        };
        let first = client.call(method, params).await?;
        Ok((client, first))
    }
    pub async fn call<T: serde::Serialize>(&mut self, method: &str, params: &T) -> Result<Value> {
        if self.failed {
            return Err(Error::Other(
                "RPC connection is unusable; reconnect before retrying".into(),
            ));
        }
        // A cancelled read can consume only part of a frame. Keep this set
        // until a complete response confirms that the stream is usable.
        self.failed = true;
        let result = self.call_inner(method, params).await;
        if result.is_ok() || matches!(result, Err(Error::Rpc(_))) {
            self.failed = false;
        }
        result
    }
    async fn call_inner<T: serde::Serialize>(&mut self, method: &str, params: &T) -> Result<Value> {
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
        let timeout = if method == "approval.watch" {
            610
        } else if method == "vcs.prepare" || method == "vcs.execute" {
            210
        } else if method == "message.send"
            || method == "pool.upgrade"
            || method.starts_with("allgather.enrollment.")
        {
            30
        } else {
            10
        };
        let response = tokio::time::timeout(Duration::from_secs(timeout), async {
            loop {
                let frame = wire::read_response(&mut self.reader)
                    .await?
                    .ok_or_else(|| {
                        Error::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "daemon disconnected",
                        ))
                    })?;
                let parsed: serde_json::Value = serde_json::from_slice(&frame)?;
                if parsed.get("method").is_some() {
                    if self.events.len() >= 256 {
                        return Err(Error::Other(
                            "event buffer full; reconnect and reload current state".into(),
                        ));
                    }
                    self.events.push_back(serde_json::from_value(parsed)?);
                    continue;
                }
                return Ok::<Response, Error>(serde_json::from_value(parsed)?);
            }
        })
        .await
        .map_err(|_| Error::Other("daemon request timed out".into()))??;

        if response.jsonrpc != "2.0" || response.id != id {
            return Err(Error::Other("invalid RPC response identity".into()));
        }
        match response.body {
            ResponseBody::Success { result } => Ok(result),
            ResponseBody::Failure { error } => Err(error.into()),
        }
    }
    pub async fn next_event(&mut self) -> Result<tower_protocol::EventNotification> {
        if self.failed {
            return Err(Error::Other(
                "Event connection is unusable; reconnect and reload current state".into(),
            ));
        }
        if let Some(event) = self.events.pop_front() {
            return Ok(event);
        }
        self.failed = true;
        let frame = wire::read_response(&mut self.reader)
            .await?
            .ok_or_else(|| {
                Error::Other("event stream disconnected; reconnect and reload current state".into())
            })?;
        let event = serde_json::from_slice(&frame)?;
        self.failed = false;
        Ok(event)
    }
}
async fn connect(path: &std::path::Path) -> io::Result<tower_pal::IpcStream> {
    tokio::time::timeout(Duration::from_millis(500), tower_pal::connect(path))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket connection timed out"))?
}
