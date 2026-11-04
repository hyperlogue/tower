use crate::codex_service::{self, Client, Config, Error, Result};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tower_pal::{Paths, SingletonLock};

pub struct Host {
    pub config: Config,
    pub process: Option<tower_pal::ProcessIdentity>,
    pub shell: PathBuf,
}

pub async fn ensure(paths: &Paths) -> Result<Host> {
    let config = Config::load(paths)?;
    let mut client = match Client::connect(&config.endpoint).await {
        Ok(client) => client,
        Err(Error::Unavailable) if config.managed => {
            paths.prepare().map_err(|_| Error::Unavailable)?;
            let lock_path = paths.run.join("codex-start.lock");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            let _lock = loop {
                match SingletonLock::acquire(&lock_path) {
                    Ok(lock) => break lock,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(_) => return Err(Error::Unavailable),
                }
            };
            match Client::connect(&config.endpoint).await {
                Ok(client) => client,
                Err(Error::Unavailable) => {
                    tokio::task::spawn_blocking(tower_pal::start_codex_host)
                        .await
                        .map_err(|_| Error::Unavailable)?
                        .map_err(|_| Error::Unavailable)?;
                    loop {
                        match Client::connect(&config.endpoint).await {
                            Ok(client) => break client,
                            Err(Error::Unavailable) if tokio::time::Instant::now() < deadline => {
                                tokio::time::sleep(Duration::from_millis(25)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(e) => return Err(e),
    };
    let process = client.process();
    let info = client
        .request("environment/info", json!({"environmentId":"local"}))
        .await?;
    let shell = info
        .pointer("/environment/shell/path")
        .or_else(|| info.pointer("/shell/path"))
        .and_then(Value::as_str)
        .ok_or(Error::Protocol)?;
    let shell = PathBuf::from(shell);
    if !shell.is_absolute()
        || !matches!(
            shell.file_name().and_then(|s| s.to_str()),
            Some("bash" | "zsh")
        )
    {
        return Err(Error::Config(
            "Shared Codex requires a Bash or Zsh execution shell",
        ));
    }
    Ok(Host {
        config,
        process,
        shell,
    })
}

pub async fn run(paths: Paths) -> std::io::Result<()> {
    paths.prepare()?;
    let _lock = SingletonLock::acquire(&paths.run.join("codex-host.lock"))?;
    tower_pal::ignore_hangup()?;
    let config = Config::load(&paths).map_err(std::io::Error::other)?;
    if !config.managed {
        return Err(std::io::Error::other(
            "Configured external Codex endpoint has its own lifecycle",
        ));
    }
    tower_pal::prepare_socket_path(&config.endpoint).await?;
    let mut env = tower_pal::environment();
    for key in [
        "TOWER_AGENT_ID",
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_LAUNCHER_SOCKET",
        "TOWER_WRAPPER",
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
    ] {
        env.remove(key);
    }
    if env.get("SHELL").is_none_or(|shell| {
        !matches!(
            std::path::Path::new(shell)
                .file_name()
                .and_then(|n| n.to_str()),
            Some("bash" | "zsh")
        )
    }) {
        let shell = tower_pal::find_program("bash", &env)
            .ok_or_else(|| std::io::Error::other("Codex host needs a supported execution shell"))?;
        env.insert("SHELL".into(), shell.to_string_lossy().into_owned());
    }
    let mut child = tower_pal::spawn_protocol(
        &[
            "codex".into(),
            "app-server".into(),
            "--listen".into(),
            format!("unix://{}", config.endpoint.display()),
        ],
        &env,
        &paths.home,
    )?;
    drop(child.stdin.take());
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("Missing server output"))?;
    let drain = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut stdout, &mut tokio::io::sink()).await;
    });
    let identity = tower_pal::process_identity(
        child
            .id()
            .ok_or_else(|| std::io::Error::other("Missing server identity"))?,
    )?;
    let listener = tower_pal::IpcListener::bind(&paths.run.join("codex-host.sock")).await?;
    let mut tasks = tokio::task::JoinSet::new();
    let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut private = false;
    let signals = tower_pal::shutdown_signal();
    tokio::pin!(signals);
    let result = loop {
        tokio::select! {
            result=child.wait()=>break result.map(|_|()),
            result=&mut signals=>break result,
            _=shutdown.notified()=>break Ok(()),
            _=tick.tick(),if !private=>{private=tower_pal::private_socket(&config.endpoint).is_ok();},
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            accepted=listener.accept(),if tasks.len()<16=>{
                if let Ok((stream,_))=accepted {
                    let shutdown=shutdown.clone();
                    tasks.spawn(async move {
                        let _=tokio::time::timeout(Duration::from_secs(5),async {
                            let mut stream=BufReader::new(stream);let mut bytes=Vec::new();
                            loop {let buf=stream.fill_buf().await?;if buf.is_empty(){return Err(std::io::Error::other("closed"));}let n=buf.iter().position(|b|*b==b'\n').map_or(buf.len(),|n|n+1);if bytes.len()+n>1024{return Err(std::io::Error::other("oversized control"));}bytes.extend_from_slice(&buf[..n]);stream.consume(n);if bytes.last()==Some(&b'\n'){break;}}
                            let v:Value=serde_json::from_slice(&bytes)?;
                            if v["method"]!="shutdown" {return Err(std::io::Error::other("Unknown host operation"));}
                            stream.get_mut().write_all(b"{\"stopping\":true}\n").await?;shutdown.notify_one();Ok::<_,std::io::Error>(())
                        }).await;
                    });
                }
            }
        }
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    if child.try_wait()?.is_none() {
        let _ = tower_pal::kill_protocol_group(identity);
        let _ = child.wait().await;
    }
    drain.abort();
    let _ = drain.await;
    result
}

pub async fn stop(paths: &Paths) -> std::io::Result<()> {
    let stream = match tower_pal::connect(&paths.run.join("codex-host.sock")).await {
        Ok(stream) => stream,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(b"{\"method\":\"shutdown\"}\n")
        .await?;
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line)).await??;
    if line != "{\"stopping\":true}\n" {
        return Err(std::io::Error::other(
            "Invalid host shutdown acknowledgement",
        ));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match SingletonLock::acquire(&paths.run.join("codex-host.lock")) {
            Ok(_) => return Ok(()),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

pub fn namespace(paths: &Paths) -> Result<String> {
    Ok(format!(
        "codex-endpoint:{}",
        Config::load(paths)?.endpoint.display()
    ))
}

pub async fn channel(
    config: &Config,
) -> Result<(
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
    Option<tower_pal::ProcessIdentity>,
)> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let connection = codex_service::connect(&config.endpoint).await?;
    let process = connection.process;
    let (owner, bridge) = tokio::io::duplex(128 * 1024);
    let task = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(bridge);
        let (mut outgoing, mut incoming) = connection.socket.split();
        let send = async {
            let mut read = BufReader::new(read);
            loop {
                let mut line = Vec::new();
                loop {
                    let buf = read.fill_buf().await.map_err(|_| Error::Unavailable)?;
                    if buf.is_empty() {
                        return Ok::<_, Error>(());
                    }
                    let n = buf
                        .iter()
                        .position(|b| *b == b'\n')
                        .map_or(buf.len(), |i| i + 1);
                    if line.len() + n > 16 * 1024 * 1024 {
                        return Err(Error::Limit);
                    }
                    line.extend_from_slice(&buf[..n]);
                    read.consume(n);
                    if line.last() == Some(&b'\n') {
                        break;
                    }
                }
                let value: Value = serde_json::from_slice(&line).map_err(|_| Error::Protocol)?;
                let text = serde_json::to_string(&value).map_err(|_| Error::Protocol)?;
                tokio::time::timeout(
                    Duration::from_secs(5),
                    outgoing.send(Message::Text(text.into())),
                )
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Unavailable)?;
            }
        };
        let receive = async {
            while let Some(frame) = incoming.next().await {
                match frame.map_err(|_| Error::Unavailable)? {
                    Message::Text(text) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|_| Error::Protocol)?;
                        let mut data = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
                        data.push(b'\n');
                        tokio::time::timeout(Duration::from_secs(5), write.write_all(&data))
                            .await
                            .map_err(|_| Error::Timeout)?
                            .map_err(|_| Error::Unavailable)?;
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                    _ => return Err(Error::Protocol),
                }
            }
            Ok::<_, Error>(())
        };
        tokio::select! {_=send=>{},_=receive=>{}}
    });
    Ok((owner, task, process))
}
