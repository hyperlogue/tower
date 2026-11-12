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
    ensure_configured(paths, Config::load(paths)?).await
}

pub async fn ensure_configured(paths: &Paths, config: Config) -> Result<Host> {
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
                    start_daemon(paths).await?;
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                    loop {
                        match Client::connect(&config.endpoint).await {
                            Ok(client) => break client,
                            Err(Error::Unavailable) if tokio::time::Instant::now() < deadline => {
                                tokio::time::sleep(Duration::from_millis(25)).await;
                            }
                            Err(Error::Unavailable) => {
                                return Err(Error::DaemonStart(
                                    "finished, but its socket did not accept connections",
                                ));
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
            "Codex app-server must run with a Bash or Zsh shell; restart it with SHELL set to one, \
             for example `SHELL=/bin/bash codex app-server daemon restart`",
        ));
    }
    Ok(Host {
        config,
        process,
        shell,
    })
}

/// Asks Codex to start its own app-server daemon. The daemon inherits this
/// environment and directory, so neither may carry a caller's identity.
async fn start_daemon(paths: &Paths) -> Result<()> {
    let mut env = tower_pal::environment();
    for key in [
        "TOWER_AGENT_ID",
        "TOWER_AGENT_TOKEN",
        "TOWER_BEARER_TOKEN",
        "TOWER_LAUNCHER_SOCKET",
        "TOWER_WRAPPER",
        crate::config::HELPER,
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
    }) && let Some(shell) = tower_pal::find_program("bash", &env)
    {
        env.insert("SHELL".into(), shell.to_string_lossy().into_owned());
    }
    let codex = tower_pal::find_program("codex", &env).ok_or(Error::DaemonStart(
        "could not run because `codex` is not on PATH",
    ))?;
    let status = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(codex)
            .args(["app-server", "daemon", "start"])
            .env_clear()
            .envs(&env)
            .current_dir(&paths.home)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .status(),
    )
    .await;
    match status {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(_)) => Err(Error::DaemonStart("failed")),
        Ok(Err(_)) => Err(Error::DaemonStart("could not run")),
        Err(_) => Err(Error::DaemonStart("did not finish within 60 seconds")),
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
