use crate::{Error, Result, worker::ServiceWorker};
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone, Deserialize)]
struct Sink {
    kind: String,
    target: String,
    topics: Vec<String>,
    #[serde(default)]
    args: Vec<String>,
    token_env: Option<String>,
}
pub(super) fn start(
    paths: &tower_pal::Paths,
    worker: &ServiceWorker,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    #[derive(Default, Deserialize)]
    #[serde(default)]
    struct Config {
        event_sinks: Vec<Sink>,
    }
    let config: Config = tower_pal::read_config(&paths.config_home.join("tower/config.toml"))?
        .map(|s| toml::from_str(&s))
        .transpose()
        .map_err(|_| Error::Other("Invalid event sink configuration".into()))?
        .unwrap_or_default();
    if config.event_sinks.len() > 16 {
        return Err(Error::Other("At most 16 event sinks are allowed".into()));
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|_| Error::Other("Cannot create event sink client".into()))?;
    let mut tasks = Vec::new();
    for sink in config.event_sinks {
        if !matches!(sink.kind.as_str(), "webhook" | "command")
            || sink.target.is_empty()
            || sink.target.contains('\0')
            || sink.args.len() > 64
            || sink.args.iter().any(|s| s.len() > 4096 || s.contains('\0'))
            || sink.topics.is_empty()
            || sink.topics.iter().any(|s| {
                !matches!(
                    s.as_str(),
                    "agents" | "sessions" | "messages" | "approvals" | "sync"
                )
            })
        {
            return Err(Error::Other("Invalid event sink".into()));
        }
        if sink.kind == "webhook" {
            crate::allgather::safe_url(&sink.target, false)?;
        }
        let mut events = worker.events();
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(v) => v,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        eprintln!("towerd: event sink lagged; transient events were dropped");
                        continue;
                    }
                    Err(_) => break,
                };
                let topic = match event.method.split('.').next().unwrap_or("") {
                    "agent" => "agents",
                    "session" => "sessions",
                    "message" => "messages",
                    "approval" => "approvals",
                    _ => "sync",
                };
                if !sink.topics.iter().any(|s| s == topic) {
                    continue;
                }
                let mut event = (*event).clone();
                event.params.data.remove("preview");
                let Ok(mut bytes) = serde_json::to_vec(&event) else {
                    continue;
                };
                if bytes.len() > 1024 * 1024 {
                    continue;
                }
                let ok = if sink.kind == "webhook" {
                    let mut request = client
                        .post(&sink.target)
                        .header("Content-Type", "application/json")
                        .body(bytes);
                    if let Some(name) = &sink.token_env {
                        match std::env::var(name) {
                            Ok(token) => request = request.bearer_auth(token),
                            Err(_) => continue,
                        }
                    }
                    request.send().await.is_ok_and(|r| r.status().is_success())
                } else {
                    bytes.push(b'\n');
                    let mut argv = vec![sink.target.clone()];
                    argv.extend(sink.args.clone());
                    tower_pal::command_input(
                        &argv,
                        &tower_pal::environment(),
                        &bytes,
                        Duration::from_secs(5),
                    )
                    .await
                    .is_ok()
                };
                if !ok {
                    eprintln!("towerd: event sink delivery failed");
                }
            }
        }));
    }
    Ok(tasks)
}
