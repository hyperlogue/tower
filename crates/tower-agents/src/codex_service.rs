use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_MESSAGE: usize = 16 * 1024 * 1024;
const MAX_REQUEST: usize = 1024 * 1024;
const MAX_EXCHANGE: usize = 32 * 1024 * 1024;
const MAX_PAGES: usize = 100;
const MAX_ENTRIES: usize = 10_000;

pub type Result<T> = std::result::Result<T, Error>;
pub type Socket = WebSocketStream<tower_pal::IpcStream>;

#[derive(Debug)]
pub enum Error {
    Config(&'static str),
    Unavailable,
    Timeout,
    Protocol,
    Limit,
    UnsupportedVersion,
    Unconfirmed,
    Rpc {
        method: String,
        code: Option<i64>,
        message: String,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => f.write_str(message),
            Self::Unavailable => f.write_str("Codex app-server is unavailable"),
            Self::Timeout => f.write_str("Codex app-server operation timed out"),
            Self::Protocol => f.write_str("Codex app-server returned an invalid response"),
            Self::Limit => f.write_str("Codex app-server operation exceeded its limit"),
            Self::UnsupportedVersion => {
                f.write_str("Codex app-server 0.166.0 or later is required")
            }
            Self::Unconfirmed => f.write_str("Codex thread cleanup could not be confirmed"),
            Self::Rpc { method, code, .. } => write!(f, "Codex rejected {method} (code {code:?})"),
        }
    }
}
impl std::error::Error for Error {}
impl Error {
    fn rpc_is(&self, method: &str, code: i64, message: &str) -> bool {
        matches!(self, Self::Rpc { method: actual, code: Some(actual_code), message: actual_message }
            if actual == method && *actual_code == code && actual_message == message)
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoint: PathBuf,
    pub managed: bool,
}
impl Config {
    pub fn load(paths: &tower_pal::Paths) -> Result<Self> {
        let raw = tower_pal::read_config(&paths.config_home.join("tower/config.toml"))
            .map_err(|_| Error::Config("Cannot read Tower configuration"))?;
        let table: toml::Table = raw
            .map(|s| s.parse())
            .transpose()
            .map_err(|_| Error::Config("Invalid Tower configuration"))?
            .unwrap_or_default();
        let Some(codex) = table.get("codex") else {
            return Ok(Self {
                endpoint: paths.run.join("codex.sock"),
                managed: true,
            });
        };
        let codex = codex
            .as_table()
            .ok_or(Error::Config("Codex configuration must be a table"))?;
        if codex.contains_key("default_mode") || codex.contains_key("override") {
            return Err(Error::Config(
                "Codex launch modes and directory overrides are no longer supported; configure codex.endpoint",
            ));
        }
        let Some(endpoint) = codex.get("endpoint") else {
            return Ok(Self {
                endpoint: paths.run.join("codex.sock"),
                managed: true,
            });
        };
        let endpoint = endpoint
            .as_str()
            .ok_or(Error::Config("Codex endpoint must be a Unix socket URL"))?;
        let path = endpoint
            .strip_prefix("unix://")
            .ok_or(Error::Config("Codex endpoint must use unix://"))?;
        if path.is_empty() || path.len() > 4096 || path.chars().any(char::is_control) {
            return Err(Error::Config(
                "Codex endpoint must name an absolute Unix socket path",
            ));
        }
        let endpoint = if let Some(suffix) = path.strip_prefix("~/") {
            paths.home.join(suffix)
        } else {
            PathBuf::from(path)
        };
        if !endpoint.is_absolute() {
            return Err(Error::Config(
                "Codex endpoint must be absolute or start with ~/",
            ));
        }
        Ok(Self {
            endpoint,
            managed: false,
        })
    }
}

pub struct Connection {
    pub socket: Socket,
    pub peer: tower_pal::Peer,
    pub process: Option<tower_pal::ProcessIdentity>,
}
pub async fn connect(path: &Path) -> Result<Connection> {
    if !path.is_absolute() {
        return Err(Error::Config("Codex socket path must be absolute"));
    }
    let resolved = tower_pal::require_private_socket(path).map_err(|_| Error::Unavailable)?;
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let stream = tower_pal::connect(&resolved)
            .await
            .map_err(|_| Error::Unavailable)?;
        let peer = tower_pal::peer(&stream).map_err(|_| Error::Unavailable)?;
        if peer.uid != tower_pal::uid() {
            return Err(Error::Unavailable);
        }
        let process = peer
            .pid
            .and_then(|pid| tower_pal::process_identity(pid).ok());
        let limits = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE))
            .max_frame_size(Some(MAX_MESSAGE))
            .max_write_buffer_size(MAX_MESSAGE + MAX_REQUEST);
        let (socket, _) =
            tokio_tungstenite::client_async_with_config("ws://localhost/", stream, Some(limits))
                .await
                .map_err(|_| Error::Unavailable)?;
        Ok(Connection {
            socket,
            peer,
            process,
        })
    })
    .await
    .map_err(|_| Error::Timeout)?
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadStatus {
    Idle,
    Active,
    NotLoaded,
    SystemError,
}
pub struct Client {
    connection: Connection,
    next_id: u64,
    pending: bool,
}
impl Client {
    pub async fn connect(path: &Path) -> Result<Self> {
        let mut client = Self {
            connection: connect(path).await?,
            next_id: 0,
            pending: false,
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "clientInfo": {"name":"tower_maintenance", "version":env!("CARGO_PKG_VERSION")},
                    "capabilities":{"experimentalApi":true}
                }),
            )
            .await?;
        if !initialized["userAgent"]
            .as_str()
            .is_some_and(supported_version)
        {
            return Err(Error::UnsupportedVersion);
        }
        client.pending = true;
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            client.connection.socket.send(Message::Text(
                json!({"method":"initialized"}).to_string().into(),
            )),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Unavailable)?;
        client.pending = false;
        Ok(client)
    }
    pub fn process(&self) -> Option<tower_pal::ProcessIdentity> {
        self.connection.process
    }
    pub fn peer(&self) -> tower_pal::Peer {
        self.connection.peer
    }

    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        if self.pending {
            return Err(Error::Unavailable);
        }
        if method.is_empty()
            || method.len() > 128
            || !method
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
        {
            return Err(Error::Protocol);
        }
        self.next_id = self.next_id.checked_add(1).ok_or(Error::Limit)?;
        let id = format!("tower-maintenance:{}", self.next_id);
        let text = json!({"id":id,"method":method,"params":params}).to_string();
        if text.len() > MAX_REQUEST {
            return Err(Error::Limit);
        }
        // Cancellation can leave an admitted mutation without its reply. Never
        // reuse this connection after a cancelled, partial or timed-out request.
        self.pending = true;
        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.exchange(method, &id, text)).await;
        match result {
            Ok(Ok(value)) => {
                self.pending = false;
                Ok(value)
            }
            Ok(Err(error @ Error::Rpc { .. })) => {
                self.pending = false;
                Err(error)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Timeout),
        }
    }
    async fn exchange(&mut self, method: &str, id: &str, text: String) -> Result<Value> {
        self.connection
            .socket
            .send(Message::Text(text.into()))
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut bytes = 0usize;
        for _ in 0..1024 {
            let frame = self
                .connection
                .socket
                .next()
                .await
                .ok_or(Error::Unavailable)?
                .map_err(|_| Error::Unavailable)?;
            bytes = bytes.checked_add(frame.len()).ok_or(Error::Limit)?;
            if bytes > MAX_EXCHANGE {
                return Err(Error::Limit);
            }
            match frame {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).map_err(|_| Error::Protocol)?;
                    if value.get("method").is_some() {
                        // Maintenance has no thread subscription. It must never
                        // race a session owner by answering an approval request.
                        continue;
                    }
                    if value.get("id") != Some(&Value::String(id.into())) {
                        continue;
                    }
                    if let Some(error) = value.get("error") {
                        return Err(Error::Rpc {
                            method: method.into(),
                            code: error["code"].as_i64(),
                            message: error["message"]
                                .as_str()
                                .unwrap_or("")
                                .chars()
                                .take(1024)
                                .filter(|c| !c.is_control())
                                .collect(),
                        });
                    }
                    return value.get("result").cloned().ok_or(Error::Protocol);
                }
                Message::Ping(_) => self
                    .connection
                    .socket
                    .flush()
                    .await
                    .map_err(|_| Error::Unavailable)?,
                Message::Pong(_) => {}
                Message::Close(_) => return Err(Error::Unavailable),
                _ => return Err(Error::Protocol),
            }
        }
        Err(Error::Limit)
    }
    pub async fn loaded_threads(&mut self) -> Result<BTreeSet<String>> {
        tokio::time::timeout(CLEANUP_TIMEOUT, self.loaded_threads_inner())
            .await
            .map_err(|_| Error::Timeout)?
    }
    async fn loaded_threads_inner(&mut self) -> Result<BTreeSet<String>> {
        let mut result = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let page = self
                .request("thread/loaded/list", json!({"cursor":cursor,"limit":100}))
                .await?;
            for id in array(&page, "data")? {
                let id = identifier(id)?;
                if !result.insert(id.into()) || result.len() > MAX_ENTRIES {
                    return Err(Error::Limit);
                }
            }
            match page.get("nextCursor") {
                None | Some(Value::Null) => return Ok(result),
                Some(value) => {
                    let next = cursor_value(value)?;
                    if !cursors.insert(next.to_owned()) {
                        return Err(Error::Protocol);
                    }
                    cursor = Some(next.into());
                }
            }
        }
        Err(Error::Limit)
    }
    pub async fn thread_status(&mut self, thread: &str) -> Result<ThreadStatus> {
        check_identifier(thread)?;
        let value = match self
            .request(
                "thread/read",
                json!({"threadId":thread,"includeTurns":false}),
            )
            .await
        {
            Err(error)
                if error.rpc_is(
                    "thread/read",
                    -32600,
                    &format!("thread not loaded: {thread}"),
                ) =>
            {
                return Ok(ThreadStatus::NotLoaded);
            }
            result => result?,
        };
        if value["thread"]["id"] != thread {
            return Err(Error::Protocol);
        }
        match value["thread"]["status"]["type"].as_str() {
            Some("idle") => Ok(ThreadStatus::Idle),
            Some("active") => Ok(ThreadStatus::Active),
            Some("notLoaded") => Ok(ThreadStatus::NotLoaded),
            Some("systemError") => Ok(ThreadStatus::SystemError),
            _ => Err(Error::Protocol),
        }
    }
    pub async fn stop_thread(&mut self, thread: &str, known_turn: Option<&str>) -> Result<()> {
        check_identifier(thread)?;
        if let Some(turn) = known_turn {
            check_identifier(turn)?;
        }
        tokio::time::timeout(CLEANUP_TIMEOUT, self.stop_thread_inner(thread, known_turn))
            .await
            .map_err(|_| Error::Timeout)?
    }
    async fn stop_thread_inner(&mut self, thread: &str, known_turn: Option<&str>) -> Result<()> {
        if self.thread_status(thread).await? == ThreadStatus::NotLoaded {
            return Ok(());
        }
        let goal = self.pause_goal(thread).await;
        let turn = self.current_turn(thread).await;
        let interrupted = match &turn {
            Ok(Some(turn)) => self
                .request("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
                .await
                .map(|_| ()),
            Ok(None) => Ok(()),
            Err(_) => match known_turn {
                Some(turn) => self
                    .request("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
                    .await
                    .map(|_| ()),
                None => Ok(()),
            },
        };
        let terminals = self.terminate_background(thread).await;
        goal?;
        turn?;
        interrupted?;
        terminals?;
        // Adapted from Captain Miao's checked cleanup constraint: thread/read
        // orders earlier per-thread mutations, including post-response effects.
        match self.thread_status(thread).await? {
            ThreadStatus::NotLoaded => Ok(()),
            ThreadStatus::Idle => {
                let page = self
                    .request(
                        "thread/backgroundTerminals/list",
                        json!({"threadId":thread,"limit":100}),
                    )
                    .await?;
                if array(&page, "data")?.is_empty()
                    && page.get("nextCursor").is_none_or(Value::is_null)
                {
                    Ok(())
                } else {
                    Err(Error::Unconfirmed)
                }
            }
            _ => Err(Error::Unconfirmed),
        }
    }
    async fn pause_goal(&mut self, thread: &str) -> Result<()> {
        let goal = match self
            .request("thread/goal/get", json!({"threadId":thread}))
            .await
        {
            Err(error) if error.rpc_is("thread/goal/get", -32600, "goals feature is disabled") => {
                return Ok(());
            }
            result => result?,
        };
        if goal["goal"].is_null() {
            return Ok(());
        }
        if goal["goal"]["threadId"] != thread {
            return Err(Error::Protocol);
        }
        match goal["goal"]["status"].as_str() {
            Some("active") => {
                let paused = self
                    .request(
                        "thread/goal/set",
                        json!({"threadId":thread,"status":"paused"}),
                    )
                    .await?;
                if paused["goal"]["threadId"] != thread || paused["goal"]["status"] != "paused" {
                    return Err(Error::Unconfirmed);
                }
            }
            Some("paused" | "blocked" | "usageLimited" | "budgetLimited" | "complete") => {}
            _ => return Err(Error::Protocol),
        }
        Ok(())
    }
    async fn current_turn(&mut self, thread: &str) -> Result<Option<String>> {
        let value = match self.request("thread/turns/list", json!({"threadId":thread,"limit":1,"sortDirection":"desc"})).await {
            Err(error) if error.rpc_is("thread/turns/list", -32600, &format!("thread {thread} is not materialized yet; thread/turns/list is unavailable before first user message")) => return Ok(None),
            result => result?,
        };
        for turn in array(&value, "data")? {
            match turn["status"].as_str() {
                Some("inProgress") => return Ok(Some(identifier(&turn["id"])?.into())),
                Some("completed" | "interrupted" | "failed") => {}
                _ => return Err(Error::Protocol),
            }
        }
        Ok(None)
    }
    async fn terminate_background(&mut self, thread: &str) -> Result<()> {
        let mut terminated = BTreeSet::new();
        for _ in 0..MAX_PAGES {
            let page = self
                .request(
                    "thread/backgroundTerminals/list",
                    json!({"threadId":thread,"limit":100}),
                )
                .await?;
            let entries = array(&page, "data")?;
            if entries.is_empty() {
                return if page.get("nextCursor").is_none_or(Value::is_null) {
                    Ok(())
                } else {
                    Err(Error::Protocol)
                };
            }
            for process in entries {
                let id = identifier(&process["processId"])?;
                if !terminated.insert(id.to_owned()) || terminated.len() > MAX_ENTRIES {
                    return Err(Error::Unconfirmed);
                }
                let reply = self
                    .request(
                        "thread/backgroundTerminals/terminate",
                        json!({"threadId":thread,"processId":id}),
                    )
                    .await?;
                if reply["terminated"] != true {
                    return Err(Error::Unconfirmed);
                }
            }
            // Removal changes pagination offsets. Always fetch the first page.
        }
        Err(Error::Limit)
    }
    pub async fn wait_unloaded(&mut self, thread: &str, timeout: Duration) -> Result<()> {
        check_identifier(thread)?;
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(Error::Limit);
        }
        tokio::time::timeout(timeout, async {
            for _ in 0..600 {
                if self.thread_status(thread).await? == ThreadStatus::NotLoaded {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(Error::Limit)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}
fn check_identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 160 || value.chars().any(char::is_control) {
        Err(Error::Protocol)
    } else {
        Ok(())
    }
}
fn identifier(value: &Value) -> Result<&str> {
    let value = value.as_str().ok_or(Error::Protocol)?;
    check_identifier(value)?;
    Ok(value)
}
fn cursor_value(value: &Value) -> Result<&str> {
    let value = value.as_str().ok_or(Error::Protocol)?;
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        Err(Error::Protocol)
    } else {
        Ok(value)
    }
}
fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    let result = value[key].as_array().ok_or(Error::Protocol)?;
    if result.len() > MAX_ENTRIES {
        return Err(Error::Limit);
    }
    Ok(result)
}
fn supported_version(agent: &str) -> bool {
    if agent.len() > 512 {
        return false;
    }
    let Some(version) = agent
        .split_whitespace()
        .next()
        .and_then(|v| v.rsplit_once('/'))
        .map(|(_, v)| v)
    else {
        return false;
    };
    let version = version.split('+').next().unwrap_or(version);
    let (version, prerelease) = version
        .split_once('-')
        .map_or((version, false), |(v, _)| (v, true));
    let values = version
        .split('.')
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>();
    let Ok(values) = values else { return false };
    let [major, minor, patch] = values.as_slice() else {
        return false;
    };
    (*major, *minor, *patch) > (0, 166, 0) || (*major, *minor, *patch) == (0, 166, 0) && !prerelease
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Server {
        _directory: tempfile::TempDir,
        path: PathBuf,
        task: tokio::task::JoinHandle<()>,
        calls: Arc<Mutex<Vec<Value>>>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn fake_server(mut answer: impl FnMut(&Value) -> Value + Send + 'static) -> Server {
        let directory = tempfile::Builder::new()
            .prefix("tower-codex-")
            .tempdir_in("/tmp")
            .unwrap();
        tower_pal::private_dir(directory.path()).unwrap();
        let path = directory.path().join("server.sock");
        let listener = tower_pal::IpcListener::bind(&path).await.unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let value: Value = serde_json::from_str(&text).unwrap();
                recorded.lock().unwrap().push(value.clone());
                if value["method"] == "initialized" {
                    continue;
                }
                let result = if value["method"] == "initialize" {
                    json!({"result":{"userAgent":"codex-app-server/0.166.0 (fixture)"}})
                } else {
                    answer(&value)
                };
                if result["_stall"] == true {
                    std::future::pending::<()>().await;
                }
                // An unrelated request must never acquire a decision from a
                // maintenance connection racing the real session owner.
                if result["_foreign"] == true {
                    socket.send(Message::Text(json!({"id":"approval-from-other-thread","method":"item/commandExecution/requestApproval","params":{"threadId":"other","turnId":"other-turn"}}).to_string().into())).await.unwrap();
                }
                let mut result = result.as_object().unwrap().clone();
                result.remove("_stall");
                result.remove("_foreign");
                result.insert("id".into(), value["id"].clone());
                if socket
                    .send(Message::Text(Value::Object(result).to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Server {
            _directory: directory,
            path,
            task,
            calls,
        }
    }
    fn paths(root: &Path) -> tower_pal::Paths {
        let state = root.join("state");
        tower_pal::Paths {
            home: root.join("home"),
            config_home: root.join("config"),
            run: state.join("run"),
            socket: state.join("run/tower.sock"),
            database: state.join("tower.db"),
            tokens: state.join("tokens"),
            logs: state.join("logs"),
            state,
        }
    }
    #[test]
    fn endpoint_configuration_has_one_host_owned_default() {
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(directory.path());
        let default = Config::load(&paths).unwrap();
        assert!(default.managed);
        assert_eq!(default.endpoint, paths.run.join("codex.sock"));
        tower_pal::private_dir(&paths.config_home.join("tower")).unwrap();
        let config = paths.config_home.join("tower/config.toml");
        tower_pal::write_private(&config, b"[codex]\nendpoint='unix://~/service.sock'\n").unwrap();
        let selected = Config::load(&paths).unwrap();
        assert!(!selected.managed);
        assert_eq!(selected.endpoint, paths.home.join("service.sock"));
        for invalid in [
            "unix://",
            "unix://relative/socket",
            "ws://127.0.0.1:8000",
            "/absolute/socket",
            "unix://~other/socket",
        ] {
            tower_pal::write_private(
                &config,
                format!("[codex]\nendpoint={invalid:?}\n").as_bytes(),
            )
            .unwrap();
            assert!(
                matches!(Config::load(&paths), Err(Error::Config(_))),
                "{invalid}"
            );
        }
        tower_pal::write_private(&config, b"[codex]\nendpoint=42\n").unwrap();
        assert!(Config::load(&paths).is_err());
        for legacy in [
            "[codex]\ndefault_mode='native'\n",
            "[codex]\ndefault_mode='app-server'\n",
            "[codex]\noverride=[]\n",
        ] {
            tower_pal::write_private(&config, legacy.as_bytes()).unwrap();
            assert!(matches!(Config::load(&paths), Err(Error::Config(_))));
        }
    }
    #[test]
    fn ordered_cleanup_requires_a_known_supported_version() {
        for (version, supported) in [
            ("codex-app-server/0.166.0", true),
            ("tower/0.166.0+build.7 (Linux)", true),
            ("tower/0.166.0-alpha.1", false),
            ("tower/0.167.0-alpha.1", true),
            ("tower/1.0.0", true),
            ("tower/0.165.9", false),
            ("tower/0.166", false),
            ("unknown", false),
            ("tower/0.166.0.1", false),
        ] {
            assert_eq!(supported_version(version), supported, "{version}");
        }
        let error = Error::Rpc {
            method: "thread/read".into(),
            code: Some(-32600),
            message: "PRIVATE-ERROR-MARKER".into(),
        };
        assert!(!error.to_string().contains("PRIVATE-ERROR-MARKER"));
    }
    #[tokio::test]
    async fn cleanup_pauses_interrupts_terminates_and_checks_without_touching_other_threads() {
        let mut reads = 0;
        let mut terminals = 0;
        let server = fake_server(move |request| {
            assert_eq!(request["params"]["threadId"], "owned-thread");
            match request["method"].as_str().unwrap() {
                "thread/read" => { reads += 1; json!({"result":{"thread":{"id":"owned-thread","status":{"type":if reads==1{"active"}else{"idle"}}}}}) }
                "thread/goal/get" => json!({"result":{"goal":{"threadId":"owned-thread","status":"active"}}}),
                "thread/goal/set" => { assert_eq!(request["params"]["status"],"paused"); json!({"result":{"goal":{"threadId":"owned-thread","status":"paused"}}}) },
                "thread/turns/list" => json!({"result":{"data":[{"id":"active-turn","status":"inProgress"}]}}),
                "turn/interrupt" => { assert_eq!(request["params"]["turnId"],"active-turn"); json!({"result":{}}) },
                "thread/backgroundTerminals/list" => { terminals += 1; json!({"result":{"data":if terminals==1{json!([{"processId":"process-1"}])}else{json!([])},"nextCursor":null}}) },
                "thread/backgroundTerminals/terminate" => { assert_eq!(request["params"]["processId"],"process-1"); json!({"result":{"terminated":true}}) },
                method => panic!("unexpected {method}"),
            }
        }).await;
        let mut client = Client::connect(&server.path).await.unwrap();
        let process = client.process();
        assert_eq!(client.peer().uid, tower_pal::uid());
        client
            .stop_thread("owned-thread", Some("old-turn"))
            .await
            .unwrap();
        assert_eq!(client.process(), process);
        let methods: Vec<_> = server
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|r| r["method"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            methods,
            vec![
                "initialize",
                "initialized",
                "thread/read",
                "thread/goal/get",
                "thread/goal/set",
                "thread/turns/list",
                "turn/interrupt",
                "thread/backgroundTerminals/list",
                "thread/backgroundTerminals/terminate",
                "thread/backgroundTerminals/list",
                "thread/read",
                "thread/backgroundTerminals/list"
            ]
        );
    }
    #[tokio::test]
    async fn incomplete_or_unsupported_cleanup_never_reports_success() {
        for failure in ["goal", "terminate", "active", "missing-terminal-proof"] {
            let mut reads = 0;
            let server=fake_server(move |request| match request["method"].as_str().unwrap() {
                "thread/read"=>{reads+=1;json!({"result":{"thread":{"id":"owned","status":{"type":if failure=="active" && reads>1{"active"}else{"idle"}}}}})},
                "thread/goal/get" if failure=="goal"=>json!({"error":{"code":-32601,"message":"unsupported"}}),
                "thread/goal/get"=>json!({"result":{"goal":null}}),
                "thread/turns/list"=>json!({"result":{"data":[]}}),
                "thread/backgroundTerminals/list" if failure=="terminate"=>json!({"result":{"data":[{"processId":"p"}],"nextCursor":null}}),
                "thread/backgroundTerminals/list" if failure=="missing-terminal-proof"=>json!({"result":{"data":[],"nextCursor":"unread-page"}}),
                "thread/backgroundTerminals/list"=>json!({"result":{"data":[],"nextCursor":null}}),
                "thread/backgroundTerminals/terminate"=>json!({"result":{"terminated":false}}),
                method=>panic!("unexpected {method}"),
            }).await;
            let mut client = Client::connect(&server.path).await.unwrap();
            assert!(
                client.stop_thread("owned", None).await.is_err(),
                "{failure}"
            );
        }
    }
    #[tokio::test]
    async fn absence_requires_exact_thread_evidence_and_never_an_archive() {
        let server=fake_server(|request|json!({"error":{"code":-32600,"message":format!("thread not loaded: {}",request["params"]["threadId"].as_str().unwrap())}})).await;
        let mut client = Client::connect(&server.path).await.unwrap();
        client.stop_thread("owned", None).await.unwrap();
        client
            .wait_unloaded("owned", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(server.calls.lock().unwrap().iter().all(|r| matches!(
            r["method"].as_str(),
            Some("initialize" | "initialized" | "thread/read")
        )));
        let server = fake_server(
            |_| json!({"error":{"code":-32600,"message":"thread not loaded: another"}}),
        )
        .await;
        let mut client = Client::connect(&server.path).await.unwrap();
        assert!(client.stop_thread("owned", None).await.is_err());
    }
    #[tokio::test]
    async fn pagination_and_unrelated_server_requests_are_bounded_and_do_not_grant_approval() {
        let mut pages = 0;
        let server=fake_server(move |_|{pages+=1;json!({"_foreign":true,"result":{"data":[format!("thread-{pages}")],"nextCursor":if pages==1{json!("next")}else{Value::Null}}})}).await;
        let mut client = Client::connect(&server.path).await.unwrap();
        assert_eq!(
            client.loaded_threads().await.unwrap(),
            BTreeSet::from(["thread-1".into(), "thread-2".into()])
        );
        assert!(
            server
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|request| request["id"] != "approval-from-other-thread")
        );
        let server = fake_server(|_| json!({"result":{"data":[],"nextCursor":"same"}})).await;
        let mut client = Client::connect(&server.path).await.unwrap();
        assert!(matches!(
            client.loaded_threads().await,
            Err(Error::Protocol)
        ));
    }
    #[tokio::test]
    async fn cancelled_requests_poison_the_connection_and_cannot_be_replayed() {
        let server = fake_server(|_| json!({"_stall":true})).await;
        let mut client = Client::connect(&server.path).await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                client.request("thread/read", json!({"threadId":"owned"}))
            )
            .await
            .is_err()
        );
        assert!(matches!(
            client
                .request("thread/read", json!({"threadId":"owned"}))
                .await,
            Err(Error::Unavailable)
        ));
        assert_eq!(
            server
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|v| v["method"] == "thread/read")
                .count(),
            1
        );
    }
    #[tokio::test]
    #[ignore = "requires installed Codex 0.166+; no model/provider calls"]
    async fn real_codex_maintenance_stops_one_thread_and_waits_for_unload() {
        let directory = tempfile::Builder::new()
            .prefix("tower-codex-live-")
            .tempdir_in("/tmp")
            .unwrap();
        let root = directory.path();
        for name in ["home", "codex", "project"] {
            tower_pal::private_dir(&root.join(name)).unwrap();
        }
        tower_pal::write_private(
            &root.join("codex/config.toml"),
            br#"model="fixture-model"
model_provider="fixture"
approval_policy="never"
sandbox_mode="read-only"
check_for_update_on_startup=false
[model_providers.fixture]
name="Local no-inference fixture"
base_url="http://127.0.0.1:9/v1"
wire_api="responses"
request_max_retries=0
stream_max_retries=0
"#,
        )
        .unwrap();
        let env = std::collections::BTreeMap::from([
            ("PATH".into(), std::env::var("PATH").unwrap()),
            ("HOME".into(), root.join("home").display().to_string()),
            (
                "CODEX_HOME".into(),
                root.join("codex").display().to_string(),
            ),
        ]);
        let path = root.join("server.sock");
        let mut process = tower_pal::spawn_protocol(
            &[
                "codex".into(),
                "app-server".into(),
                "--listen".into(),
                format!("unix://{}", path.display()),
            ],
            &env,
            root,
        )
        .unwrap();
        struct Group(tower_pal::ProcessIdentity);
        impl Drop for Group {
            fn drop(&mut self) {
                let _ = tower_pal::kill_protocol_group(self.0);
            }
        }
        let group = Group(tower_pal::process_identity(process.id().unwrap()).unwrap());
        tokio::time::timeout(Duration::from_secs(10), async {
            while !path.exists() {
                assert!(process.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut first = Client::connect(&path).await.unwrap();
        let mut second = Client::connect(&path).await.unwrap();
        let start = json!({"cwd":root.join("project"),"config":{"thread_unload_delay_secs":0}});
        let a = first.request("thread/start", start.clone()).await.unwrap()["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let b = second.request("thread/start", start).await.unwrap()["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut maintenance = Client::connect(&path).await.unwrap();
        maintenance.stop_thread(&a, None).await.unwrap();
        assert_eq!(
            maintenance.thread_status(&b).await.unwrap(),
            ThreadStatus::Idle
        );
        assert!(matches!(
            maintenance
                .wait_unloaded(&a, Duration::from_millis(50))
                .await,
            Err(Error::Timeout)
        ));
        let mut maintenance = Client::connect(&path).await.unwrap();
        first
            .request("thread/unsubscribe", json!({"threadId":a}))
            .await
            .unwrap();
        maintenance
            .wait_unloaded(&a, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            maintenance.loaded_threads().await.unwrap(),
            BTreeSet::from([b])
        );
        let _ = tower_pal::kill_protocol_group(group.0);
        process.wait().await.unwrap();
    }
}
