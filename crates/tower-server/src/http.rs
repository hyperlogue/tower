use crate::{Result, worker::ServiceWorker};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tower_protocol::{
    CallerContext, Request, RequestId, ResponseBody, Secret, SubscribeResult, Value,
};

#[derive(Clone, Deserialize)]
#[serde(default)]
struct Config {
    enabled: bool,
    port: u16,
    allowed_origins: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 7878,
            allowed_origins: Vec::new(),
        }
    }
}
#[derive(Clone)]
struct App {
    worker: ServiceWorker,
    config: Config,
    permits: Arc<tokio::sync::Semaphore>,
    stop: tokio::sync::watch::Receiver<bool>,
}
pub(super) struct Runtime {
    stop: tokio::sync::watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Runtime {
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        for mut task in self.tasks {
            if tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
            {
                task.abort();
            }
        }
    }
}
async fn cancel_on_shutdown(
    State(app): State<App>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let mut stop = app.stop.clone();
    if *stop.borrow() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    tokio::select! {response=next.run(request)=>response,_=stop.changed()=>StatusCode::SERVICE_UNAVAILABLE.into_response()}
}
fn valid_origin(origin: &str) -> bool {
    reqwest::Url::parse(origin).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/"
            && url.origin().ascii_serialization() == origin
    })
}
async fn origin_gate(
    State(app): State<App>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(authority) = request.uri().authority().map(|a| a.as_str().to_owned()) {
        if request
            .headers()
            .get("host")
            .is_some_and(|h| h.as_bytes() != authority.as_bytes())
        {
            return StatusCode::FORBIDDEN.into_response();
        }
        if !request.headers().contains_key("host") {
            let Ok(value) = HeaderValue::from_str(&authority) else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            request.headers_mut().insert("host", value);
        }
    }
    let origin = match check_origin(request.headers(), &app.config) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    let preflight = request.method() == Method::OPTIONS;
    if preflight {
        let method = request
            .headers()
            .get("access-control-request-method")
            .and_then(|h| h.to_str().ok());
        let headers = request
            .headers()
            .get("access-control-request-headers")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if origin.is_none()
            || !matches!(method, Some("POST" | "GET"))
            || headers
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .any(|s| {
                    !matches!(
                        s.trim().to_ascii_lowercase().as_str(),
                        "authorization" | "content-type"
                    )
                })
        {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let mut response = if preflight {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };
    if let Some(origin) = origin {
        if let Ok(value) = HeaderValue::from_str(&origin) {
            response
                .headers_mut()
                .insert("access-control-allow-origin", value);
        }
        response
            .headers_mut()
            .insert("vary", HeaderValue::from_static("Origin"));
        if preflight {
            response.headers_mut().insert(
                "access-control-allow-methods",
                HeaderValue::from_static("GET, POST"),
            );
            response.headers_mut().insert(
                "access-control-allow-headers",
                HeaderValue::from_static("Authorization, Content-Type"),
            );
            response
                .headers_mut()
                .insert("access-control-max-age", HeaderValue::from_static("300"));
        }
    }
    response
}
pub(super) async fn start(paths: &tower_pal::Paths, worker: ServiceWorker) -> Result<Runtime> {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Root {
        http: Config,
    }
    let root: Root = tower_pal::read_config(&paths.config_home.join("tower/config.toml"))?
        .map(|s| toml::from_str(&s))
        .transpose()
        .map_err(|_| crate::Error::Other("Invalid HTTP configuration".into()))?
        .unwrap_or_default();
    let (stop, receiver) = tokio::sync::watch::channel(false);
    if !root.http.enabled {
        return Ok(Runtime {
            stop,
            tasks: Vec::new(),
        });
    }
    if root.http.port == 0 {
        return Err(crate::Error::Other("HTTP port must be nonzero".into()));
    }
    if root.http.allowed_origins.iter().any(|s| !valid_origin(s)) {
        return Err(crate::Error::Other(
            "HTTP allowed_origins must contain exact HTTP(S) origins".into(),
        ));
    }
    let listeners = tower_pal::loopback_listeners(root.http.port).await?;
    let app = App {
        worker,
        config: root.http,
        permits: Arc::new(tokio::sync::Semaphore::new(128)),
        stop: receiver.clone(),
    };
    let router = Router::new()
        .route("/rpc", post(rpc))
        .route("/events", get(events))
        .layer(DefaultBodyLimit::max(tower_protocol::MAX_FRAME_BYTES))
        .layer(middleware::from_fn_with_state(
            app.clone(),
            cancel_on_shutdown,
        ))
        .layer(middleware::from_fn_with_state(app.clone(), origin_gate))
        .with_state(app);
    let connections = Arc::new(tokio::sync::Semaphore::new(128));
    let tasks = listeners
        .into_iter()
        .map(|listener| {
            let listener = crate::http_io::Limited {
                listener,
                permits: connections.clone(),
            };
            let router = router.clone();
            let mut receiver = receiver.clone();
            tokio::spawn(async move {
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        let _ = receiver.changed().await;
                    })
                    .await;
            })
        })
        .collect();
    Ok(Runtime { stop, tasks })
}
fn check_origin(
    headers: &HeaderMap,
    config: &Config,
) -> std::result::Result<Option<String>, StatusCode> {
    let authority = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    if headers.get_all("host").iter().count() != 1
        || ![
            format!("127.0.0.1:{}", config.port),
            format!("[::1]:{}", config.port),
            format!("localhost:{}", config.port),
        ]
        .contains(&authority.to_owned())
    {
        return Err(StatusCode::FORBIDDEN);
    }
    if let Some(origin) = headers.get("origin") {
        let origin = origin.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
        if !valid_origin(origin)
            || headers.get_all("origin").iter().count() != 1
            || (origin != format!("http://{authority}")
                && !config.allowed_origins.iter().any(|s| s == origin))
        {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    Ok(headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned))
}
fn context(headers: &HeaderMap, config: &Config) -> std::result::Result<CallerContext, StatusCode> {
    check_origin(headers, config)?;
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if headers.get_all("authorization").iter().count() != 1 {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(CallerContext {
        bearer_token: Some(Secret(token.into())),
        ..Default::default()
    })
}
fn peer() -> tower_pal::Peer {
    tower_pal::Peer {
        uid: tower_pal::uid(),
        pid: None,
    }
}
async fn rpc(
    State(app): State<App>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let _permit = match app.permits.try_acquire() {
        Ok(p) => p,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let context = match context(&headers, &app.config) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    let body = match tokio::time::timeout(
        Duration::from_secs(10),
        axum::body::to_bytes(request.into_body(), tower_protocol::MAX_FRAME_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let mut request: Request = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if request.jsonrpc != "2.0" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    request.context = context;
    if matches!(
        request.method.as_str(),
        "subscribe" | "unsubscribe" | "daemon.shutdown"
    ) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let id = request.id.clone();
    let result = app.worker.request(request, peer()).await;
    let status = if result
        .as_ref()
        .is_err_and(|e| e.data.code == "unauthorized")
    {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::OK
    };
    let Some(id) = id else {
        return if status == StatusCode::UNAUTHORIZED {
            status
        } else {
            StatusCode::NO_CONTENT
        }
        .into_response();
    };
    (
        status,
        Json(tower_protocol::Response {
            jsonrpc: "2.0".into(),
            id,
            body: match result {
                Ok(result) => ResponseBody::Success { result },
                Err(error) => ResponseBody::Failure { error },
            },
        }),
    )
        .into_response()
}
#[derive(Deserialize)]
struct Topics {
    topics: Option<String>,
    approval_id: Option<String>,
}
async fn events(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<Topics>,
) -> Response {
    let permit = match app.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let context = match context(&headers, &app.config) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    let topics: Vec<String> = query
        .topics
        .unwrap_or_else(|| {
            if query.approval_id.is_some() {
                "approvals".into()
            } else {
                "agents,sessions,messages,approvals,sync".into()
            }
        })
        .split(',')
        .map(str::to_owned)
        .collect();
    let params = serde_json::from_value(
        serde_json::json!({"topics":topics,"approval_id":query.approval_id}),
    )
    .unwrap_or(Value::Null);
    let request = Request {
        jsonrpc: "2.0".into(),
        id: Some(RequestId::Null),
        method: "subscribe".into(),
        params,
        context: context.clone(),
    };
    let mut events = app.worker.events();
    let snapshot = match app.worker.request(request, peer()).await {
        Ok(v) => v,
        Err(e) => {
            return (
                if e.data.code == "unauthorized" {
                    StatusCode::UNAUTHORIZED
                } else {
                    StatusCode::FORBIDDEN
                },
                Json(e),
            )
                .into_response();
        }
    };
    let state: SubscribeResult =
        match serde_json::to_value(&snapshot).and_then(serde_json::from_value) {
            Ok(v) => v,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
    let stream = async_stream::stream! {
        let _permit=permit;
        let mut stop=app.stop.clone();
        yield Ok::<_,Infallible>(Event::default().event("snapshot").data(serde_json::to_string(&snapshot).unwrap_or_default()));
        let mut interval=tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                event=events.recv()=>{
                    let Ok(event)=event else{break};
                    if event.params.seq<=state.cursor || query.approval_id.as_ref().is_some_and(|id|id!=&event.params.id){continue;}
                    let topic=event.method.split('.').next().unwrap_or("");
                    let topic=match topic{"agent"=>"agents","session"=>"sessions","message"=>"messages","approval"=>"approvals","allgather"|"rules"|"sync"=>"sync",_=>continue};
                    if !topics.iter().any(|t|t==topic){continue;}
                    let ctx=context.clone();let check=event.clone();
                    match app.worker.execute(move |s|s.event_visible(&ctx,peer(),&check)).await {Ok(Ok(true))=>{},Ok(Ok(false))=>continue,_=>break}
                    yield Ok(Event::default().event(event.method.clone()).id(event.params.seq.to_string()).data(serde_json::to_string(&*event).unwrap_or_default()));
                }
                _=interval.tick()=>{
                    let ctx=context.clone();
                    if !matches!(app.worker.execute(move |s|s.check_subscription(&ctx,peer())).await,Ok(Ok(()))){break;}
                }
            }
        }
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
        .into_response()
}
