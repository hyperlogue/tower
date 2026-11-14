use std::time::Duration;
use tower_agents::runtime::RuntimeState;
use tower_pool_proto::{InputProof, Reply, Request};
use tower_protocol::{DeliveryReceipt, RpcError, Value};

fn error(code: &str, message: &str) -> RpcError {
    RpcError::application(code, message)
}
async fn connect(paths: &tower_pal::Paths) -> tower_sessions::Result<tower_pal::IpcStream> {
    let mut stream = tower_pal::connect(&paths.pool_socket())
        .await
        .map_err(|_| error("delivery_failed", "Pool is unavailable"))?;
    tower_pool_proto::write(
        &mut stream,
        &Request::Hello {
            version: tower_pool_proto::VERSION,
        },
    )
    .await
    .map_err(|_| error("delivery_failed", "Pool handshake failed"))?;
    let reply = tower_pool_proto::read(&mut stream)
        .await
        .and_then(|f| f.json::<Reply>())
        .map_err(|_| error("delivery_failed", "Pool handshake failed"))?;
    if !matches!(reply,Reply::Hello{version,capabilities} if version==tower_pool_proto::VERSION && capabilities.iter().any(|c|c=="inject_if_idle.v1"))
    {
        return Err(error(
            "unsupported",
            "Pool does not support guarded input injection",
        ));
    }
    Ok(stream)
}
pub async fn supported(paths: &tower_pal::Paths) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(1), connect(paths)).await,
        Ok(Ok(_))
    )
}
pub fn observe_idle(
    state: &mut RuntimeState,
    kind: tower_agents::events::EventKind,
    was_idle: bool,
) {
    if was_idle && state.private.contains_key("input_ready_at") {
        return;
    }
    if state.status == "idle"
        && matches!(
            kind,
            tower_agents::events::EventKind::Ready | tower_agents::events::EventKind::Idle
        )
    {
        state
            .private
            .insert("input_ready_at".into(), tower_pal::monotonic_ms().into());
    }
}
pub fn proof(state: &RuntimeState) -> tower_sessions::Result<InputProof> {
    if state.status != "idle" || state.stop_requested {
        return Err(busy("target_busy_pty"));
    }
    let idle_since = state
        .private
        .get("input_ready_at")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| busy("stale_input_evidence"))?;
    Ok(InputProof {
        pid: state.launcher_pid,
        pid_start_time: state.launcher_start_time,
        idle_since,
        checked_at: tower_pal::monotonic_ms(),
    })
}
fn busy(reason: &str) -> RpcError {
    let mut e = error(
        "delivery_failed",
        "Target cannot accept a safe PTY submission",
    );
    e.data
        .details
        .insert("reason".into(), Value::String(reason.into()));
    e
}
pub async fn send(
    paths: tower_pal::Paths,
    name: String,
    proof: InputProof,
    text: String,
) -> tower_sessions::Result<DeliveryReceipt> {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = connect(&paths).await?;
        tower_pool_proto::write(&mut stream, &Request::Inject { name, proof, text })
            .await
            .map_err(|_| {
                error(
                    "delivery_unknown",
                    "Pool injection write failed; do not automatically retry",
                )
            })?;
        match tower_pool_proto::read(&mut stream)
            .await
            .and_then(|f| f.json::<Reply>())
            .map_err(|_| {
                error(
                    "delivery_unknown",
                    "Pool injection response was lost; do not automatically retry",
                )
            })? {
            Reply::Ok => Ok(DeliveryReceipt {
                method: "pty".into(),
                acknowledgement: "pty_submission".into(),
            }),
            Reply::Error { code, .. } if code == "delivery_unknown" => Err(error(
                "delivery_unknown",
                "PTY submission outcome is unknown; do not automatically retry",
            )),
            Reply::Error { code, .. } => Err(busy(&code)),
            _ => Err(error(
                "delivery_unknown",
                "Unexpected pool response; do not automatically retry",
            )),
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        Err(error(
            "delivery_unknown",
            "PTY submission timed out; do not automatically retry",
        ))
    })
}
