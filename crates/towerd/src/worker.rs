//! One blocking operation at a time; queued callers wait without occupying
//! Tokio workers. Serialization includes filesystem credential publication.
mod vcs;

use crate::{Error, Result, service::Service};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) struct ServiceWorker {
    service: Arc<Service>,
    gate: Arc<Semaphore>,
    inventory_gate: Arc<Semaphore>,
    delivery_gate: Arc<Semaphore>,
    enrollment_gate: Arc<Semaphore>,
}
impl ServiceWorker {
    pub(crate) fn vcs(&self) -> Arc<crate::vcs::Vcs> {
        self.service.vcs.clone()
    }
    pub(crate) fn clipboard(&self) -> Arc<tower_clipboard::Broker> {
        self.service.clipboard.clone()
    }
    pub fn events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<Arc<tower_protocol::EventNotification>> {
        self.service.events()
    }
    pub fn new(service: Service) -> Self {
        Self {
            service: Arc::new(service),
            gate: Arc::new(Semaphore::new(1)),
            inventory_gate: Arc::new(Semaphore::new(2)),
            delivery_gate: Arc::new(Semaphore::new(16)),
            enrollment_gate: Arc::new(Semaphore::new(4)),
        }
    }
    pub async fn execute<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Service) -> T + Send + 'static,
    ) -> Result<T> {
        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("service worker closed".into()))?;
        let service = Arc::clone(&self.service);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&service)
        })
        .await
        .map_err(|_| Error::Other("service worker failed".into()))
    }
    pub async fn request(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> std::result::Result<tower_protocol::Value, tower_protocol::RpcError> {
        let result = match request.method.as_str() {
            "clipboard.read" | "clipboard.fetch" | "clipboard.claim" | "clipboard.activate"
            | "clipboard.consumed" | "clipboard.wait" | "clipboard.finish" | "clipboard.status" => {
                self.clipboard_request(request, peer).await
            }
            method
                if method.starts_with("checkout.")
                    || method.starts_with("vcs.")
                    || method == "catalog.get" =>
            {
                self.vcs_request(request, peer).await
            }
            "session.open" => self.checkout_open(request, peer).await,
            "host.metrics" => self.host_metrics(request, peer).await,
            "message.send" => self.send_message(request, peer).await,
            "approval.watch" => self.approval_watch(request, peer).await,
            "conversation.list" => self.conversations(request, peer).await,
            "allgather.enrollment.begin" | "allgather.enrollment.poll" => {
                self.enroll(request, peer).await
            }
            _ => self.execute(move |s| s.dispatch(&request, peer)).await,
        };
        match result {
            Ok(r) => r,
            Err(Error::Rpc(e)) => Err(e),
            Err(_) => Err(tower_protocol::RpcError::application(
                "internal_error",
                "Request processing failed",
            )),
        }
    }
    async fn host_metrics(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let check = request.clone();
        self.execute(move |s| s.authorize_host_metrics(&check, peer))
            .await??;
        let sample = self.service.metrics.get().await?;
        self.execute(move |s| s.authorize_host_metrics(&request, peer))
            .await??;
        Ok(Ok(serde_json::from_value(serde_json::to_value(sample)?)?))
    }
    async fn clipboard_request(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let method = request.method.clone();
        let recheck = request.clone();
        let (p, session) = self
            .execute(move |s| s.clipboard_prepare(&request, peer))
            .await??;
        let broker = self.clipboard();
        let result: std::result::Result<tower_protocol::Value, tower_protocol::RpcError> = async {
            let v = match method.as_str() {
                "clipboard.read" => serde_json::to_value(
                    broker
                        .helper(
                            session.clone(),
                            p.format.unwrap_or(tower_protocol::ImageFormat::Png),
                        )
                        .await?,
                )
                .map_err(|_| {
                    tower_protocol::RpcError::application("internal_error", "Cannot encode image")
                })?,
                "clipboard.fetch" => {
                    let lease = p.lease.as_ref().ok_or_else(|| {
                        tower_protocol::RpcError::application(
                            "unauthorized",
                            "An attached client credential is required",
                        )
                    })?;
                    serde_json::to_value(
                        broker.fetch(session.clone(), Some(lease), p.format).await?,
                    )
                    .map_err(|_| {
                        tower_protocol::RpcError::application(
                            "internal_error",
                            "Cannot encode image",
                        )
                    })?
                }
                "clipboard.claim" => serde_json::to_value(broker.claim(&session, &p.paste_id)?)
                    .map_err(|_| {
                        tower_protocol::RpcError::application(
                            "internal_error",
                            "Cannot encode image",
                        )
                    })?,
                "clipboard.activate" => {
                    broker.activate(&session, &p.paste_id)?;
                    serde_json::Value::Null
                }
                "clipboard.consumed" => {
                    broker.consumed(&session, &p.paste_id)?;
                    serde_json::Value::Null
                }
                "clipboard.wait" => {
                    broker.wait(&session, &p.paste_id).await?;
                    serde_json::Value::Null
                }
                "clipboard.finish" => {
                    broker.finish(&session, &p.paste_id, p.success)?;
                    serde_json::Value::Null
                }
                "clipboard.status" => broker.status(&session)?,
                _ => unreachable!(),
            };
            self.execute(move |s| s.clipboard_prepare(&recheck, peer).map(|_| ()))
                .await
                .map_err(|_| {
                    tower_protocol::RpcError::application(
                        "internal_error",
                        "Clipboard authority is unavailable",
                    )
                })??;
            serde_json::from_value(v).map_err(|_| {
                tower_protocol::RpcError::application(
                    "internal_error",
                    "Cannot encode clipboard response",
                )
            })
        }
        .await;
        Ok(result)
    }
    async fn enroll(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let _permit = self
            .enrollment_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Error::Rpc(tower_protocol::RpcError::application(
                    "rate_limited",
                    "Enrollment workers are busy",
                ))
            })?;
        if request.method == "allgather.enrollment.begin" {
            let plan = self
                .execute(move |s| s.enrollment_plan(&request, peer))
                .await??;
            let started = crate::allgather::enrollment::begin(&plan)
                .await
                .map_err(|_| {
                    Error::Rpc(tower_protocol::RpcError::application(
                        "enrollment_unavailable",
                        "Cannot start enrollment at this Allgather server",
                    ))
                })?;
            return self
                .execute(move |s| s.remember_enrollment(plan, started))
                .await;
        }
        let pending = match self
            .execute(move |s| s.claim_enrollment_poll(&request, peer))
            .await??
        {
            Ok(pending) => pending,
            Err(result) => return Ok(Ok(result)),
        };
        let polled = crate::allgather::enrollment::poll(&pending).await;
        let id = pending.started.enrollment_id;
        let (result, confirmation) = self
            .execute(move |s| s.finish_enrollment_poll(&id, polled))
            .await??;
        if let Some(confirmation) = confirmation
            && crate::allgather::enrollment::confirm(&confirmation)
                .await
                .is_ok()
        {
            return self
                .execute(move |s| s.confirm_enrollment(&confirmation))
                .await;
        }
        Ok(Ok(result))
    }
    pub async fn remote(
        &self,
        request: tower_protocol::Request,
        epoch: u64,
        protocols: crate::allgather::ExecutionProtocols,
    ) -> std::result::Result<tower_protocol::Value, tower_protocol::RpcError> {
        let result = if request.method == "message.send" {
            let permit = self
                .delivery_gate
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    tower_protocol::RpcError::application(
                        "rate_limited",
                        "Delivery workers are busy",
                    )
                })?;
            let service = self.service.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                service.send_message_origin(
                    &request,
                    tower_pal::Peer {
                        uid: tower_pal::uid(),
                        pid: None,
                    },
                    Some(epoch),
                )
            })
            .await
            .map_err(|_| Error::Other("delivery worker failed".into()))
        } else {
            self.execute(move |s| s.remote_dispatch(&request, epoch, protocols))
                .await
        };
        result.map_err(|_| {
            tower_protocol::RpcError::application("internal_error", "Remote request failed")
        })?
    }
    pub async fn send_message(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let permit = self
            .delivery_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Error::Rpc(tower_protocol::RpcError::application(
                    "rate_limited",
                    "Delivery workers are busy",
                ))
            })?;
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            service.send_message(&request, peer)
        })
        .await
        .map_err(|_| Error::Other("delivery worker failed".into()))
    }
    pub async fn approval_watch(
        &self,
        mut request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let p: tower_protocol::ApprovalWatchParams =
            serde_json::from_value(serde_json::to_value(&request.params)?)?;
        if p.timeout_s > 600 {
            return Ok(Err(tower_protocol::RpcError::application(
                "invalid_params",
                "Watch timeout cannot exceed 600 seconds",
            )));
        }
        request.method = "approval.status".into();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(p.timeout_s);
        loop {
            let r = request.clone();
            let result = self.execute(move |s| s.dispatch(&r, peer)).await?;
            let now = tokio::time::Instant::now();
            match result {
                Ok(tower_protocol::Value::Object(mut fields))
                    if fields.get("state")
                        == Some(&tower_protocol::Value::String("pending".into())) =>
                {
                    if now >= deadline {
                        fields.insert("timed_out".into(), tower_protocol::Value::Bool(true));
                        return Ok(Ok(tower_protocol::Value::Object(fields)));
                    }
                }
                other => return Ok(other),
            }
            tokio::time::sleep_until(deadline.min(now + std::time::Duration::from_millis(250)))
                .await;
        }
    }
    pub async fn conversations(
        &self,
        request: tower_protocol::Request,
        peer: tower_pal::Peer,
    ) -> Result<std::result::Result<tower_protocol::Value, tower_protocol::RpcError>> {
        let permit = self
            .inventory_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Other("inventory worker closed".into()))?;
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            service.conversations(&request, peer)
        })
        .await
        .map_err(|_| Error::Other("inventory worker failed".into()))
    }
    /// After aborting queued callers, wait for any started mutation to finish
    /// before releasing the socket and singleton lock to a replacement daemon.
    pub async fn drain(&self) -> Result<()> {
        let _vcs = self
            .service
            .vcs
            .gate
            .acquire_many(4)
            .await
            .map_err(|_| Error::Other("VCS worker closed".into()))?;
        let _deliveries = self
            .delivery_gate
            .acquire_many(16)
            .await
            .map_err(|_| Error::Other("delivery worker closed".into()))?;
        let _inventories = self
            .inventory_gate
            .acquire_many(2)
            .await
            .map_err(|_| Error::Other("inventory worker closed".into()))?;
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| Error::Other("service worker closed".into()))?;
        Ok(())
    }
}
