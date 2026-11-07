use crate::{Error, worker::ServiceWorker};
use std::sync::Arc;
use tower_clipboard::{Broker, LeaseGuard, ProviderGuard, ProviderRequest};
use tower_protocol::{
    CallerContext, ClipboardAttach, ClipboardChunk, ClipboardOffer, Request, RpcError, Value,
};
struct Provider {
    _guard: ProviderGuard,
    id: String,
    context: CallerContext,
    receive: tokio::sync::mpsc::Receiver<ProviderRequest>,
}
pub(crate) struct Connection {
    broker: Arc<Broker>,
    provider: Option<Provider>,
    attachment: Option<LeaseGuard>,
}
fn unavailable(_: impl std::fmt::Display) -> RpcError {
    RpcError::application(
        "clipboard_unavailable",
        "Clipboard connection is unavailable",
    )
}
fn public(v: impl serde::Serialize) -> Result<Value, RpcError> {
    serde_json::to_value(v)
        .and_then(serde_json::from_value)
        .map_err(unavailable)
}
impl Connection {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self {
            broker,
            provider: None,
            attachment: None,
        }
    }
    pub fn context(&self) -> Option<CallerContext> {
        self.provider.as_ref().map(|p| p.context.clone())
    }
    pub async fn next(&mut self) -> Option<ProviderRequest> {
        if let Some(p) = &mut self.provider {
            let next = p.receive.recv().await;
            if next.is_none() {
                self.provider = None;
            }
            next
        } else {
            std::future::pending().await
        }
    }
    pub async fn request(
        &mut self,
        request: &Request,
        peer: tower_pal::Peer,
        service: &ServiceWorker,
    ) -> Option<Result<Value, RpcError>> {
        if !matches!(
            request.method.as_str(),
            "clipboard.offer" | "clipboard.chunk" | "clipboard.revoke" | "clipboard.attach"
        ) {
            return None;
        }
        Some(
            async {
                match request.method.as_str() {
                    "clipboard.offer" => {
                        if self.provider.is_some() || self.attachment.is_some() {
                            return Err(RpcError::application(
                                "conflict",
                                "This connection already has a clipboard role",
                            ));
                        }
                        let copy = request.clone();
                        service
                            .execute(move |s| s.clipboard_authorize_provider(&copy, peer))
                            .await
                            .map_err(unavailable)??;
                        let offer: ClipboardOffer = serde_json::from_value(
                            serde_json::to_value(&request.params).map_err(unavailable)?,
                        )
                        .map_err(unavailable)?;
                        let (client, guard, receive) = self.broker.offer(offer)?;
                        self.provider = Some(Provider {
                            _guard: guard,
                            id: client.client_id.clone(),
                            context: request.context.clone(),
                            receive,
                        });
                        public(client)
                    }
                    "clipboard.chunk" => {
                        let provider = self
                            .provider
                            .as_ref()
                            .ok_or_else(|| unavailable("no provider"))?;
                        let mut copy = request.clone();
                        copy.context = provider.context.clone();
                        service
                            .execute(move |s| s.clipboard_authorize_provider(&copy, peer))
                            .await
                            .map_err(unavailable)??;
                        let chunk: ClipboardChunk = serde_json::from_value(
                            serde_json::to_value(&request.params).map_err(unavailable)?,
                        )
                        .map_err(unavailable)?;
                        self.broker.chunk(&provider.id, chunk)?;
                        Ok(Value::Null)
                    }
                    "clipboard.revoke" => {
                        self.provider = None;
                        Ok(Value::Null)
                    }
                    "clipboard.attach" => {
                        if self.provider.is_some() || self.attachment.is_some() {
                            return Err(RpcError::application(
                                "conflict",
                                "This connection already has a clipboard role",
                            ));
                        }
                        let p: ClipboardAttach = serde_json::from_value(
                            serde_json::to_value(&request.params).map_err(unavailable)?,
                        )
                        .map_err(unavailable)?;
                        let id = p.id.clone();
                        let session = service
                            .execute(move |s| s.clipboard_target(&id))
                            .await
                            .map_err(unavailable)??;
                        let (lease, guard) = self.broker.attach(&p, &session)?;
                        self.attachment = Some(guard);
                        public(lease)
                    }
                    _ => unreachable!(),
                }
            }
            .await,
        )
    }
    pub async fn check(&self, service: &ServiceWorker, peer: tower_pal::Peer) -> Result<(), Error> {
        if let Some(context) = self.context() {
            let request = Request {
                jsonrpc: "2.0".into(),
                id: None,
                method: "clipboard.offer".into(),
                params: Value::Null,
                context,
            };
            service
                .execute(move |s| s.clipboard_authorize_provider(&request, peer))
                .await??;
        }
        Ok(())
    }
}
