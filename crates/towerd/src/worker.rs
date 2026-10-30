//! One blocking operation at a time; queued callers wait without occupying
//! Tokio workers. Serialization includes filesystem credential publication.
use crate::{Error, Result, service::Service};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) struct ServiceWorker {
    service: Arc<Service>,
    gate: Arc<Semaphore>,
}
impl ServiceWorker {
    pub fn new(service: Service) -> Self {
        Self {
            service: Arc::new(service),
            gate: Arc::new(Semaphore::new(1)),
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
    /// After aborting queued callers, wait for any started mutation to finish
    /// before releasing the socket and singleton lock to a replacement daemon.
    pub async fn drain(&self) -> Result<()> {
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| Error::Other("service worker closed".into()))?;
        Ok(())
    }
}
