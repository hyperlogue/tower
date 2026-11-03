use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tower_protocol::{DiskMetrics, HostMetrics, MemoryMetrics, RpcError};

type Probe = Box<dyn FnMut(&Path) -> tower_pal::HostSample + Send>;

struct State {
    probe: Probe,
    cached: Option<(Instant, HostMetrics)>,
}

#[derive(Clone)]
pub(crate) struct Metrics {
    home: PathBuf,
    state: Arc<Mutex<State>>,
}

impl Metrics {
    pub fn new(home: PathBuf) -> Self {
        let mut sampler = tower_pal::HostSampler::default();
        Self::with_probe(home, Box::new(move |home| sampler.sample(home)))
    }

    fn with_probe(home: PathBuf, probe: Probe) -> Self {
        Self {
            home,
            state: Arc::new(Mutex::new(State {
                probe,
                cached: None,
            })),
        }
    }

    pub async fn get(&self) -> Result<HostMetrics, RpcError> {
        tokio::time::timeout(Duration::from_secs(2), self.read())
            .await
            .map_err(|_| unavailable())?
    }

    async fn read(&self) -> Result<HostMetrics, RpcError> {
        let mut state = self.state.clone().lock_owned().await;
        if let Some((at, sample)) = &state.cached
            && at.elapsed() < Duration::from_secs(1)
        {
            return Ok(sample.clone());
        }
        let home = self.home.clone();
        // The blocking task owns the probe lock, even if its requesting client
        // disconnects. A stalled probe cannot start an unbounded number of workers.
        tokio::task::spawn_blocking(move || {
            let sample = (state.probe)(&home);
            let sampled_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .ok_or_else(unavailable)?;
            let sample = HostMetrics {
                sampled_at,
                cpu_percent: sample.cpu_percent,
                memory: MemoryMetrics {
                    used_bytes: sample.memory_used_bytes,
                    total_bytes: sample.memory_total_bytes,
                },
                disk: DiskMetrics {
                    available_bytes: sample.disk_available_bytes,
                },
            };
            state.cached = Some((Instant::now(), sample.clone()));
            Ok(sample)
        })
        .await
        .map_err(|_| unavailable())?
    }
}

fn unavailable() -> RpcError {
    RpcError::application("metrics_unavailable", "Host metrics could not be sampled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn concurrent_readers_share_one_probe_and_cache_timestamp() {
        let calls = Arc::new(AtomicUsize::new(0));
        let measured = calls.clone();
        let metrics = Metrics::with_probe(
            PathBuf::from("/fixture/home"),
            Box::new(move |home| {
                assert_eq!(home, Path::new("/fixture/home"));
                measured.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                tower_pal::HostSample {
                    cpu_percent: Some(25.0),
                    memory_used_bytes: Some(3),
                    memory_total_bytes: Some(8),
                    disk_available_bytes: Some(0),
                }
            }),
        );
        let (a, b, c) = tokio::join!(metrics.get(), metrics.get(), metrics.get());
        let a = a.unwrap();
        assert_eq!(a, b.unwrap());
        assert_eq!(a, c.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(a.disk.available_bytes, Some(0));
        assert!(a.sampled_at > 0);
        metrics.state.lock().await.cached.as_mut().unwrap().0 -= Duration::from_secs(2);
        metrics.get().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelling_a_reader_does_not_duplicate_or_cancel_the_shared_probe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let measured = calls.clone();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let metrics = Metrics::with_probe(
            PathBuf::new(),
            Box::new(move |_| {
                measured.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = started.take() {
                    let _ = started.send(());
                }
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                tower_pal::HostSample::default()
            }),
        );
        let a = metrics.clone();
        let pending = tokio::spawn(async move { a.get().await });
        waiting.await.unwrap();
        pending.abort();
        let _ = pending.await;
        let b = metrics.clone();
        let next = tokio::spawn(async move { b.get().await });
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        let result = next.await.unwrap().unwrap();
        assert_eq!(result.cpu_percent, None);
        assert_eq!(result.memory.used_bytes, None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
