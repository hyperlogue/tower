use notify::Watcher;
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
};
use tokio::sync::watch;

pub struct FileWatch {
    _watcher: notify::RecommendedWatcher,
    changes: watch::Receiver<u64>,
}
impl FileWatch {
    pub fn new(paths: &[PathBuf]) -> io::Result<Self> {
        let (updates, changes) = watch::channel(0u64);
        let targets = paths.to_vec();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if event.is_ok_and(|event| {
                    matches!(event.kind, notify::EventKind::Access(_))
                        || !event.paths.iter().any(|changed| {
                            targets
                                .iter()
                                .any(|target| target == changed || target.starts_with(changed))
                        })
                }) {
                    return;
                }
                updates.send_modify(|revision| *revision = revision.wrapping_add(1));
            })
            .map_err(io::Error::other)?;
        let mut roots = BTreeSet::new();
        for path in paths {
            let parent = path.parent().unwrap_or(Path::new("."));
            if let Some(root) = parent.ancestors().find(|p| p.is_dir()) {
                roots.insert(root.to_path_buf());
            }
        }
        if roots.is_empty() {
            return Err(io::Error::other("no observable file parents"));
        }
        for root in roots {
            watcher
                .watch(&root, notify::RecursiveMode::NonRecursive)
                .map_err(io::Error::other)?;
        }
        Ok(Self {
            _watcher: watcher,
            changes,
        })
    }
    pub async fn changed(&mut self) {
        if self.changes.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Recursive watch below `root` that signals changes to files named `name`,
/// or to any file when `name` is `None`.
pub struct TreeWatch {
    _watcher: notify::RecommendedWatcher,
    changes: watch::Receiver<u64>,
}
impl TreeWatch {
    pub fn new(root: &Path, name: impl Into<Option<&'static str>>) -> io::Result<Self> {
        let name = name.into();
        let (updates, changes) = watch::channel(0u64);
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if event.is_ok_and(|event| {
                    !matches!(event.kind, notify::EventKind::Access(_))
                        && event.paths.iter().any(|p| {
                            name.is_none_or(|name| p.file_name().is_some_and(|n| n == name))
                        })
                }) {
                    updates.send_modify(|revision| *revision = revision.wrapping_add(1));
                }
            })
            .map_err(io::Error::other)?;
        watcher
            .watch(root, notify::RecursiveMode::Recursive)
            .map_err(io::Error::other)?;
        Ok(Self {
            _watcher: watcher,
            changes,
        })
    }
    pub async fn changed(&mut self) {
        if self.changes.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Recursive watch below `root` that reports which paths changed.
pub struct PathWatch {
    _watcher: notify::RecommendedWatcher,
    changes: watch::Receiver<u64>,
    pending: std::sync::Arc<std::sync::Mutex<Option<BTreeSet<PathBuf>>>>,
}
impl PathWatch {
    pub fn new(root: &Path) -> io::Result<Self> {
        let (updates, changes) = watch::channel(0u64);
        let pending = std::sync::Arc::new(std::sync::Mutex::new(Some(BTreeSet::new())));
        let sink = pending.clone();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                let mut pending = sink.lock().unwrap_or_else(|e| e.into_inner());
                match event {
                    Ok(event) if matches!(event.kind, notify::EventKind::Access(_)) => return,
                    Ok(event) if !event.need_rescan() => {
                        if let Some(paths) = pending.as_mut() {
                            paths.extend(event.paths);
                        }
                    }
                    _ => *pending = None,
                }
                updates.send_modify(|revision| *revision = revision.wrapping_add(1));
            })
            .map_err(io::Error::other)?;
        watcher
            .watch(root, notify::RecursiveMode::Recursive)
            .map_err(io::Error::other)?;
        Ok(Self {
            _watcher: watcher,
            changes,
            pending,
        })
    }
    /// Waits for changes, then returns the changed paths, or `None` when
    /// events were lost and the caller must rescan.
    pub async fn changed(&mut self) -> Option<BTreeSet<PathBuf>> {
        if self.changes.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(BTreeSet::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_file_replacement_is_observed_without_read_feedback() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("state");
        std::fs::write(&target, "first").unwrap();
        let mut watcher = FileWatch::new(std::slice::from_ref(&target)).unwrap();
        let next = dir.path().join("replacement");
        std::fs::write(&next, "second").unwrap();
        std::fs::rename(next, &target).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), watcher.changed())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        watcher.changes.borrow_and_update();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "second");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), watcher.changed())
                .await
                .is_err()
        );
    }
}
