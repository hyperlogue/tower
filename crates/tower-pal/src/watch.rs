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
