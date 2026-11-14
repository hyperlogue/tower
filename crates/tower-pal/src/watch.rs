use notify::Watcher;
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
};
use tokio::sync::watch;

/// `changed` names `target` or a directory that contains it.
/// macOS reports the real path (`/private/tmp`) for a watch opened through
/// `/tmp`, so the original string comparison misses the event.
fn concerns(target: &Path, changed: &Path) -> bool {
    let hit = |target: &Path, changed: &Path| target == changed || target.starts_with(changed);
    if hit(target, changed) {
        return true;
    }
    let changed = std::fs::canonicalize(changed).unwrap_or_else(|_| changed.to_path_buf());
    if let Ok(target) = std::fs::canonicalize(target) {
        return hit(&target, &changed);
    }
    for ancestor in target.ancestors() {
        let Ok(real) = std::fs::canonicalize(ancestor) else {
            continue;
        };
        let Ok(rest) = target.strip_prefix(ancestor) else {
            continue;
        };
        return hit(&real.join(rest), &changed);
    }
    false
}

pub struct FileWatch {
    _watcher: notify::RecommendedWatcher,
    #[cfg(target_os = "macos")]
    _vnodes: std::sync::Arc<vnode::Vnodes>,
    changes: watch::Receiver<u64>,
}
impl FileWatch {
    pub fn new(paths: &[PathBuf]) -> io::Result<Self> {
        let (updates, changes) = watch::channel(0u64);
        let targets = paths.to_vec();
        #[cfg(target_os = "macos")]
        let vnodes = {
            let updates = updates.clone();
            let vnodes = std::sync::Arc::new(vnode::Vnodes::new(move |_| {
                updates.send_modify(|revision| *revision = revision.wrapping_add(1));
            })?);
            vnodes.set(targets.iter().cloned());
            vnodes
        };
        #[cfg(target_os = "macos")]
        let opened = (vnodes.clone(), targets.clone());
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if event.is_ok_and(|event| {
                    matches!(event.kind, notify::EventKind::Access(_))
                        || !event
                            .paths
                            .iter()
                            .any(|changed| targets.iter().any(|target| concerns(target, changed)))
                }) {
                    return;
                }
                // A target that was created or replaced needs a new descriptor.
                #[cfg(target_os = "macos")]
                opened.0.set(opened.1.iter().cloned());
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
            // macOS reports events for the real directory. Linux already
            // watches the path the caller opened.
            #[cfg(target_os = "macos")]
            let root = std::fs::canonicalize(&root).unwrap_or(root);
            watcher
                .watch(&root, notify::RecursiveMode::NonRecursive)
                .map_err(io::Error::other)?;
        }
        Ok(Self {
            _watcher: watcher,
            #[cfg(target_os = "macos")]
            _vnodes: vnodes,
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
        #[cfg(target_os = "macos")]
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        #[cfg(not(target_os = "macos"))]
        let root = root.to_path_buf();
        watcher
            .watch(&root, notify::RecursiveMode::Recursive)
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
    #[cfg(target_os = "macos")]
    vnodes: vnode::Vnodes,
    changes: watch::Receiver<u64>,
    pending: std::sync::Arc<std::sync::Mutex<Option<BTreeSet<PathBuf>>>>,
}
impl PathWatch {
    pub fn new(root: &Path) -> io::Result<Self> {
        let (updates, changes) = watch::channel(0u64);
        let pending = std::sync::Arc::new(std::sync::Mutex::new(Some(BTreeSet::new())));
        #[cfg(target_os = "macos")]
        let vnodes = {
            let (updates, sink) = (updates.clone(), pending.clone());
            vnode::Vnodes::new(move |path| {
                if let Some(paths) = sink.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                    paths.insert(path.to_path_buf());
                }
                updates.send_modify(|revision| *revision = revision.wrapping_add(1));
            })?
        };
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
        #[cfg(target_os = "macos")]
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        #[cfg(not(target_os = "macos"))]
        let root = root.to_path_buf();
        watcher
            .watch(&root, notify::RecursiveMode::Recursive)
            .map_err(io::Error::other)?;
        Ok(Self {
            _watcher: watcher,
            #[cfg(target_os = "macos")]
            vnodes,
            changes,
            pending,
        })
    }
    /// Keeps watching `paths` while their writers hold them open, and returns
    /// the ones newly watched, which may have changed before the watch began.
    /// The recursive watch already reports such writes except on macOS.
    pub fn follow(&mut self, paths: impl IntoIterator<Item = PathBuf>) -> BTreeSet<PathBuf> {
        #[cfg(target_os = "macos")]
        return self.vnodes.set(paths);
        #[cfg(not(target_os = "macos"))]
        {
            drop(paths.into_iter());
            BTreeSet::new()
        }
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

/// FSEvents reports nothing for writes through a descriptor that stays open
/// until it is closed, which is how Codex appends and SQLite commits. A vnode
/// event on the file itself fires for every write.
#[cfg(target_os = "macos")]
mod vnode {
    use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::File,
        io,
        os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    };

    const NOW: Option<nix::libc::timespec> = Some(nix::libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    });
    struct Shared {
        queue: Kqueue,
        files: Mutex<BTreeMap<PathBuf, File>>,
    }
    pub(super) struct Vnodes(Arc<Shared>);
    impl Vnodes {
        pub(super) fn new(changed: impl Fn(&Path) + Send + 'static) -> io::Result<Self> {
            let shared = Arc::new(Shared {
                queue: Kqueue::new()?,
                files: Mutex::default(),
            });
            let stop =
                |flags, trigger| KEvent::new(0, EventFilter::EVFILT_USER, flags, trigger, 0, 0);
            shared.queue.kevent(
                &[stop(
                    EvFlags::EV_ADD | EvFlags::EV_CLEAR,
                    FilterFlag::empty(),
                )],
                &mut [],
                NOW,
            )?;
            let worker = shared.clone();
            std::thread::Builder::new()
                .name("vnode-watch".into())
                .spawn(move || {
                    let mut events = [stop(EvFlags::empty(), FilterFlag::empty()); 16];
                    loop {
                        let count = match worker.queue.kevent(&[], &mut events, None) {
                            Ok(count) => count,
                            Err(nix::errno::Errno::EINTR) => continue,
                            Err(_) => return,
                        };
                        for event in &events[..count] {
                            if event.filter() == Ok(EventFilter::EVFILT_USER) {
                                return;
                            }
                            let mut files = worker.files.lock().unwrap_or_else(|e| e.into_inner());
                            let Some(path) = files
                                .iter()
                                .find(|(_, file)| file.as_raw_fd() as usize == event.ident())
                                .map(|(path, _)| path.clone())
                            else {
                                continue;
                            };
                            // The name now refers to another file, or to none.
                            if event
                                .fflags()
                                .intersects(FilterFlag::NOTE_DELETE | FilterFlag::NOTE_RENAME)
                            {
                                files.remove(&path);
                            }
                            drop(files);
                            changed(&path);
                        }
                    }
                })?;
            Ok(Self(shared))
        }
        /// Watches exactly the `paths` that exist, and returns the added ones.
        pub(super) fn set(&self, paths: impl IntoIterator<Item = PathBuf>) -> BTreeSet<PathBuf> {
            let wanted = paths.into_iter().collect::<BTreeSet<_>>();
            let mut files = self.0.files.lock().unwrap_or_else(|e| e.into_inner());
            files.retain(|path, _| wanted.contains(path));
            let mut added = BTreeSet::new();
            for path in wanted {
                if files.contains_key(&path) {
                    continue;
                }
                let Ok(file) = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(nix::libc::O_EVTONLY)
                    .open(&path)
                else {
                    continue;
                };
                let change = KEvent::new(
                    file.as_raw_fd() as usize,
                    EventFilter::EVFILT_VNODE,
                    EvFlags::EV_ADD | EvFlags::EV_CLEAR,
                    FilterFlag::NOTE_WRITE
                        | FilterFlag::NOTE_EXTEND
                        | FilterFlag::NOTE_DELETE
                        | FilterFlag::NOTE_RENAME,
                    0,
                    0,
                );
                if self.0.queue.kevent(&[change], &mut [], NOW).is_ok() {
                    files.insert(path.clone(), file);
                    added.insert(path);
                }
            }
            added
        }
    }
    impl Drop for Vnodes {
        fn drop(&mut self) {
            let wake = KEvent::new(
                0,
                EventFilter::EVFILT_USER,
                EvFlags::empty(),
                FilterFlag::NOTE_TRIGGER,
                0,
                0,
            );
            let _ = self.0.queue.kevent(&[wake], &mut [], NOW);
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
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn appends_through_a_held_descriptor_are_observed() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("log");
        let mut file = std::fs::File::create(&target).unwrap();
        let mut watcher = FileWatch::new(std::slice::from_ref(&target)).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        watcher.changes.borrow_and_update();
        file.write_all(b"line\n").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), watcher.changed())
            .await
            .unwrap();
        let mut tree = PathWatch::new(dir.path()).unwrap();
        assert_eq!(tree.follow([target.clone()]).len(), 1);
        assert!(tree.follow([target.clone()]).is_empty());
        file.write_all(b"line\n").unwrap();
        let changed = tokio::time::timeout(std::time::Duration::from_secs(3), tree.changed())
            .await
            .unwrap();
        assert!(changed.is_none_or(|paths| paths.contains(&target)));
    }
}
