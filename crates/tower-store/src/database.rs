use crate::{Error, Result};
use rusqlite::{Connection, params};
use std::{
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
    time::Duration,
};

const SCHEMA_VERSION: u32 = 2;
const REGISTRY: &str = include_str!("schema.sql");
const RUNTIME: &str = include_str!("runtime.sql");

/// Shared by persistence modules; domain callers use Store or Journal operations.
static CHANGES: std::sync::LazyLock<tokio::sync::watch::Sender<u64>> =
    std::sync::LazyLock::new(|| tokio::sync::watch::channel(0).0);
/// Signals that persistent or live process state may have changed.
pub fn touch() {
    CHANGES.send_modify(|n| *n = n.wrapping_add(1));
}
static TABLES: std::sync::LazyLock<
    Mutex<std::collections::BTreeMap<String, tokio::sync::watch::Sender<u64>>>,
> = std::sync::LazyLock::new(Default::default);
/// Wakes when rows of `table` are written in this process, including writes
/// later rolled back.
pub fn table_changes(table: &str) -> tokio::sync::watch::Receiver<u64> {
    TABLES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(table.to_owned())
        .or_insert_with(|| tokio::sync::watch::channel(0).0)
        .subscribe()
}
/// Wakes after any database commit or [`touch`] in this process.
pub fn changes() -> tokio::sync::watch::Receiver<u64> {
    CHANGES.subscribe()
}
pub struct Database {
    conn: Mutex<Connection>,
    runtime_directory: PathBuf,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let runtime_directory = path.parent().unwrap_or(Path::new(".")).join("runtime");
        let memory = path == Path::new(":memory:");
        let marker = runtime_directory.join("journal.id");
        if !memory {
            let missing = match path.metadata() {
                Ok(metadata) => metadata.len() == 0,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(error) => return Err(error.into()),
            };
            if missing && marker.try_exists()? {
                return Err(Error::Conflict(
                    "Restore the database; its journal identity cannot be reset",
                ));
            }
            tower_pal::private_file(path, true)?;
        }
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let version: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        match version {
            0 | 1 => {
                let old = runtime_directory.join("runtime.db");
                let import = !memory && old.try_exists()?;
                if import && version != 1 {
                    return Err(Error::Conflict(
                        "Restore the registry paired with the runtime journal",
                    ));
                }
                if !memory && !import && marker.try_exists()? {
                    return Err(Error::Conflict(
                        "Restore the missing runtime journal before upgrading",
                    ));
                }
                if import {
                    attach_journal(&conn, &old, &runtime_directory)?;
                }
                let tx = conn.transaction()?;
                if version == 0 {
                    tx.execute_batch(REGISTRY)?;
                }
                tx.execute_batch(RUNTIME)?;
                if import {
                    for table in [
                        "runtimes",
                        "commands",
                        "events",
                        "journal_identity",
                        "force_resumes",
                    ] {
                        let columns = tx
                            .prepare(&format!("PRAGMA main.table_info({table})"))?
                            .query_map([], |row| row.get::<_, String>(1))?
                            .collect::<std::result::Result<Vec<_>, _>>()?
                            .join(",");
                        tx.execute_batch(&format!("INSERT INTO main.{table} ({columns}) SELECT {columns} FROM previous.{table}"))?;
                    }
                    tx.execute_batch("DELETE FROM main.runtime_counters; INSERT INTO main.runtime_counters SELECT * FROM previous.counters;")?;
                    if tx
                        .prepare("PRAGMA main.foreign_key_check")?
                        .query([])?
                        .next()?
                        .is_some()
                    {
                        return Err(Error::Conflict(
                            "Runtime journal contains broken references",
                        ));
                    }
                }
                tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                tx.commit()?;
                if import {
                    conn.execute_batch("DETACH DATABASE previous")?;
                }
            }
            SCHEMA_VERSION => {}
            _ => return Err(Error::Schema),
        }
        conn.commit_hook(Some(|| {
            touch();
            false
        }))?;
        conn.update_hook(Some(|_, _: &str, table: &str, _| {
            if let Some(changes) = TABLES.lock().unwrap_or_else(|e| e.into_inner()).get(table) {
                changes.send_modify(|n| *n = n.wrapping_add(1));
            }
        }))?;
        Ok(Self {
            conn: Mutex::new(conn),
            runtime_directory,
        })
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| Error::Poisoned)
    }
    /// The connection for writes that only restate what their source will
    /// report again, such as a session's status. Their commits do not wait
    /// for the disk: after a power loss the last of them may be missing,
    /// while the database stays consistent. Every other commit still waits,
    /// and takes these with it.
    pub fn lock_relaxed(&self) -> Result<Relaxed<'_>> {
        let conn = self.lock()?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Relaxed(conn))
    }

    pub fn runtime_directory(&self) -> PathBuf {
        self.runtime_directory.clone()
    }
}

fn attach_journal(conn: &Connection, path: &Path, directory: &Path) -> Result<()> {
    tower_pal::require_private_dir(directory)?;
    let _file = tower_pal::private_file(path, false)?;
    let mut uri = url::Url::from_file_path(path.canonicalize()?)
        .map_err(|_| Error::Conflict("Invalid runtime journal path"))?;
    uri.set_query(Some("mode=ro"));
    conn.execute("ATTACH DATABASE ?1 AS previous", params![uri.as_str()])?;
    let version: u32 = conn.query_row("PRAGMA previous.user_version", [], |r| r.get(0))?;
    if version != 3 {
        return Err(Error::Schema);
    }
    let identity: String =
        conn.query_row("SELECT id FROM previous.journal_identity", [], |r| r.get(0))?;
    if tower_pal::read_private(&directory.join("journal.id"))? != identity.as_bytes()
        || tower_pal::read_private(&directory.join("fingerprint.key"))?.len() != 32
    {
        return Err(Error::Conflict(
            "Restore the runtime journal's matching identity and fingerprint key",
        ));
    }
    // Only the main database is written. The old journal remains a recovery copy.
    Ok(())
}

#[cfg(test)]
mod tests;

pub struct Relaxed<'a>(MutexGuard<'a, Connection>);
impl std::ops::Deref for Relaxed<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.0
    }
}
impl std::ops::DerefMut for Relaxed<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.0
    }
}
impl Drop for Relaxed<'_> {
    fn drop(&mut self) {
        let _ = self.0.pragma_update(None, "synchronous", "FULL");
    }
}
