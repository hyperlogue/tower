use super::Facts;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tower_protocol::Patch;

type Stamp = ((u64, u64), u64, Option<SystemTime>);
fn stamp(path: &Path) -> Option<Stamp> {
    let m = path.metadata().ok()?;
    Some((
        tower_pal::file_identity(path).ok()?,
        m.len(),
        m.modified().ok(),
    ))
}
pub(super) struct Reader {
    pub id: String,
    root: PathBuf,
    log_stamp: Option<Stamp>,
    log_facts: Facts,
    title_stamp: Option<(Option<Stamp>, Option<Stamp>)>,
    title: Patch<String>,
    tasks: Tasks,
    pub more: bool,
}
impl Reader {
    pub fn new(root: PathBuf, id: String) -> Self {
        Self {
            tasks: Tasks::new(root.join("conversations").join(format!("{id}.db"))),
            id,
            root,
            log_stamp: None,
            log_facts: Facts::default(),
            title_stamp: None,
            title: Patch::Missing,
            more: false,
        }
    }
    fn log(&self) -> PathBuf {
        self.root
            .join("brain")
            .join(&self.id)
            .join(".system_generated/logs/transcript.jsonl")
    }
    pub fn paths(&self) -> Vec<PathBuf> {
        vec![
            self.log(),
            self.tasks.path.clone(),
            PathBuf::from(format!("{}-wal", self.tasks.path.display())),
            self.root.join("conversation_summaries.db"),
            self.root.join("conversation_summaries.db-wal"),
        ]
    }
    pub fn read(&mut self, idle: bool) -> Facts {
        let log = self.log();
        let version = stamp(&log);
        if version.is_some()
            && version != self.log_stamp
            && let Ok(facts) = compaction(&log)
        {
            self.log_stamp = version;
            self.log_facts = facts;
        }
        let mut facts = self.log_facts.clone();
        if idle {
            facts.tasks = self.tasks.read().ok();
            self.more = self.tasks.more;
        } else {
            self.more = false;
        }
        let path = self.root.join("conversation_summaries.db");
        let version = (
            stamp(&path),
            stamp(&self.root.join("conversation_summaries.db-wal")),
        );
        if version.0.is_some() && self.title_stamp.as_ref() != Some(&version) {
            let read = || -> rusqlite::Result<Option<String>> {
                let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                conn.busy_timeout(Duration::ZERO)?;
                conn.query_row("SELECT substr(coalesce(title,''),1,200) FROM conversation_summaries WHERE conversation_id=?1",[&self.id],|r|r.get(0)).optional()
            };
            if let Ok(title) = read() {
                self.title = title
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.chars().filter(|c| !c.is_control()).collect())
                    .map_or(Patch::Clear, Patch::Set);
                self.title_stamp = Some(version);
            }
        }
        facts.title = self.title.clone();
        facts
    }
}

// A bounded backward scan ignores partial records and stops at measured usage
// or a checkpoint. Exhausting the budget withholds usage instead of inventing it.
fn compaction(path: &Path) -> io::Result<Facts> {
    let mut file = tower_pal::open_regular(path)?;
    let mut position = file.metadata()?.len();
    let mut budget = 8 * 1024 * 1024usize;
    let mut block = [0u8; 8192];
    let mut line = Vec::new();
    let mut complete = false;
    let mut oversized = false;
    let mut uncertain = false;
    let mut requested = None;
    while position > 0 && budget > 0 {
        let n = (position as usize).min(block.len()).min(budget);
        position -= n as u64;
        budget -= n;
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(&mut block[..n])?;
        for byte in block[..n].iter().rev() {
            if *byte == b'\n' {
                if complete && !oversized {
                    line.reverse();
                    if let Some(facts) = fold(&line, &mut requested) {
                        return Ok(if uncertain {
                            Facts {
                                stale_usage: Some(true),
                                ..Default::default()
                            }
                        } else {
                            facts
                        });
                    }
                }
                uncertain |= oversized;
                line.clear();
                oversized = false;
                complete = true;
            } else if complete {
                if line.len() < 256 * 1024 {
                    line.push(*byte);
                } else {
                    oversized = true;
                }
            }
        }
    }
    if position == 0 && complete && !oversized {
        line.reverse();
        if let Some(facts) = fold(&line, &mut requested) {
            return Ok(if uncertain {
                Facts {
                    stale_usage: Some(true),
                    ..Default::default()
                }
            } else {
                facts
            });
        }
    }
    Ok(Facts {
        stale_usage: Some(position > 0 || oversized || uncertain),
        compacting: requested == Some(true),
        ..Default::default()
    })
}
fn fold(line: &[u8], requested: &mut Option<bool>) -> Option<Facts> {
    let row: serde_json::Value = serde_json::from_slice(line).ok()?;
    match row["type"].as_str()? {
        "USER_INPUT" if requested.is_none() => {
            if let Some(body) = row["content"]
                .as_str()
                .and_then(|s| s.split_once("<USER_REQUEST>"))
                .and_then(|(_, s)| s.split_once("</USER_REQUEST>"))
                .map(|(s, _)| s)
            {
                *requested = Some(body.split_whitespace().next() == Some("/compact"));
            }
        }
        "CHECKPOINT" if row["status"] == "DONE" => {
            return Some(Facts {
                stale_usage: Some(true),
                compacting: *requested == Some(true),
                compacted: requested.is_none(),
                ..Default::default()
            });
        }
        "PLANNER_RESPONSE"
            if row["status"] == "DONE"
                && ["input_tokens", "cache_read_tokens"]
                    .iter()
                    .any(|key| row[key].as_u64().is_some_and(|n| n > 0)) =>
        {
            return Some(Facts {
                stale_usage: Some(false),
                compacting: *requested == Some(true),
                ..Default::default()
            });
        }
        _ => {}
    }
    None
}
struct Tasks {
    path: PathBuf,
    identity: Option<(u64, u64)>,
    connection: Option<Connection>,
    through: i64,
    unfinished: BTreeMap<i64, bool>,
    more: bool,
}
impl Tasks {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            identity: None,
            connection: None,
            through: -1,
            unfinished: BTreeMap::new(),
            more: false,
        }
    }
    fn read(&mut self) -> rusqlite::Result<u32> {
        self.more = false;
        let identity =
            tower_pal::file_identity(&self.path).map_err(|_| rusqlite::Error::InvalidQuery)?;
        if self.identity != Some(identity) || self.connection.is_none() {
            let conn = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            conn.busy_timeout(Duration::ZERO)?;
            self.connection = Some(conn);
            self.identity = Some(identity);
            self.through = -1;
            self.unfinished.clear();
        }
        let tx = self.connection.as_mut().unwrap().transaction()?;
        let end: i64 = tx.query_row("SELECT coalesce(max(idx),-1) FROM steps", [], |r| r.get(0))?;
        let mut unfinished = if end < self.through {
            BTreeMap::new()
        } else {
            self.unfinished.clone()
        };
        let mut through = if end < self.through { -1 } else { self.through };
        for id in unfinished.keys().copied().collect::<Vec<_>>() {
            let (status, details): (i32, i64) = tx.query_row(
                "SELECT status, coalesce(length(task_details),0) FROM steps WHERE idx=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            fold_task(&mut unfinished, id, status, details)?;
        }
        let mut query=tx.prepare("SELECT idx,status,coalesce(length(task_details),0) FROM steps WHERE idx>?1 ORDER BY idx LIMIT 10000")?;
        let mut rows = query.query([through])?;
        while let Some(row) = rows.next()? {
            let id = row.get(0)?;
            fold_task(&mut unfinished, id, row.get(1)?, row.get(2)?)?;
            through = id;
        }
        self.through = through;
        self.unfinished = unfinished;
        self.more = through < end;
        if self.more {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(self.unfinished.values().filter(|v| **v).count() as u32)
    }
}
fn fold_task(
    tasks: &mut BTreeMap<i64, bool>,
    id: i64,
    status: i32,
    details: i64,
) -> rusqlite::Result<()> {
    match status {
        0 | 1 | 2 | 8 | 9 | 11 => {
            tasks.insert(id, details > 0);
        }
        3..=7 | 12 => {
            tasks.remove(&id);
        }
        _ => return Err(rusqlite::Error::InvalidQuery),
    }
    if tasks.len() > 4096 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn compaction_uses_complete_native_records_and_withholds_unknown_usage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let measured = r#"{"type":"PLANNER_RESPONSE","status":"DONE","input_tokens":20}"#;
        std::fs::write(&path, format!("{measured}\n{{\"type\":\"CHECKPOINT\"")).unwrap();
        assert_eq!(compaction(&path).unwrap().stale_usage, Some(false));
        std::fs::write(
            &path,
            format!("{measured}\n{{\"type\":\"CHECKPOINT\",\"status\":\"DONE\"}}\n"),
        )
        .unwrap();
        let facts = compaction(&path).unwrap();
        assert_eq!(facts.stale_usage, Some(true));
        assert!(facts.compacted);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, "{measured}").unwrap();
        assert_eq!(compaction(&path).unwrap().stale_usage, Some(false));
        writeln!(file, "{}", "x".repeat(300_000)).unwrap();
        let facts = compaction(&path).unwrap();
        assert_eq!(facts.stale_usage, Some(true));
        assert!(!facts.compacted);
    }
    #[test]
    fn native_tasks_revisit_unfinished_rows_and_do_not_treat_unknown_as_idle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE steps(idx INTEGER PRIMARY KEY,status INTEGER,task_details BLOB); INSERT INTO steps VALUES(0,1,x'01'),(1,2,NULL);").unwrap();
        let mut reader = Tasks::new(path);
        assert_eq!(reader.read().unwrap(), 1);
        db.execute("UPDATE steps SET status=3 WHERE idx=0", [])
            .unwrap();
        assert_eq!(reader.read().unwrap(), 0);
        db.execute("UPDATE steps SET status=99 WHERE idx=1", [])
            .unwrap();
        reader.more = true;
        assert!(reader.read().is_err());
        assert!(!reader.more);
        db.execute_batch("DELETE FROM steps; INSERT INTO steps VALUES(0,2,x'01');")
            .unwrap();
        assert_eq!(reader.read().unwrap(), 1);
    }
    #[test]
    fn native_titles_are_presence_aware_and_private_task_text_stays_native() {
        let dir = tempfile::tempdir().unwrap();
        let db = Connection::open(dir.path().join("conversation_summaries.db")).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE conversation_summaries(conversation_id TEXT,title TEXT); INSERT INTO conversation_summaries VALUES('one','Chosen title');").unwrap();
        let mut reader = Reader::new(dir.path().into(), "one".into());
        assert_eq!(reader.read(false).title, Patch::Set("Chosen title".into()));
        db.execute("DELETE FROM conversation_summaries", [])
            .unwrap();
        assert_eq!(reader.read(false).title, Patch::Clear);
    }
}
