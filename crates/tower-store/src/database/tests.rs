use super::*;
use rusqlite::types::Value;

fn legacy(root: &Path) -> (Connection, Connection) {
    let registry = Connection::open(root.join("tower.db")).unwrap();
    registry.execute_batch(REGISTRY).unwrap();
    registry.execute_batch("PRAGMA user_version=1; INSERT INTO host VALUES(1,'host','standalone','paired','allgather'); UPDATE counters SET value=37 WHERE name='registry';").unwrap();
    let directory = root.join("runtime");
    tower_pal::private_dir(&directory).unwrap();
    tower_pal::write_private(&directory.join("journal.id"), b"journal_fixture").unwrap();
    tower_pal::write_private(&directory.join("fingerprint.key"), &[7; 32]).unwrap();
    let runtime = Connection::open(directory.join("runtime.db")).unwrap();
    runtime
        .execute_batch(&RUNTIME.replace("runtime_counters", "counters"))
        .unwrap();
    runtime.execute_batch(r#"PRAGMA user_version=3;
INSERT INTO journal_identity VALUES('journal_fixture');
INSERT INTO runtimes(namespace,runtime_id,generation,logical_session,tower_id,owner_id,binding,state,fenced,termination,next_sequence,ack_sequence,ack_key,spool_bytes,loss,loss_ack) VALUES('pair','runtime','generation','session','tower','owner','{}','lost',1,'unknown',9,7,'event-seven',123,'{"loss_id":"stable-loss","report_revision":2}',1);
INSERT INTO commands VALUES('pair','command','runtime','generation',X'010203','resume','{}',100,'{"state":"accepted"}',4,3,1);
INSERT INTO events VALUES('pair','runtime','generation',8,'event-eight','{"sequence":8}',123,0);
INSERT INTO force_resumes VALUES('pair','command','["old"]','cleaning');
UPDATE counters SET spool_bytes=123;
ALTER TABLE force_resumes ADD COLUMN cleanup_started INTEGER DEFAULT 0;"#).unwrap();
    (registry, runtime)
}

fn rows(conn: &Connection, table: &str, columns: &str) -> Vec<Vec<Value>> {
    let mut query = conn
        .prepare(&format!("SELECT {columns} FROM {table}"))
        .unwrap();
    let count = query.column_count();
    query
        .query_map([], |row| (0..count).map(|i| row.get(i)).collect())
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap()
}

#[test]
fn upgrade_preserves_both_stores_and_does_not_reimport_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (registry, runtime) = legacy(root.path());
    let tables = ["runtimes", "commands", "events", "journal_identity"];
    let expected: Vec<_> = tables.iter().map(|t| rows(&runtime, t, "*")).collect();
    let db = Database::open(&root.path().join("tower.db")).unwrap();
    {
        let conn = db.lock().unwrap();
        for (table, expected) in tables.iter().zip(&expected) {
            assert_eq!(rows(&conn, table, "*"), *expected);
        }
        assert_eq!(
            rows(&conn, "force_resumes", "*"),
            rows(
                &runtime,
                "force_resumes",
                "namespace,command_id,targets,phase"
            )
        );
        assert_eq!(
            rows(&conn, "runtime_counters", "*"),
            rows(&runtime, "counters", "*")
        );
        assert_eq!(rows(&conn, "host", "*"), rows(&registry, "host", "*"));
        assert_eq!(
            rows(&conn, "counters", "*"),
            rows(&registry, "counters", "*")
        );
        conn.execute("UPDATE commands SET receipt_revision=5", [])
            .unwrap();
    }
    drop(db);
    let db = Database::open(&root.path().join("tower.db")).unwrap();
    assert_eq!(
        rows(&db.lock().unwrap(), "commands", "receipt_revision"),
        vec![vec![Value::Integer(5)]]
    );
    assert_eq!(
        rows(&runtime, "commands", "receipt_revision"),
        vec![vec![Value::Integer(4)]]
    );
    assert_eq!(
        std::fs::read(root.path().join("runtime/fingerprint.key")).unwrap(),
        [7; 32]
    );
}

#[test]
fn failed_copy_rolls_back_schema_and_version_and_can_be_retried() {
    let root = tempfile::tempdir().unwrap();
    let (registry, runtime) = legacy(root.path());
    runtime
        .execute_batch("PRAGMA foreign_keys=OFF; UPDATE commands SET runtime_id='missing';")
        .unwrap();
    assert!(Database::open(&root.path().join("tower.db")).is_err());
    assert_eq!(
        registry
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        registry
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='runtimes'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        rows(&registry, "counters WHERE name='registry'", "value"),
        vec![vec![Value::Integer(37)]]
    );
    runtime
        .execute("UPDATE commands SET runtime_id='runtime'", [])
        .unwrap();
    assert!(Database::open(&root.path().join("tower.db")).is_ok());
}

#[test]
fn upgrade_requires_matching_private_identity_and_supported_versions() {
    let root = tempfile::tempdir().unwrap();
    let (registry, runtime) = legacy(root.path());
    let path = root.path().join("tower.db");
    tower_pal::write_private(&root.path().join("runtime/journal.id"), b"other").unwrap();
    assert!(Database::open(&path).is_err());
    tower_pal::write_private(&root.path().join("runtime/journal.id"), b"journal_fixture").unwrap();
    runtime.pragma_update(None, "user_version", 4).unwrap();
    assert!(Database::open(&path).is_err());
    runtime.pragma_update(None, "user_version", 3).unwrap();
    std::fs::remove_file(root.path().join("runtime/fingerprint.key")).unwrap();
    assert!(Database::open(&path).is_err());
    assert_eq!(
        registry
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn missing_database_is_not_recreated_beside_an_existing_journal_identity() {
    let root = tempfile::tempdir().unwrap();
    tower_pal::private_dir(&root.path().join("runtime")).unwrap();
    tower_pal::write_private(&root.path().join("runtime/journal.id"), b"existing").unwrap();
    let path = root.path().join("tower.db");
    assert!(Database::open(&path).is_err());
    assert!(!path.exists());
}

#[test]
fn registry_only_upgrade_initializes_an_empty_journal_without_resetting_rules() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("tower.db");
    let old = Connection::open(&path).unwrap();
    old.execute_batch(REGISTRY).unwrap();
    old.execute_batch("PRAGMA user_version=1; DELETE FROM local_rules;")
        .unwrap();
    let db = Database::open(&path).unwrap();
    assert!(rows(&db.lock().unwrap(), "local_rules", "*").is_empty());
    assert!(rows(&db.lock().unwrap(), "runtimes", "*").is_empty());
    db.lock()
        .unwrap()
        .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
        .unwrap();
    assert!(Database::open(&path).is_err());
}
