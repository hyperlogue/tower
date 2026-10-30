CREATE TABLE host (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    host_id TEXT NOT NULL,
    standalone_node TEXT NOT NULL,
    node_name TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('standalone','allgather'))
);
CREATE TABLE counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO counters VALUES('registry',0);
CREATE TABLE agents (
    id TEXT PRIMARY KEY,
    harness TEXT NOT NULL,
    native_id TEXT,
    pid INTEGER,
    pid_start TEXT,
    ownership TEXT NOT NULL,
    public_json TEXT NOT NULL,
    token_hash BLOB NOT NULL,
    source_seq TEXT NOT NULL DEFAULT '0',
    ended_at INTEGER,
    recovery_pending INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX native_identity ON agents(harness,native_id) WHERE native_id IS NOT NULL;
CREATE UNIQUE INDEX process_identity ON agents(pid,pid_start) WHERE pid IS NOT NULL AND pid_start IS NOT NULL;
CREATE TABLE local_rules(id TEXT PRIMARY KEY, position INTEGER NOT NULL, data TEXT NOT NULL);
CREATE TABLE allgather_rules(id TEXT PRIMARY KEY, position INTEGER NOT NULL, data TEXT NOT NULL);
CREATE TABLE policy_snapshot(singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision TEXT NOT NULL, payload TEXT NOT NULL);
CREATE TABLE tokens(name TEXT PRIMARY KEY, data TEXT NOT NULL, hash BLOB NOT NULL);
INSERT INTO local_rules VALUES('default-human',0,'{"id":"default-human","from":"human","to":"*","action":"allow","created_by":"default","created_at":0}');
INSERT INTO local_rules VALUES('default-ask',1,'{"id":"default-ask","from":"*","to":"*","action":"ask","created_by":"default","created_at":0}');
