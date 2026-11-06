CREATE TABLE host (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    host_id TEXT NOT NULL,
    standalone_node TEXT NOT NULL,
    node_name TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('standalone','allgather'))
);
CREATE TABLE counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO counters VALUES('registry',0);
INSERT INTO counters VALUES('checkouts',0);
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
    recovery_pending INTEGER NOT NULL DEFAULT 0,
    observed_status TEXT,
    stop_requested INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX native_identity ON agents(harness,native_id) WHERE native_id IS NOT NULL;
CREATE UNIQUE INDEX process_identity ON agents(pid,pid_start) WHERE pid IS NOT NULL AND pid_start IS NOT NULL AND ownership='registered' AND coalesce(json_extract(public_json,'$.shared_process'),0)=0;
CREATE INDEX process_observation ON agents(pid,pid_start);
CREATE INDEX agent_token_hash ON agents(token_hash);
CREATE TABLE local_rules(id TEXT PRIMARY KEY, position INTEGER NOT NULL, data TEXT NOT NULL);
CREATE TABLE allgather_rules(id TEXT PRIMARY KEY, position INTEGER NOT NULL, data TEXT NOT NULL);
CREATE TABLE policy_snapshot(singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision TEXT NOT NULL, payload TEXT NOT NULL, delta_hash BLOB);
CREATE TABLE tokens(name TEXT PRIMARY KEY, data TEXT NOT NULL, hash BLOB NOT NULL);
CREATE INDEX named_token_hash ON tokens(hash);
INSERT INTO local_rules VALUES('default-human',0,'{"id":"default-human","from":"human","to":"*","action":"allow","kind":"message","created_by":"default","created_at":0}');
INSERT INTO local_rules VALUES('default-ask',1,'{"id":"default-ask","from":"*","to":"*","action":"ask","kind":"message","created_by":"default","created_at":0}');
CREATE TABLE managed_sessions (
    id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    protocol TEXT NOT NULL,
    binding TEXT NOT NULL,
    launch TEXT,
    auto_resume INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE delivery(id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE, data TEXT NOT NULL);
CREATE TABLE approvals(id TEXT PRIMARY KEY, data TEXT NOT NULL, body_hash BLOB NOT NULL, consumed INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE INDEX approvals_created ON approvals(created_at);
CREATE INDEX approvals_expiry ON approvals(json_extract(data,'$.expires_at')) WHERE json_extract(data,'$.state')='pending';
CREATE INDEX approvals_body_hash ON approvals(body_hash);
CREATE TABLE repository_catalog(singleton INTEGER PRIMARY KEY CHECK(singleton=1), pairing TEXT NOT NULL, revision INTEGER, payload TEXT, delta_hash BLOB);
CREATE TABLE checkouts(id TEXT PRIMARY KEY, binding TEXT NOT NULL, record TEXT NOT NULL, identity TEXT NOT NULL UNIQUE, association_manual INTEGER NOT NULL DEFAULT 0);
CREATE TABLE vcs_operations(id TEXT PRIMARY KEY, actor TEXT NOT NULL, state TEXT NOT NULL, prepared TEXT NOT NULL, record TEXT NOT NULL, released INTEGER NOT NULL DEFAULT 0);
CREATE INDEX vcs_operation_state ON vcs_operations(state);
