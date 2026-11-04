CREATE TABLE repository_catalog(singleton INTEGER PRIMARY KEY CHECK(singleton=1),pairing TEXT NOT NULL,revision INTEGER,payload TEXT,delta_hash BLOB);
CREATE TABLE checkouts(id TEXT PRIMARY KEY,binding TEXT NOT NULL,record TEXT NOT NULL,identity TEXT NOT NULL UNIQUE,association_manual INTEGER NOT NULL DEFAULT 0);
CREATE TABLE vcs_operations(id TEXT PRIMARY KEY,actor TEXT NOT NULL,checkout_id TEXT,state TEXT NOT NULL,prepared TEXT NOT NULL,record TEXT NOT NULL,released INTEGER NOT NULL DEFAULT 0);
CREATE INDEX vcs_operation_state ON vcs_operations(state);
CREATE TABLE session_checkouts(session_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,checkout_id TEXT NOT NULL REFERENCES checkouts(id));
INSERT INTO counters VALUES('checkouts',0);
