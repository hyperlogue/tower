CREATE TABLE managed_sessions (
    id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    protocol TEXT NOT NULL,
    binding TEXT NOT NULL,
    launch TEXT
);
INSERT INTO managed_sessions(id,protocol,binding,launch)
SELECT a.id,'terminal',json_object('name',json_extract(a.public_json,'$.pool_session')),CASE WHEN o.data IS NOT NULL THEN json_object('spec',json(o.data),'headless',json('false')) END
FROM agents a LEFT JOIN owned_launches o ON a.id=o.id
WHERE a.ownership='owned' AND json_type(a.public_json,'$.pool_session')='text';
UPDATE agents SET public_json=json_set(public_json,'$.protocol','terminal','$.caps.snapshot',json('true'))
WHERE id IN (SELECT id FROM managed_sessions);
DROP TABLE owned_launches;
