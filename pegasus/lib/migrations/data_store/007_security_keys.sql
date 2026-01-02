CREATE TABLE IF NOT EXISTS security_keys (
  id INTEGER PRIMARY KEY,
  did TEXT NOT NULL,
  name TEXT NOT NULL DEFAULT 'Security Key',
  secret BLOB NOT NULL,
  counter INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  last_used_at INTEGER,
  verified_at INTEGER,
  FOREIGN KEY (did) REFERENCES actors(did) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS security_keys_did_idx ON security_keys(did);
