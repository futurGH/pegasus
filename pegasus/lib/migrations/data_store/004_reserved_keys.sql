CREATE TABLE IF NOT EXISTS reserved_keys (
  key_did TEXT PRIMARY KEY,
  did TEXT,
  private_key TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS reserved_keys_did_idx ON reserved_keys(did);
