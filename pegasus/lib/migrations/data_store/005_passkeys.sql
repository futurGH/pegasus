CREATE TABLE IF NOT EXISTS passkeys (
  id INTEGER PRIMARY KEY,
  did TEXT NOT NULL,
  credential_id TEXT NOT NULL UNIQUE,
  public_key BLOB NOT NULL,
  sign_count INTEGER NOT NULL DEFAULT 0,
  name TEXT NOT NULL DEFAULT 'Passkey',
  created_at INTEGER NOT NULL,
  last_used_at INTEGER,
  FOREIGN KEY (did) REFERENCES actors(did) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS passkeys_did_idx ON passkeys(did);
CREATE INDEX IF NOT EXISTS passkeys_credential_id_idx ON passkeys(credential_id);

CREATE TABLE IF NOT EXISTS passkey_challenges (
  challenge TEXT PRIMARY KEY,
  did TEXT,
  challenge_type TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS passkey_challenges_expires_idx ON passkey_challenges(expires_at);

CREATE TRIGGER IF NOT EXISTS cleanup_expired_passkey_challenges
AFTER INSERT ON passkey_challenges
BEGIN
  DELETE FROM passkey_challenges WHERE expires_at < unixepoch() * 1000;
END;
