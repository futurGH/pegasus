ALTER TABLE actors ADD COLUMN totp_secret BLOB;
ALTER TABLE actors ADD COLUMN totp_verified_at INTEGER;

ALTER TABLE actors ADD COLUMN email_2fa_enabled INTEGER DEFAULT 0;

CREATE TABLE IF NOT EXISTS backup_codes (
  id INTEGER PRIMARY KEY,
  did TEXT NOT NULL,
  code_hash TEXT NOT NULL,
  used_at INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (did) REFERENCES actors(did) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS backup_codes_did_idx ON backup_codes(did);

CREATE TABLE IF NOT EXISTS pending_2fa (
  id INTEGER PRIMARY KEY,
  session_token TEXT NOT NULL UNIQUE,
  did TEXT NOT NULL,
  password_verified_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  email_code TEXT,
  email_code_expires_at INTEGER,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS pending_2fa_session_idx ON pending_2fa(session_token);
CREATE INDEX IF NOT EXISTS pending_2fa_expires_idx ON pending_2fa(expires_at);

CREATE TRIGGER IF NOT EXISTS cleanup_expired_pending_2fa
AFTER INSERT ON pending_2fa
BEGIN
  DELETE FROM pending_2fa WHERE expires_at < unixepoch() * 1000;
END;
