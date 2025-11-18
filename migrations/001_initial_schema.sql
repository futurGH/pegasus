CREATE TABLE IF NOT EXISTS actors (
  id INTEGER PRIMARY KEY,
  did TEXT NOT NULL UNIQUE,
  handle TEXT NOT NULL UNIQUE,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  signing_key TEXT NOT NULL,
  preferences TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  deactivated_at INTEGER
);

CREATE INDEX IF NOT EXISTS actors_did_idx ON actors (did);
CREATE INDEX IF NOT EXISTS actors_handle_idx ON actors (handle);
CREATE INDEX IF NOT EXISTS actors_email_idx ON actors (email);

CREATE TABLE IF NOT EXISTS invite_codes (
  code TEXT PRIMARY KEY,
  did TEXT NOT NULL,
  remaining INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS firehose (
  seq INTEGER PRIMARY KEY,
  time INTEGER NOT NULL,
  t TEXT NOT NULL,
  data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS revoked_tokens (
  did TEXT NOT NULL,
  jti TEXT NOT NULL,
  revoked_at INTEGER NOT NULL,
  PRIMARY KEY (did, jti)
);

CREATE TABLE IF NOT EXISTS oauth_requests (
  request_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL,
  request_data TEXT NOT NULL,
  dpop_jkt TEXT,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS oauth_requests_expires_idx ON oauth_requests(expires_at);

CREATE TABLE IF NOT EXISTS oauth_codes (
  code TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES oauth_requests(request_id) ON DELETE CASCADE,
  authorized_by TEXT,
  authorized_at INTEGER,
  expires_at INTEGER NOT NULL,
  used BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS oauth_codes_expires_idx ON oauth_codes(expires_at);

CREATE TABLE IF NOT EXISTS oauth_tokens (
  refresh_token TEXT UNIQUE NOT NULL,
  client_id TEXT NOT NULL,
  did TEXT NOT NULL,
  dpop_jkt TEXT,
  scope TEXT NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS oauth_tokens_refresh_idx ON oauth_tokens(refresh_token);

CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_requests
AFTER INSERT ON oauth_requests
BEGIN
  DELETE FROM oauth_requests WHERE expires_at < unixepoch() * 1000;
END;

CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_codes
AFTER INSERT ON oauth_codes
BEGIN
  DELETE FROM oauth_codes WHERE expires_at < unixepoch() * 1000 OR used = 1;
END;

CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_tokens
AFTER INSERT ON oauth_tokens
BEGIN
  DELETE FROM oauth_tokens WHERE expires_at < unixepoch() * 1000;
END;
