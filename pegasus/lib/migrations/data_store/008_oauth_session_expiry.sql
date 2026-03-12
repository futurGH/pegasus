ALTER TABLE oauth_tokens
ADD COLUMN session_expires_at INTEGER NOT NULL DEFAULT 0;

UPDATE oauth_tokens
SET session_expires_at =
  CASE
    WHEN typeof(created_at) = 'integer' THEN created_at + 31536000000
    WHEN typeof(created_at) = 'text' THEN (unixepoch(created_at) * 1000) + 31536000000
    ELSE (unixepoch() * 1000) + 31536000000
  END
WHERE session_expires_at = 0;

CREATE INDEX IF NOT EXISTS oauth_tokens_session_expires_idx
ON oauth_tokens(session_expires_at);

DROP TRIGGER IF EXISTS cleanup_expired_oauth_tokens;

CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_tokens
AFTER INSERT ON oauth_tokens
BEGIN
  DELETE FROM oauth_tokens WHERE session_expires_at < unixepoch() * 1000;
END;
