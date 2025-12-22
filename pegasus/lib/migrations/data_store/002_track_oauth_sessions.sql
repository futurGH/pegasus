ALTER TABLE oauth_tokens ADD COLUMN created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE oauth_tokens ADD COLUMN last_refreshed_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE oauth_tokens ADD COLUMN last_ip TEXT NOT NULL DEFAULT '';
ALTER TABLE oauth_tokens ADD COLUMN last_user_agent TEXT;

ALTER TABLE oauth_codes ADD COLUMN authorized_ip TEXT;
ALTER TABLE oauth_codes ADD COLUMN authorized_user_agent TEXT;

CREATE INDEX IF NOT EXISTS oauth_tokens_did_idx ON oauth_tokens(did);
