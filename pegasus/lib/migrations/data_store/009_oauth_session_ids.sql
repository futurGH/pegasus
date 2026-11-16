ALTER TABLE oauth_tokens ADD COLUMN session_id TEXT;

UPDATE oauth_tokens
SET session_id = 'ses-' || lower(hex(randomblob(16)));

CREATE UNIQUE INDEX oauth_tokens_session_id_idx ON oauth_tokens(session_id);
