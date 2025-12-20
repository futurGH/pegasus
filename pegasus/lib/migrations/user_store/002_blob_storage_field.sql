ALTER TABLE blobs ADD COLUMN storage TEXT NOT NULL DEFAULT 'local';

CREATE INDEX IF NOT EXISTS blobs_storage_idx ON blobs (storage);
