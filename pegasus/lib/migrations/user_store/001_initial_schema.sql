CREATE TABLE IF NOT EXISTS mst (
  cid TEXT NOT NULL PRIMARY KEY,
  data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS repo_commit (
  id INTEGER PRIMARY KEY CHECK (id = 0),
  cid TEXT NOT NULL,
  data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
  path TEXT NOT NULL PRIMARY KEY,
  cid TEXT NOT NULL,
  since TEXT NOT NULL,
  data BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS records_cid_idx ON records (cid);
CREATE INDEX IF NOT EXISTS records_since_idx ON records (since);

CREATE TABLE IF NOT EXISTS blobs (
  cid TEXT PRIMARY KEY,
  mimetype TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs_records (
  record_path TEXT NOT NULL REFERENCES records(path),
  blob_cid TEXT NOT NULL,
  PRIMARY KEY (record_path, blob_cid)
);

CREATE INDEX IF NOT EXISTS blobs_records_blob_cid_idx ON blobs_records (blob_cid);
