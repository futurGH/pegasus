-- I forgor
CREATE TABLE IF NOT EXISTS blobs_records_2 (
  record_path TEXT NOT NULL REFERENCES records(path) ON DELETE CASCADE,
  blob_cid TEXT NOT NULL,
  PRIMARY KEY (record_path, blob_cid)
);

INSERT INTO blobs_records_2 SELECT * FROM blobs_records;

ALTER TABLE blobs_records RENAME TO blobs_records_old;
ALTER TABLE blobs_records_2 RENAME TO blobs_records;
DROP TABLE blobs_records_old;
