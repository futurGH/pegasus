CREATE INDEX IF NOT EXISTS blobs_records_blob_cid_idx
ON blobs_records (blob_cid, record_path);
