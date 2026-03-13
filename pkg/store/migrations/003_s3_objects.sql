CREATE TABLE IF NOT EXISTS s3_buckets (
    name          TEXT PRIMARY KEY,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS s3_objects (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    size          INTEGER NOT NULL,
    etag          TEXT NOT NULL,
    content_type  TEXT,
    last_modified INTEGER NOT NULL,
    height        INTEGER NOT NULL,
    namespace     TEXT NOT NULL,
    blob_count    INTEGER NOT NULL,
    commitments   TEXT NOT NULL,
    data          BLOB,
    UNIQUE(bucket, key),
    FOREIGN KEY (bucket) REFERENCES s3_buckets(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_s3_objects_bucket ON s3_objects(bucket);
CREATE INDEX IF NOT EXISTS idx_s3_objects_bucket_key ON s3_objects(bucket, key);
