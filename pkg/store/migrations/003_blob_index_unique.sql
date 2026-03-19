CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs_ns_height_index ON blobs(namespace, height, blob_index);
DROP INDEX IF EXISTS idx_blobs_ns_height;
