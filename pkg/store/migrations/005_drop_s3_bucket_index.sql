-- idx_s3_objects_bucket is a strict prefix of idx_s3_objects_bucket_key(bucket, key).
-- SQLite can use the composite index for bucket-only lookups, so the
-- single-column index is redundant write overhead.
DROP INDEX IF EXISTS idx_s3_objects_bucket;
