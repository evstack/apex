CREATE TABLE IF NOT EXISTS sync_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    state          INTEGER NOT NULL DEFAULT 0,
    latest_height  INTEGER NOT NULL DEFAULT 0,
    network_height INTEGER NOT NULL DEFAULT 0,
    updated_at     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS headers (
    height     INTEGER PRIMARY KEY,
    hash       BLOB NOT NULL,
    data_hash  BLOB NOT NULL,
    time_ns    INTEGER NOT NULL,
    raw_header BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    height        INTEGER NOT NULL,
    namespace     BLOB NOT NULL,
    commitment    BLOB NOT NULL,
    data          BLOB NOT NULL,
    share_version INTEGER NOT NULL DEFAULT 0,
    signer        BLOB,
    blob_index    INTEGER NOT NULL,
    UNIQUE(height, namespace, commitment)
);

CREATE INDEX IF NOT EXISTS idx_blobs_ns_height ON blobs(namespace, height);

CREATE TABLE IF NOT EXISTS namespaces (
    namespace BLOB PRIMARY KEY
);
