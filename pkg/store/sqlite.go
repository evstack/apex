package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/evstack/apex/pkg/types"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

// SQLiteStore implements Store using modernc.org/sqlite (CGo-free).
// It maintains separate read and write connection pools to the same database.
// The writer is limited to a single connection (WAL single-writer constraint),
// while the reader pool allows concurrent API reads.
type SQLiteStore struct {
	writer *sql.DB
	reader *sql.DB
}

// maxReadConns is the upper bound for the read connection pool.
// Beyond ~8 readers, SQLite WAL contention outweighs parallelism gains.
const maxReadConns = 8

// Open creates or opens a SQLite database at the given path.
// The read pool is sized to min(NumCPU, 8).
// The database is configured with WAL journal mode and a 5-second busy timeout.
func Open(path string) (*SQLiteStore, error) {
	poolSize := runtime.NumCPU()
	if poolSize > maxReadConns {
		poolSize = maxReadConns
	}

	writer, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite writer: %w", err)
	}
	writer.SetMaxOpenConns(1)

	if err := configureSQLite(writer); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("configure writer: %w", err)
	}

	reader, err := sql.Open("sqlite", path)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}
	reader.SetMaxOpenConns(poolSize)

	if err := configureSQLite(reader); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("configure reader: %w", err)
	}

	s := &SQLiteStore{writer: writer, reader: reader}
	if err := s.migrate(); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func configureSQLite(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return fmt.Errorf("set busy_timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("set foreign_keys: %w", err)
	}
	return nil
}

func (s *SQLiteStore) migrate() error {
	var version int
	if err := s.writer.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}

	if version >= 1 {
		return nil
	}

	ddl, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}

	tx, err := s.writer.Begin()
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(string(ddl)); err != nil {
		return fmt.Errorf("exec migration: %w", err)
	}
	if _, err := tx.Exec("PRAGMA user_version = 1"); err != nil {
		return fmt.Errorf("set user_version: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) PutBlobs(ctx context.Context, blobs []types.Blob) error {
	if len(blobs) == 0 {
		return nil
	}

	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO blobs (height, namespace, commitment, data, share_version, signer, blob_index)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare insert blob: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	for i := range blobs {
		b := &blobs[i]
		if _, err := stmt.ExecContext(ctx,
			b.Height, b.Namespace[:], b.Commitment, b.Data, b.ShareVersion, b.Signer, b.Index,
		); err != nil {
			return fmt.Errorf("insert blob at height %d index %d: %w", b.Height, b.Index, err)
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetBlob(ctx context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error) {
	row := s.reader.QueryRowContext(ctx,
		`SELECT height, namespace, commitment, data, share_version, signer, blob_index
		 FROM blobs WHERE namespace = ? AND height = ? AND blob_index = ?`,
		ns[:], height, index)

	return scanBlob(row)
}

func (s *SQLiteStore) GetBlobs(ctx context.Context, ns types.Namespace, startHeight, endHeight uint64) ([]types.Blob, error) {
	rows, err := s.reader.QueryContext(ctx,
		`SELECT height, namespace, commitment, data, share_version, signer, blob_index
		 FROM blobs WHERE namespace = ? AND height >= ? AND height <= ?
		 ORDER BY height, blob_index`,
		ns[:], startHeight, endHeight)
	if err != nil {
		return nil, fmt.Errorf("query blobs: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var blobs []types.Blob
	for rows.Next() {
		b, err := scanBlobRow(rows)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, b)
	}
	return blobs, rows.Err()
}

func (s *SQLiteStore) PutHeader(ctx context.Context, header *types.Header) error {
	_, err := s.writer.ExecContext(ctx,
		`INSERT OR IGNORE INTO headers (height, hash, data_hash, time_ns, raw_header)
		 VALUES (?, ?, ?, ?, ?)`,
		header.Height, header.Hash, header.DataHash, header.Time.UnixNano(), header.RawHeader)
	if err != nil {
		return fmt.Errorf("insert header at height %d: %w", header.Height, err)
	}
	return nil
}

func (s *SQLiteStore) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	var h types.Header
	var timeNs int64
	err := s.reader.QueryRowContext(ctx,
		`SELECT height, hash, data_hash, time_ns, raw_header FROM headers WHERE height = ?`,
		height).Scan(&h.Height, &h.Hash, &h.DataHash, &timeNs, &h.RawHeader)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query header at height %d: %w", height, err)
	}
	h.Time = time.Unix(0, timeNs)
	return &h, nil
}

func (s *SQLiteStore) PutNamespace(ctx context.Context, ns types.Namespace) error {
	_, err := s.writer.ExecContext(ctx,
		`INSERT OR IGNORE INTO namespaces (namespace) VALUES (?)`, ns[:])
	if err != nil {
		return fmt.Errorf("insert namespace: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetNamespaces(ctx context.Context) ([]types.Namespace, error) {
	rows, err := s.reader.QueryContext(ctx, `SELECT namespace FROM namespaces`)
	if err != nil {
		return nil, fmt.Errorf("query namespaces: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var namespaces []types.Namespace
	for rows.Next() {
		var nsBytes []byte
		if err := rows.Scan(&nsBytes); err != nil {
			return nil, fmt.Errorf("scan namespace: %w", err)
		}
		if len(nsBytes) != types.NamespaceSize {
			return nil, fmt.Errorf("invalid namespace size: got %d, want %d", len(nsBytes), types.NamespaceSize)
		}
		var ns types.Namespace
		copy(ns[:], nsBytes)
		namespaces = append(namespaces, ns)
	}
	return namespaces, rows.Err()
}

func (s *SQLiteStore) GetSyncState(ctx context.Context) (*types.SyncStatus, error) {
	var state int
	var latestHeight, networkHeight uint64
	err := s.reader.QueryRowContext(ctx,
		`SELECT state, latest_height, network_height FROM sync_state WHERE id = 1`).
		Scan(&state, &latestHeight, &networkHeight)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query sync_state: %w", err)
	}
	return &types.SyncStatus{
		State:         types.SyncState(state),
		LatestHeight:  latestHeight,
		NetworkHeight: networkHeight,
	}, nil
}

func (s *SQLiteStore) SetSyncState(ctx context.Context, status types.SyncStatus) error {
	_, err := s.writer.ExecContext(ctx,
		`INSERT INTO sync_state (id, state, latest_height, network_height, updated_at)
		 VALUES (1, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   state = excluded.state,
		   latest_height = excluded.latest_height,
		   network_height = excluded.network_height,
		   updated_at = excluded.updated_at`,
		int(status.State), status.LatestHeight, status.NetworkHeight, time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("upsert sync_state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Close() error {
	rErr := s.reader.Close()
	wErr := s.writer.Close()
	if rErr != nil {
		return rErr
	}
	return wErr
}

// scanBlob scans a single blob from a *sql.Row.
func scanBlob(row *sql.Row) (*types.Blob, error) {
	var b types.Blob
	var nsBytes []byte
	err := row.Scan(&b.Height, &nsBytes, &b.Commitment, &b.Data, &b.ShareVersion, &b.Signer, &b.Index)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan blob: %w", err)
	}
	copy(b.Namespace[:], nsBytes)
	return &b, nil
}

// scanBlobRow scans a single blob from *sql.Rows.
func scanBlobRow(rows *sql.Rows) (types.Blob, error) {
	var b types.Blob
	var nsBytes []byte
	err := rows.Scan(&b.Height, &nsBytes, &b.Commitment, &b.Data, &b.ShareVersion, &b.Signer, &b.Index)
	if err != nil {
		return types.Blob{}, fmt.Errorf("scan blob row: %w", err)
	}
	copy(b.Namespace[:], nsBytes)
	return b, nil
}
