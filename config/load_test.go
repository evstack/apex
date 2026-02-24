package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadProfilingEnabledRequiresListenAddr(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"

storage:
  type: "sqlite"
  db_path: "apex.db"

rpc:
  listen_addr: ":8080"
  grpc_listen_addr: ":9090"

sync:
  start_height: 1
  batch_size: 64
  concurrency: 4

subscription:
  buffer_size: 64

metrics:
  enabled: true
  listen_addr: ":9091"

profiling:
  enabled: true
  listen_addr: ""

log:
  level: "info"
  format: "json"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "profiling.listen_addr is required when profiling is enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadAppDBBackfillRequiresDBPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
data_source:
  type: "app"
  celestia_app_grpc_addr: "localhost:9090"
  backfill_source: "db"

storage:
  type: "sqlite"
  db_path: "apex.db"

rpc:
  listen_addr: ":8080"
  grpc_listen_addr: ":9090"

sync:
  start_height: 1
  batch_size: 64
  concurrency: 4

subscription:
  buffer_size: 64

metrics:
  enabled: true
  listen_addr: ":9091"

profiling:
  enabled: false
  listen_addr: "127.0.0.1:6061"

log:
  level: "info"
  format: "json"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "celestia_app_db_path is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}
