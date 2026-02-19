package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultConfigProfilingDefaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	if cfg.Profiling.Enabled {
		t.Fatal("profiling.enabled default = true, want false")
	}
	if cfg.Profiling.ListenAddr != "127.0.0.1:6061" {
		t.Fatalf("profiling.listen_addr default = %q, want %q", cfg.Profiling.ListenAddr, "127.0.0.1:6061")
	}
}

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
