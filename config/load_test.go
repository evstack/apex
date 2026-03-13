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

func TestSubmissionRequiresAppGRPCHost(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
submission:
  enabled: true
  app_grpc_addr: ""
  chain_id: "mychain"

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
	if !strings.Contains(err.Error(), "submission.app_grpc_addr is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubmissionRequiresChainID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: ""

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
	if !strings.Contains(err.Error(), "submission.chain_id is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubmissionRequiresSignerKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: ""

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
	if !strings.Contains(err.Error(), "submission.signer_key is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubmissionLoadsSignerKeyFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "submit.key")
	if err := os.WriteFile(keyPath, []byte("00112233\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	path := filepath.Join(dir, "config.yaml")
	content := `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: "submit.key"

log:
  level: "info"
  format: "json"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Submission.SignerPrivateKey != "00112233" {
		t.Fatalf("signer private key = %q", cfg.Submission.SignerPrivateKey)
	}
}
