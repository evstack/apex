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

func TestLoadRejectsReservedNamespace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"
  namespaces:
    - "0000000000000000000000000000000000000000000000000000000001"

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
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubmissionValidationRejectsIncompleteConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		yaml      string
		wantError string
	}{
		{
			name: "missing app_grpc_addr",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: ""
  chain_id: "mychain"
`,
			wantError: "submission.app_grpc_addr is required",
		},
		{
			name: "whitespace app_grpc_addr",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: "   "
  chain_id: "mychain"
  signer_key: "signer.key"
`,
			wantError: "submission.app_grpc_addr is required",
		},
		{
			name: "missing chain_id",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: ""
`,
			wantError: "submission.chain_id is required",
		},
		{
			name: "whitespace chain_id",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "   "
  signer_key: "signer.key"
`,
			wantError: "submission.chain_id is required",
		},
		{
			name: "missing signer_key",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: ""
`,
			wantError: "submission.signer_key is required",
		},
		{
			name: "whitespace signer_key",
			yaml: `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: "   "
`,
			wantError: "submission.signer_key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			content := tt.yaml + `
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
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantError)
			}
		})
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
  app_grpc_addr: "  localhost:9090  "
  chain_id: "  mychain  "
  signer_key: "  submit.key  "

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
	if cfg.Submission.SignerKey != keyPath {
		t.Fatalf("signer key path = %q, want %q", cfg.Submission.SignerKey, keyPath)
	}
	if cfg.Submission.CelestiaAppGRPCAddr != "localhost:9090" {
		t.Fatalf("app grpc addr = %q, want %q", cfg.Submission.CelestiaAppGRPCAddr, "localhost:9090")
	}
	if cfg.Submission.ChainID != "mychain" {
		t.Fatalf("chain id = %q, want %q", cfg.Submission.ChainID, "mychain")
	}
}

func TestSubmissionRejectsMissingSignerKeyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: "missing.key"

log:
  level: "info"
  format: "json"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected missing signer key file error, got nil")
	}
	if !strings.Contains(err.Error(), "submission signer key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateIncludesS3Section(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := Generate(path); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "\ns3:\n") {
		t.Fatalf("generated config missing s3 section:\n%s", content)
	}
	if !strings.Contains(content, `listen_addr: ":8333"`) {
		t.Fatalf("generated config missing s3 listen addr default:\n%s", content)
	}
}

func TestLoadRejectsS3EnabledWithNonSQLiteStorage(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"

storage:
  type: "s3"
  s3:
    bucket: "bucket"
    region: "us-east-1"

s3:
  enabled: true
  listen_addr: ":8333"

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
	if !strings.Contains(err.Error(), `s3.enabled requires storage.type to be "sqlite"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsS3SubmissionWithoutNamespace(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	keyPath := filepath.Join(t.TempDir(), "submit.key")
	if err := os.WriteFile(keyPath, []byte("00112233"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	content := `
data_source:
  type: "app"
  celestia_app_grpc_addr: "localhost:9090"

storage:
  type: "sqlite"
  db_path: "apex.db"

submission:
  enabled: true
  app_grpc_addr: "localhost:9090"
  chain_id: "mychain"
  signer_key: "` + keyPath + `"

s3:
  enabled: true
  listen_addr: ":8333"

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
	if !strings.Contains(err.Error(), "s3.namespace is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsInvalidS3Namespace(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"

storage:
  type: "sqlite"
  db_path: "apex.db"

s3:
  enabled: true
  listen_addr: ":8333"
  namespace: "not-a-namespace"

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
	if !strings.Contains(err.Error(), "s3.namespace is invalid") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsPartialS3Credentials(t *testing.T) {
	// Cannot run in parallel because t.Setenv modifies env vars.
	t.Setenv("APEX_S3_ACCESS_KEY_ID", "app-key")
	// APEX_S3_SECRET_ACCESS_KEY intentionally not set to test partial-credentials rejection.

	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"

storage:
  type: "sqlite"
  db_path: "apex.db"

s3:
  enabled: true
  listen_addr: ":8333"

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
	if !strings.Contains(err.Error(), "s3.access_key_id and s3.secret_access_key must be provided together") { //nolint:staticcheck // partial message check
		t.Fatalf("unexpected error: %v", err)
	}
}
