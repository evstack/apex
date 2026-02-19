package config

import (
	"fmt"

	"github.com/evstack/apex/pkg/types"
)

// Config is the top-level configuration for the Apex indexer.
type Config struct {
	DataSource   DataSourceConfig   `yaml:"data_source"`
	Storage      StorageConfig      `yaml:"storage"`
	RPC          RPCConfig          `yaml:"rpc"`
	Sync         SyncConfig         `yaml:"sync"`
	Subscription SubscriptionConfig `yaml:"subscription"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Profiling    ProfilingConfig    `yaml:"profiling"`
	Log          LogConfig          `yaml:"log"`
}

// DataSourceConfig configures the Celestia data source.
// Type selects the backend: "node" (default) uses a Celestia DA node,
// "app" uses a celestia-app consensus node via CometBFT RPC.
type DataSourceConfig struct {
	Type            string   `yaml:"type"` // "node" (default) or "app"
	CelestiaNodeURL string   `yaml:"celestia_node_url"`
	CelestiaAppURL  string   `yaml:"celestia_app_url"`
	AuthToken       string   `yaml:"-"` // populated only via APEX_AUTH_TOKEN env var
	Namespaces      []string `yaml:"namespaces"`
}

// StorageConfig configures the persistence backend.
// Type selects the backend: "sqlite" (default) uses a local SQLite file,
// "s3" uses an S3-compatible object store.
type StorageConfig struct {
	Type   string    `yaml:"type"`    // "sqlite" (default) or "s3"
	DBPath string    `yaml:"db_path"` // SQLite path (used when type=sqlite)
	S3     *S3Config `yaml:"s3"`      // S3 settings (used when type=s3)
}

// S3Config configures an S3-compatible object store backend.
// Credentials are resolved via standard AWS SDK mechanisms
// (env vars, IAM role, shared credentials file).
type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Prefix    string `yaml:"prefix"`
	Region    string `yaml:"region"`
	Endpoint  string `yaml:"endpoint"`   // custom endpoint for MinIO, R2, Spaces
	ChunkSize int    `yaml:"chunk_size"` // heights per S3 object, default 64
}

// RPCConfig configures the API servers.
type RPCConfig struct {
	ListenAddr     string `yaml:"listen_addr"`
	GRPCListenAddr string `yaml:"grpc_listen_addr"`
}

// SyncConfig configures the sync coordinator.
type SyncConfig struct {
	StartHeight uint64 `yaml:"start_height"`
	BatchSize   int    `yaml:"batch_size"`
	Concurrency int    `yaml:"concurrency"`
}

// SubscriptionConfig configures API event subscriptions.
type SubscriptionConfig struct {
	BufferSize int `yaml:"buffer_size"`
}

// MetricsConfig configures Prometheus metrics.
type MetricsConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
}

// ProfilingConfig configures pprof profiling endpoints.
type ProfilingConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
}

// LogConfig configures logging.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataSource: DataSourceConfig{
			Type:            "node",
			CelestiaNodeURL: "http://localhost:26658",
		},
		Storage: StorageConfig{
			Type:   "sqlite",
			DBPath: "apex.db",
		},
		RPC: RPCConfig{
			ListenAddr:     ":8080",
			GRPCListenAddr: ":9090",
		},
		Sync: SyncConfig{
			BatchSize:   64,
			Concurrency: 4,
		},
		Subscription: SubscriptionConfig{
			BufferSize: 64,
		},
		Metrics: MetricsConfig{
			Enabled:    true,
			ListenAddr: ":9091",
		},
		Profiling: ProfilingConfig{
			Enabled:    false,
			ListenAddr: "127.0.0.1:6061",
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// ParsedNamespaces converts hex namespace strings into typed Namespaces.
func (c *Config) ParsedNamespaces() ([]types.Namespace, error) {
	namespaces := make([]types.Namespace, 0, len(c.DataSource.Namespaces))
	for _, hex := range c.DataSource.Namespaces {
		ns, err := types.NamespaceFromHex(hex)
		if err != nil {
			return nil, fmt.Errorf("invalid namespace %q: %w", hex, err)
		}
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}
