package config

import (
	"fmt"

	"github.com/evstack/apex/pkg/types"
)

// Config is the top-level configuration for the Apex indexer.
type Config struct {
	DataSource DataSourceConfig `yaml:"data_source"`
	Storage    StorageConfig    `yaml:"storage"`
	RPC        RPCConfig        `yaml:"rpc"`
	Sync       SyncConfig       `yaml:"sync"`
	Log        LogConfig        `yaml:"log"`
}

// DataSourceConfig configures the Celestia node connection.
type DataSourceConfig struct {
	CelestiaNodeURL string   `yaml:"celestia_node_url"`
	AuthToken       string   `yaml:"-"` // populated only via APEX_AUTH_TOKEN env var
	Namespaces      []string `yaml:"namespaces"`
}

// StorageConfig configures the SQLite database.
type StorageConfig struct {
	DBPath string `yaml:"db_path"`
}

// RPCConfig configures the HTTP API server.
type RPCConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// SyncConfig configures the sync coordinator.
type SyncConfig struct {
	StartHeight uint64 `yaml:"start_height"`
	BatchSize   int    `yaml:"batch_size"`
	Concurrency int    `yaml:"concurrency"`
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
			CelestiaNodeURL: "http://localhost:26658",
		},
		Storage: StorageConfig{
			DBPath: "apex.db",
		},
		RPC: RPCConfig{
			ListenAddr: ":8080",
		},
		Sync: SyncConfig{
			BatchSize:   64,
			Concurrency: 4,
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
