package config

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load reads and validates a YAML config file at the given path.
// Environment variable APEX_AUTH_TOKEN overrides data_source.auth_token.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg := DefaultConfig()

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Env var override.
	if token := os.Getenv("APEX_AUTH_TOKEN"); token != "" {
		cfg.DataSource.AuthToken = token
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

func validate(cfg *Config) error {
	if cfg.DataSource.CelestiaNodeURL == "" {
		return fmt.Errorf("data_source.celestia_node_url is required")
	}
	if cfg.Storage.DBPath == "" {
		return fmt.Errorf("storage.db_path is required")
	}
	if cfg.Sync.BatchSize <= 0 {
		return fmt.Errorf("sync.batch_size must be positive")
	}
	if cfg.Sync.Concurrency <= 0 {
		return fmt.Errorf("sync.concurrency must be positive")
	}

	// Validate namespace hex strings.
	for _, ns := range cfg.DataSource.Namespaces {
		if _, err := parseNamespaceHex(ns); err != nil {
			return fmt.Errorf("invalid namespace %q: %w", ns, err)
		}
	}

	return nil
}

func parseNamespaceHex(s string) ([29]byte, error) {
	var ns [29]byte
	if len(s) != 58 {
		return ns, fmt.Errorf("expected 58 hex chars (29 bytes), got %d", len(s))
	}
	for i := range 29 {
		hi, err := hexDigit(s[2*i])
		if err != nil {
			return ns, err
		}
		lo, err := hexDigit(s[2*i+1])
		if err != nil {
			return ns, err
		}
		ns[i] = hi<<4 | lo
	}
	return ns, nil
}

func hexDigit(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	default:
		return 0, fmt.Errorf("invalid hex digit: %c", c)
	}
}
