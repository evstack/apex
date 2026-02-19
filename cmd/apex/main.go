package main

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/evstack/apex/config"
)

// Set via ldflags at build time.
var version = "dev"

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var cfgPath string

	root := &cobra.Command{
		Use:          "apex",
		Short:        "Lightweight Celestia namespace indexer",
		SilenceUsage: true,
	}

	root.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to config file")

	root.AddCommand(versionCmd())
	root.AddCommand(startCmd(&cfgPath))

	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version)
		},
	}
}

func startCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the Apex indexer",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			setupLogger(cfg.Log)

			log.Info().
				Str("version", version).
				Str("node_url", cfg.DataSource.CelestiaNodeURL).
				Int("namespaces", len(cfg.DataSource.Namespaces)).
				Msg("starting apex indexer")

			// TODO(phase1): wire store, fetcher, and sync coordinator.
			log.Info().Msg("apex indexer is not yet implemented — scaffolding only")

			return nil
		},
	}
}

func setupLogger(cfg config.LogConfig) {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	if cfg.Format == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}
