package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/evstack/apex/config"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	syncer "github.com/evstack/apex/pkg/sync"
)

// Set via ldflags at build time.
var version = "dev"

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "apex",
		Short:        "Lightweight Celestia namespace indexer",
		SilenceUsage: true,
	}

	root.PersistentFlags().String("config", "config.yaml", "path to config file")

	root.AddCommand(versionCmd())
	root.AddCommand(initCmd())
	root.AddCommand(startCmd())

	return root
}

func configPath(cmd *cobra.Command) (string, error) {
	return cmd.Flags().GetString("config")
}

func initCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Generate a default config file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgPath, err := configPath(cmd)
			if err != nil {
				return err
			}
			if err := config.Generate(cfgPath); err != nil {
				return err
			}
			fmt.Printf("Config written to %s\n", cfgPath)
			return nil
		},
	}
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

func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the Apex indexer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgPath, err := configPath(cmd)
			if err != nil {
				return err
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			setupLogger(cfg.Log)

			log.Info().
				Str("version", version).
				Str("node_url", cfg.DataSource.CelestiaNodeURL).
				Int("namespaces", len(cfg.DataSource.Namespaces)).
				Msg("starting apex indexer")

			return runIndexer(cmd.Context(), cfg)
		},
	}
}

func setupLogger(cfg config.LogConfig) {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	switch cfg.Format {
	case "console":
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	default:
		log.Logger = log.Output(os.Stdout)
	}
}

func runIndexer(ctx context.Context, cfg *config.Config) error {
	// Parse namespaces from config.
	namespaces, err := cfg.ParsedNamespaces()
	if err != nil {
		return fmt.Errorf("parse namespaces: %w", err)
	}

	// Open store.
	db, err := store.Open(cfg.Storage.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer db.Close() //nolint:errcheck

	// Persist configured namespaces.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for _, ns := range namespaces {
		if err := db.PutNamespace(ctx, ns); err != nil {
			return fmt.Errorf("put namespace: %w", err)
		}
	}

	// Connect to Celestia node.
	fetcher, err := fetch.NewCelestiaNodeFetcher(ctx, cfg.DataSource.CelestiaNodeURL, cfg.DataSource.AuthToken, log.Logger)
	if err != nil {
		return fmt.Errorf("connect to celestia node: %w", err)
	}
	defer fetcher.Close() //nolint:errcheck

	// Build and run the sync coordinator.
	coord := syncer.New(db, fetcher,
		syncer.WithStartHeight(cfg.Sync.StartHeight),
		syncer.WithBatchSize(cfg.Sync.BatchSize),
		syncer.WithConcurrency(cfg.Sync.Concurrency),
		syncer.WithLogger(log.Logger),
	)

	log.Info().
		Int("namespaces", len(namespaces)).
		Uint64("start_height", cfg.Sync.StartHeight).
		Msg("sync coordinator starting")

	err = coord.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("coordinator: %w", err)
	}

	log.Info().Msg("apex indexer stopped")
	return nil
}
