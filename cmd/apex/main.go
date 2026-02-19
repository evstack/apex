package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/evstack/apex/config"
	"github.com/evstack/apex/pkg/api"
	grpcapi "github.com/evstack/apex/pkg/api/grpc"
	jsonrpcapi "github.com/evstack/apex/pkg/api/jsonrpc"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	syncer "github.com/evstack/apex/pkg/sync"
	"github.com/evstack/apex/pkg/types"
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

	// Set up API layer.
	notifier := api.NewNotifier(cfg.Subscription.BufferSize, log.Logger)
	svc := api.NewService(db, fetcher, fetcher, notifier, log.Logger)

	// Build and run the sync coordinator with observer hook.
	coord := syncer.New(db, fetcher,
		syncer.WithStartHeight(cfg.Sync.StartHeight),
		syncer.WithBatchSize(cfg.Sync.BatchSize),
		syncer.WithConcurrency(cfg.Sync.Concurrency),
		syncer.WithLogger(log.Logger),
		syncer.WithObserver(func(h uint64, hdr *types.Header, blobs []types.Blob) {
			notifier.Publish(api.HeightEvent{Height: h, Header: hdr, Blobs: blobs})
		}),
	)

	// Start JSON-RPC server.
	rpcServer := jsonrpcapi.NewServer(svc, log.Logger)
	httpSrv := &http.Server{
		Addr:              cfg.RPC.ListenAddr,
		Handler:           rpcServer,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info().Str("addr", cfg.RPC.ListenAddr).Msg("JSON-RPC server listening")
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("JSON-RPC server error")
		}
	}()

	// Start gRPC server.
	grpcSrv := grpcapi.NewServer(svc, log.Logger)
	lis, err := net.Listen("tcp", cfg.RPC.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}

	go func() {
		log.Info().Str("addr", cfg.RPC.GRPCListenAddr).Msg("gRPC server listening")
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error().Err(err).Msg("gRPC server error")
		}
	}()

	log.Info().
		Int("namespaces", len(namespaces)).
		Uint64("start_height", cfg.Sync.StartHeight).
		Msg("sync coordinator starting")

	err = coord.Run(ctx)

	// Graceful shutdown.
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()

	grpcTimeout := time.After(5 * time.Second)
	select {
	case <-stopped:
	case <-grpcTimeout:
		log.Warn().Msg("gRPC graceful stop timed out, forcing stop")
		grpcSrv.Stop()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error().Err(shutdownErr).Msg("JSON-RPC server shutdown error")
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("coordinator: %w", err)
	}

	log.Info().Msg("apex indexer stopped")
	return nil
}
