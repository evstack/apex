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
	"google.golang.org/grpc"

	"github.com/evstack/apex/config"
	"github.com/evstack/apex/pkg/api"
	grpcapi "github.com/evstack/apex/pkg/api/grpc"
	jsonrpcapi "github.com/evstack/apex/pkg/api/jsonrpc"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/profile"
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

	root.PersistentFlags().String("rpc-addr", "localhost:8080", "JSON-RPC server address")
	root.PersistentFlags().String("format", "json", "output format (json, table)")

	root.AddCommand(versionCmd())
	root.AddCommand(initCmd())
	root.AddCommand(startCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(blobCmd())
	root.AddCommand(configCmd())

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

			startLog := log.Info().
				Str("version", version).
				Str("datasource_type", cfg.DataSource.Type).
				Int("namespaces", len(cfg.DataSource.Namespaces))
			if cfg.DataSource.Type == "app" {
				startLog = startLog.Str("app_grpc_addr", cfg.DataSource.CelestiaAppGRPCAddr)
			} else {
				startLog = startLog.Str("node_url", cfg.DataSource.CelestiaNodeURL)
			}
			startLog.Msg("starting apex indexer")

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

func setupMetrics(cfg *config.Config) (metrics.Recorder, *metrics.Server) {
	if !cfg.Metrics.Enabled {
		return metrics.Nop(), nil
	}
	rec := metrics.NewPromRecorder(nil, version)
	srv := metrics.NewServer(cfg.Metrics.ListenAddr, log.Logger)
	go func() {
		if err := srv.Start(); err != nil {
			log.Error().Err(err).Msg("metrics server error")
		}
	}()
	return rec, srv
}

func setupProfiling(cfg *config.Config) *profile.Server {
	if !cfg.Profiling.Enabled {
		return nil
	}
	srv := profile.NewServer(cfg.Profiling.ListenAddr, log.Logger)
	go func() {
		if err := srv.Start(); err != nil {
			log.Error().Err(err).Msg("profiling server error")
		}
	}()
	return srv
}

func openDataSource(ctx context.Context, cfg *config.Config) (fetch.DataFetcher, fetch.ProofForwarder, error) {
	switch cfg.DataSource.Type {
	case "app":
		appFetcher, err := fetch.NewCelestiaAppFetcher(cfg.DataSource.CelestiaAppGRPCAddr, cfg.DataSource.AuthToken, log.Logger)
		if err != nil {
			return nil, nil, fmt.Errorf("create celestia-app fetcher: %w", err)
		}
		return appFetcher, nil, nil
	case "node", "":
		nodeFetcher, err := fetch.NewCelestiaNodeFetcher(ctx, cfg.DataSource.CelestiaNodeURL, cfg.DataSource.AuthToken, log.Logger)
		if err != nil {
			return nil, nil, fmt.Errorf("connect to celestia node: %w", err)
		}
		return nodeFetcher, nodeFetcher, nil
	default:
		return nil, nil, fmt.Errorf("unsupported data source type: %q", cfg.DataSource.Type)
	}
}

func openStore(ctx context.Context, cfg *config.Config) (store.Store, error) {
	switch cfg.Storage.Type {
	case "s3":
		return store.NewS3Store(ctx, cfg.Storage.S3)
	case "sqlite", "":
		return store.Open(cfg.Storage.DBPath)
	default:
		return nil, fmt.Errorf("unsupported storage type: %q", cfg.Storage.Type)
	}
}

func runIndexer(ctx context.Context, cfg *config.Config) error {
	// Parse namespaces from config.
	namespaces, err := cfg.ParsedNamespaces()
	if err != nil {
		return fmt.Errorf("parse namespaces: %w", err)
	}

	rec, metricsSrv := setupMetrics(cfg)
	profileSrv := setupProfiling(cfg)

	// Open store.
	db, err := openStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer db.Close() //nolint:errcheck

	// Wire metrics into the store.
	switch s := db.(type) {
	case *store.SQLiteStore:
		s.SetMetrics(rec)
	case *store.S3Store:
		s.SetMetrics(rec)
	}

	// Persist configured namespaces.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for _, ns := range namespaces {
		if err := db.PutNamespace(ctx, ns); err != nil {
			return fmt.Errorf("put namespace: %w", err)
		}
	}

	// Connect to data source.
	dataFetcher, proofFwd, err := openDataSource(ctx, cfg)
	if err != nil {
		return err
	}
	defer dataFetcher.Close() //nolint:errcheck

	// Set up API layer.
	notifier := api.NewNotifier(cfg.Subscription.BufferSize, cfg.Subscription.MaxSubscribers, log.Logger)
	notifier.SetMetrics(rec)
	svc := api.NewService(db, dataFetcher, proofFwd, notifier, log.Logger)

	// Build and run the sync coordinator with observer hook.
	coord := syncer.New(db, dataFetcher,
		syncer.WithStartHeight(cfg.Sync.StartHeight),
		syncer.WithBatchSize(cfg.Sync.BatchSize),
		syncer.WithConcurrency(cfg.Sync.Concurrency),
		syncer.WithLogger(log.Logger),
		syncer.WithMetrics(rec),
		syncer.WithObserver(func(h uint64, hdr *types.Header, blobs []types.Blob) {
			notifier.Publish(api.HeightEvent{Height: h, Header: hdr, Blobs: blobs})
		}),
	)

	// Build HTTP mux: mount health endpoints alongside JSON-RPC.
	rpcServer := jsonrpcapi.NewServer(svc, log.Logger)
	healthHandler := api.NewHealthHandler(coord, db, notifier, version)

	mux := http.NewServeMux()
	healthHandler.Register(mux)
	mux.Handle("/", rpcServer)

	httpSrv := &http.Server{
		Addr:              cfg.RPC.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       time.Duration(cfg.RPC.ReadTimeout) * time.Second,
		WriteTimeout:      time.Duration(cfg.RPC.WriteTimeout) * time.Second,
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
		_ = httpSrv.Close()
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

	gracefulShutdown(httpSrv, grpcSrv, metricsSrv, profileSrv)

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("coordinator: %w", err)
	}

	log.Info().Msg("apex indexer stopped")
	return nil
}

func gracefulShutdown(httpSrv *http.Server, grpcSrv *grpc.Server, metricsSrv *metrics.Server, profileSrv *profile.Server) {
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
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("JSON-RPC server shutdown error")
	}

	if metricsSrv != nil {
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("metrics server shutdown error")
		}
	}
	if profileSrv != nil {
		if err := profileSrv.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("profiling server shutdown error")
		}
	}
}
