package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aromancev/synapse/internal/config"
	"github.com/spf13/cobra"
)

const defaultDBPath = ".synapse/db"

var dbPath string

var rootCmd = &cobra.Command{
	Use:   "synapse",
	Short: "Synapse is an agentic memory system",
	Long: `Synapse is a CLI tool for agentic memory management,
inspired by the A-MEM paper and Zettelkasten method.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return configureLogging(cmd.Context(), dbPath)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dbPath, "db-path", defaultDBPath, "path to sqlite database")
	// Commands added in their own init() functions
}

func configureLogging(ctx context.Context, dbPath string) error {
	logger, err := resolveLogger(ctx, dbPath)
	if err != nil {
		return err
	}

	handler, cleanup, err := newLogHandler(logger)
	if err != nil {
		return err
	}
	_ = cleanup

	slog.SetDefault(slog.New(handler))
	return nil
}

func resolveLogger(ctx context.Context, dbPath string) (config.Logger, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return config.DefaultLogger(), nil
	}
	defer db.Close()

	repo := config.NewRepository(db)
	cfg, err := repo.Get(ctx)
	if err != nil {
		// Return default logger on any error
		return config.DefaultLogger(), nil
	}
	return cfg.Logging.Logger, nil
}

func newLogHandler(logger config.Logger) (slog.Handler, func() error, error) {
	switch logger.Type {
	case config.LoggerTypeFile:
		cfg, err := logger.FileConfig()
		if err != nil {
			return nil, nil, err
		}

		if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
			return nil, nil, fmt.Errorf("create log directory: %w", err)
		}

		f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file: %w", err)
		}

		return slog.NewJSONHandler(f, nil), f.Close, nil
	default:
		return nil, nil, fmt.Errorf("unsupported logger type %q", logger.Type)
	}
}
