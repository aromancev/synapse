package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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
	logPath, err := resolveLogPath(ctx, dbPath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(f, nil))
	slog.SetDefault(logger)
	return nil
}

func resolveLogPath(ctx context.Context, dbPath string) (string, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return config.DefaultLogPath, nil
	}
	defer db.Close()

	repo := config.NewRepository(db)
	cfg, err := repo.Get(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no such table: config") {
			return config.DefaultLogPath, nil
		}
		return "", err
	}
	return cfg.LogPath, nil
}
