package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/replicators"
	"github.com/aromancev/synapse/internal/services/synapse"
)

func openSynapse() (*synapse.Synapse, config.Config, func() error, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, config.Config{}, nil, err
	}

	repo := config.NewRepository(db)
	cfg, err := repo.Get(rootCmd.Context())
	if err != nil {
		_ = db.Close()
		return nil, config.Config{}, nil, err
	}

	rep, err := replicators.NewFromConfig(cfg)
	if err != nil {
		_ = db.Close()
		return nil, config.Config{}, nil, err
	}

	cleanup := func() error {
		return db.Close()
	}

	return synapse.NewSynapse(db, rep), cfg, cleanup, nil
}

func writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = rootCmd.OutOrStdout().Write(append(b, '\n'))
	return err
}

func readJSONPayload(args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	info, err := os.Stdin.Stat()
	if err != nil {
		return "", fmt.Errorf("stat stdin: %w", err)
	}
	if info.Mode()&os.ModeCharDevice != 0 {
		return "", fmt.Errorf("provide json payload as an argument or pipe it via stdin")
	}

	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read stdin: %w", err)
	}
	if len(payload) == 0 {
		return "", fmt.Errorf("stdin payload is empty")
	}
	return string(payload), nil
}
