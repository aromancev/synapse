package cmd

import (
	"context"

	"github.com/aromancev/synapse/internal/config"
	"github.com/spf13/cobra"
)

var configGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get config",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := getConfig(cmd.Context())
		if err != nil {
			return err
		}
		return writeJSON(cfg)
	},
}

func getConfig(ctx context.Context) (config.Config, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return config.Config{}, err
	}
	defer db.Close()

	repo := config.NewRepository(db)
	return repo.Get(ctx)
}

func init() {
	configCmd.AddCommand(configGetCmd)
}
