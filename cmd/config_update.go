package cmd

import (
	"encoding/json"

	"github.com/aromancev/synapse/internal/config"
	"github.com/spf13/cobra"
)

var configUpdateCmd = &cobra.Command{
	Use:   "update [json-payload]",
	Short: "Update config",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		payloadJSON, err := readJSONPayload(args)
		if err != nil {
			return err
		}

		var cfg config.Config
		if err := json.Unmarshal([]byte(payloadJSON), &cfg); err != nil {
			return err
		}

		db, err := openDB(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		repo := config.NewRepository(db)
		if err := repo.Upsert(cmd.Context(), cfg); err != nil {
			return err
		}

		stored, err := repo.Get(cmd.Context())
		if err != nil {
			return err
		}
		return writeJSON(stored)
	},
}

func init() {
	configCmd.AddCommand(configUpdateCmd)
}
