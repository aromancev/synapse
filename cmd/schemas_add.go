package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/spf13/cobra"
)

var addSchemaName string

var schemasAddCmd = &cobra.Command{
	Use:   "add --name <name> [json-payload]",
	Short: "Add a JSON schema",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if addSchemaName == "" {
			return fmt.Errorf("--name is required")
		}
		schemaJSON, err := readJSONPayload(args)
		if err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		schemaID, err := service.AddSchema(cmd.Context(), addSchemaName, json.RawMessage(schemaJSON))
		if err != nil {
			return err
		}
		if cfg.Replication.Mode == config.ReplicationModeAuto {
			if err := service.RunReplication(cmd.Context()); err != nil {
				return err
			}
		}
		if err := service.RunProjections(cmd.Context()); err != nil {
			return err
		}

		return writeOK("schema_added", map[string]any{
			"id":   schemaID.String(),
			"name": addSchemaName,
		})
	},
}

func init() {
	schemasAddCmd.Flags().StringVar(&addSchemaName, "name", "", "schema name")
	schemasCmd.AddCommand(schemasAddCmd)
}
