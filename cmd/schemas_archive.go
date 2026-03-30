package cmd

import (
	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/spf13/cobra"
)

var schemasArchiveCmd = &cobra.Command{
	Use:   "archive <schema-id>",
	Short: "Archive a schema",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		schemaID, err := schemas.ParseID(args[0])
		if err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.ArchiveSchema(cmd.Context(), schemaID); err != nil {
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

		return writeOK("schema_archived", map[string]any{
			"id": schemaID.String(),
		})
	},
}

func init() {
	schemasCmd.AddCommand(schemasArchiveCmd)
}
