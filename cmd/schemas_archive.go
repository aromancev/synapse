package cmd

import (
	"fmt"

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

		service, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.ArchiveSchema(cmd.Context(), schemaID); err != nil {
			return err
		}
		if err := service.RunReplication(cmd.Context()); err != nil {
			return err
		}
		if err := service.RunProjection(cmd.Context(), schemas.NewProjection()); err != nil {
			return err
		}

		fmt.Println(schemaID.String())
		return nil
	},
}

func init() {
	schemasCmd.AddCommand(schemasArchiveCmd)
}
