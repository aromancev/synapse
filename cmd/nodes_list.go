package cmd

import (
	"fmt"

	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/spf13/cobra"
)

var (
	nodesListSchemaID string
	nodesListArchived bool
)

var nodesListCmd = &cobra.Command{
	Use:   "list --schema-id <schema-id>",
	Short: "List nodes",
	RunE: func(cmd *cobra.Command, args []string) error {
		if nodesListSchemaID == "" {
			return fmt.Errorf("--schema-id is required")
		}
		schemaID, err := schemas.ParseID(nodesListSchemaID)
		if err != nil {
			return err
		}

		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if nodesListArchived {
			storedNodes, err := service.GetArchivedNodesBySchemaID(cmd.Context(), schemaID, 1000)
			if err != nil {
				return err
			}
			if storedNodes == nil {
				storedNodes = []nodes.Node{}
			}
			return writeJSON(storedNodes)
		}

		storedNodes, err := service.GetNodesBySchemaID(cmd.Context(), schemaID, 1000)
		if err != nil {
			return err
		}
		if storedNodes == nil {
			storedNodes = []nodes.Node{}
		}
		return writeJSON(storedNodes)
	},
}

func init() {
	nodesListCmd.Flags().StringVar(&nodesListSchemaID, "schema-id", "", "schema id")
	nodesListCmd.Flags().BoolVar(&nodesListArchived, "archived", false, "list archived nodes instead of live ones")
	nodesCmd.AddCommand(nodesListCmd)
}
