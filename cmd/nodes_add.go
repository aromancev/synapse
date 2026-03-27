package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/spf13/cobra"
)

var addNodeSchemaID string

var nodesAddCmd = &cobra.Command{
	Use:   "add --schema-id <schema-id> [json-payload]",
	Short: "Add a node",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if addNodeSchemaID == "" {
			return fmt.Errorf("--schema-id is required")
		}
		schemaID, err := schemas.ParseID(addNodeSchemaID)
		if err != nil {
			return err
		}
		payloadJSON, err := readJSONPayload(args)
		if err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		nodeID, err := service.AddNode(cmd.Context(), schemaID, json.RawMessage(payloadJSON))
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

		fmt.Println(nodeID.String())
		return nil
	},
}

func init() {
	nodesAddCmd.Flags().StringVar(&addNodeSchemaID, "schema-id", "", "schema id")
	nodesCmd.AddCommand(nodesAddCmd)
}
