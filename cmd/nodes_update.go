package cmd

import (
	"encoding/json"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var nodesUpdateCmd = &cobra.Command{
	Use:   "update <node-id> [json-payload]",
	Short: "Update a node",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID, err := nodes.ParseID(args[0])
		if err != nil {
			return err
		}
		payloadArgs := args[1:]
		payloadJSON, err := readJSONPayload(payloadArgs)
		if err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.UpdateNode(cmd.Context(), nodeID, json.RawMessage(payloadJSON)); err != nil {
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

		return writeJSON(map[string]string{"id": nodeID.String()})
	},
}

func init() {
	nodesCmd.AddCommand(nodesUpdateCmd)
}
