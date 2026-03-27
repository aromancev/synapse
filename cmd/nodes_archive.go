package cmd

import (
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var nodesArchiveCmd = &cobra.Command{
	Use:   "archive <node-id>",
	Short: "Archive a node",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID, err := nodes.ParseID(args[0])
		if err != nil {
			return err
		}
		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.ArchiveNode(cmd.Context(), nodeID); err != nil {
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
	nodesCmd.AddCommand(nodesArchiveCmd)
}
