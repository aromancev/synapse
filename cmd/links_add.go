package cmd

import (
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var linksAddCmd = &cobra.Command{
	Use:   "add <from-node-id> <to-node-id>",
	Short: "Create a link between two nodes",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		fromID, err := nodes.ParseID(args[0])
		if err != nil {
			return err
		}
		toID, err := nodes.ParseID(args[1])
		if err != nil {
			return err
		}

		service, cfg, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		if err := service.LinkNodes(cmd.Context(), fromID, toID); err != nil {
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

		fmt.Println("ok")
		return nil
	},
}

func init() {
	linksCmd.AddCommand(linksAddCmd)
}
