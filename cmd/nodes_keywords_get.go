package cmd

import (
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var nodesKeywordsGetCmd = &cobra.Command{
	Use:   "get <node-id>",
	Short: "Get node keywords",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID, err := nodes.ParseID(args[0])
		if err != nil {
			return err
		}

		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		keywords, err := service.GetNodeKeywords(cmd.Context(), nodeID)
		if err != nil {
			return err
		}

		if keywords == nil {
			keywords = []string{}
		}

		return writeJSON(keywords)
	},
}

func init() {
	nodesKeywordsCmd.AddCommand(nodesKeywordsGetCmd)
}
