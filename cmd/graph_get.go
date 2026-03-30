package cmd

import (
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/spf13/cobra"
)

var (
	graphGetDepth   int
	graphGetBreadth int
)

var graphGetCmd = &cobra.Command{
	Use:   "get [json-node-id-array]",
	Short: "Traverse the graph from explicit node IDs",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		frontier, err := readNodeIDArrayPayload(args)
		if err != nil {
			return err
		}

		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		linked, err := service.GetLinkedNodes(cmd.Context(), frontier, graphGetDepth, graphGetBreadth)
		if err != nil {
			return err
		}
		if linked == nil {
			linked = []nodes.Node{}
		}

		return writeJSON(linked)
	},
}

func init() {
	graphGetCmd.Flags().IntVar(&graphGetDepth, "depth", 3, "maximum traversal depth")
	graphGetCmd.Flags().IntVar(&graphGetBreadth, "breadth", 20, "maximum number of linked nodes to expand per level")
	graphCmd.AddCommand(graphGetCmd)
}
