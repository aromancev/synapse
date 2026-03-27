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
	Use:   "get <node-id>...",
	Short: "Traverse the graph from explicit node IDs",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		frontier := make([]nodes.ID, 0, len(args))
		for _, arg := range args {
			id, err := nodes.ParseID(arg)
			if err != nil {
				return err
			}
			frontier = append(frontier, id)
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

		return writeJSON(linked)
	},
}

func init() {
	graphGetCmd.Flags().IntVar(&graphGetDepth, "depth", 3, "maximum traversal depth")
	graphGetCmd.Flags().IntVar(&graphGetBreadth, "breadth", 20, "maximum number of linked nodes to expand per level")
	graphCmd.AddCommand(graphGetCmd)
}
