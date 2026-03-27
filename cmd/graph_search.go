package cmd

import (
	"strings"

	"github.com/spf13/cobra"
)

var (
	graphSearchLimit   int
	graphSearchDepth   int
	graphSearchBreadth int
)

var graphSearchCmd = &cobra.Command{
	Use:   "search <keywords...>",
	Short: "Search for seed nodes, then traverse the graph",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		seedIDs, err := service.SearchNodes(cmd.Context(), strings.Join(args, " "), graphSearchLimit)
		if err != nil {
			return err
		}

		linked, err := service.GetLinkedNodes(cmd.Context(), seedIDs, graphSearchDepth, graphSearchBreadth)
		if err != nil {
			return err
		}

		return writeJSON(linked)
	},
}

func init() {
	graphSearchCmd.Flags().IntVar(&graphSearchLimit, "search-limit", 10, "maximum number of matching seed node ids")
	graphSearchCmd.Flags().IntVar(&graphSearchDepth, "depth", 3, "maximum traversal depth")
	graphSearchCmd.Flags().IntVar(&graphSearchBreadth, "breadth", 20, "maximum number of linked nodes to expand per level")
	graphCmd.AddCommand(graphSearchCmd)
}
