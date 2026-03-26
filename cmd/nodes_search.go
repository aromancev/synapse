package cmd

import (
	"strings"

	"github.com/spf13/cobra"
)

var nodesSearchLimit int

var nodesSearchCmd = &cobra.Command{
	Use:   "search <keywords...>",
	Short: "Search nodes and return matching node IDs",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		service, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		ids, err := service.SearchNodes(cmd.Context(), strings.Join(args, " "), nodesSearchLimit)
		if err != nil {
			return err
		}

		result := make([]string, 0, len(ids))
		for _, id := range ids {
			result = append(result, id.String())
		}

		return writeJSON(result)
	},
}

func init() {
	nodesSearchCmd.Flags().IntVar(&nodesSearchLimit, "limit", 20, "maximum number of matching node ids")
	nodesCmd.AddCommand(nodesSearchCmd)
}
