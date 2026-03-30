package cmd

import "github.com/spf13/cobra"

var nodesSearchLimit int

var nodesSearchCmd = &cobra.Command{
	Use:   "search [json-query-object]",
	Short: "Search nodes and return matching node IDs",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		input, err := readQueryInput(args)
		if err != nil {
			return err
		}
		service, _, cleanup, err := openSynapse()
		if err != nil {
			return err
		}
		defer cleanup()

		ids, err := service.SearchNodes(cmd.Context(), input.Query, nodesSearchLimit)
		if err != nil {
			return err
		}

		result := make([]string, 0, len(ids))
		for _, id := range ids {
			result = append(result, id.String())
		}
		if result == nil {
			result = []string{}
		}

		return writeJSON(result)
	},
}

func init() {
	nodesSearchCmd.Flags().IntVar(&nodesSearchLimit, "limit", 20, "maximum number of matching node ids")
	nodesCmd.AddCommand(nodesSearchCmd)
}
