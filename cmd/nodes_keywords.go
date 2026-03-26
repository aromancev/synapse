package cmd

import "github.com/spf13/cobra"

var nodesKeywordsCmd = &cobra.Command{
	Use:   "keywords [node-id]",
	Short: "Manage node keywords",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return nodesKeywordsGetCmd.RunE(cmd, args)
	},
}

func init() {
	nodesCmd.AddCommand(nodesKeywordsCmd)
}
