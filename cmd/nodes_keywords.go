package cmd

import "github.com/spf13/cobra"

var nodesKeywordsCmd = &cobra.Command{
	Use:   "keywords",
	Short: "Manage node keywords",
}

func init() {
	nodesCmd.AddCommand(nodesKeywordsCmd)
}
