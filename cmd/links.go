package cmd

import "github.com/spf13/cobra"

var linksCmd = &cobra.Command{
	Use:   "links",
	Short: "Manage links between nodes",
}

func init() {
	rootCmd.AddCommand(linksCmd)
}
