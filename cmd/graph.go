package cmd

import "github.com/spf13/cobra"

var graphCmd = &cobra.Command{
	Use:   "graph",
	Short: "Traverse the node graph",
}

func init() {
	rootCmd.AddCommand(graphCmd)
}
