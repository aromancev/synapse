package cmd

import "github.com/spf13/cobra"

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Manage nodes",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nodesListCmd.RunE(cmd, args)
	},
}

func init() {
	nodesCmd.Flags().StringVar(&nodesListSchemaID, "schema-id", "", "schema id")
	nodesCmd.Flags().BoolVar(&nodesListArchived, "archived", false, "list archived nodes instead of live ones")
	rootCmd.AddCommand(nodesCmd)
}
