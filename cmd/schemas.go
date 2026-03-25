package cmd

import "github.com/spf13/cobra"

var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "Manage schemas",
	RunE: func(cmd *cobra.Command, args []string) error {
		return schemasListCmd.RunE(cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(schemasCmd)
}
