package cmd

import "github.com/spf13/cobra"

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage config",
	RunE: func(cmd *cobra.Command, args []string) error {
		return configGetCmd.RunE(cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
