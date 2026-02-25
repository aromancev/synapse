package cmd

import "github.com/spf13/cobra"

var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "Manage schemas",
}

func init() {
	rootCmd.AddCommand(schemasCmd)
}
