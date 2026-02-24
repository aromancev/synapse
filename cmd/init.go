package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize synapse (no-op for now)",
	Long:  `Initializes the synapse memory system. Currently a placeholder.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("synapse init called (no-op)")
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
