package cmd

import (
	"github.com/spf13/cobra"
)

var dbPath string

var rootCmd = &cobra.Command{
	Use:   "synapse",
	Short: "Synapse is an agentic memory system",
	Long: `Synapse is a CLI tool for agentic memory management,
inspired by the A-MEM paper and Zettelkasten method.`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dbPath, "db-path", "data.db", "path to sqlite database")
	// Commands added in their own init() functions
}
