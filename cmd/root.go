package cmd

import (
	"github.com/spf13/cobra"
)

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
	// Commands added in their own init() functions
}
