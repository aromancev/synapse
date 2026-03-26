package cmd

import (
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/spf13/cobra"
)

var configSchemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Print config JSON schema",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Fprintln(cmd.OutOrStdout(), config.Schema())
		return nil
	},
}

func init() {
	configCmd.AddCommand(configSchemaCmd)
}
