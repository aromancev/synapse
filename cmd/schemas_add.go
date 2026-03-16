package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aromancev/synapse/internal/services/synapse"
	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

var (
	addSchemaName       string
	addSchemaJSON       string
	addSchemaSchemaFile string
)

var schemasAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a JSON schema",
	RunE: func(cmd *cobra.Command, args []string) error {
		if addSchemaName == "" {
			return fmt.Errorf("--name is required")
		}

		schemaJSON := addSchemaJSON
		if addSchemaSchemaFile != "" {
			b, err := os.ReadFile(addSchemaSchemaFile)
			if err != nil {
				return err
			}
			schemaJSON = string(b)
		}
		if schemaJSON == "" {
			return fmt.Errorf("provide --schema or --schema-file")
		}

		db, err := openDB(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		service := synapse.NewSynapse(db, nil)
		if err := service.AddSchema(cmd.Context(), addSchemaName, json.RawMessage(schemaJSON)); err != nil {
			return err
		}

		fmt.Printf("schema %q added\n", addSchemaName)
		return nil
	},
}

func init() {
	schemasAddCmd.Flags().StringVar(&addSchemaName, "name", "", "schema name")
	schemasAddCmd.Flags().StringVar(&addSchemaJSON, "schema", "", "json schema document")
	schemasAddCmd.Flags().StringVar(&addSchemaSchemaFile, "schema-file", "", "path to json schema file")
	schemasCmd.AddCommand(schemasAddCmd)
}
