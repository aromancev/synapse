package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/aromancev/synapse/internal/domains/events/schemas"
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

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		repo := schemas.NewRepository(db)
		if err := repo.AddSchema(context.Background(), schemas.Schema{
			Name:   addSchemaName,
			Schema: schemaJSON,
		}); err != nil {
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
