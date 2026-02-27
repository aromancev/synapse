package cmd

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/aromancev/synapse/src/links"
	"github.com/aromancev/synapse/src/nodes"
	"github.com/aromancev/synapse/src/schemas"
	"github.com/aromancev/synapse/src/settings"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize synapse",
	Long:  `Initializes the synapse memory system.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		settingsRepo := settings.NewRepository(db)
		if err := settingsRepo.Init(context.Background()); err != nil {
			return err
		}

		schemasRepo := schemas.NewRepository(db)
		if err := schemasRepo.Init(context.Background()); err != nil {
			return err
		}

		nodesRepo := nodes.NewRepository(db)
		if err := nodesRepo.Init(context.Background()); err != nil {
			return err
		}

		linksRepo := links.NewRepository(db)
		if err := linksRepo.Init(context.Background()); err != nil {
			return err
		}

		fmt.Println("synapse initialized")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
