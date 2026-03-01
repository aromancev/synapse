package cmd

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/links"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/aromancev/synapse/internal/settings"
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

		schemasRepo := schemas.NewProjectionRepository(db)
		if err := schemasRepo.Init(context.Background()); err != nil {
			return err
		}

		eventsRepo := events.NewRepository(db)
		if err := eventsRepo.Init(context.Background()); err != nil {
			return err
		}

		nodesRepo := nodes.NewRepository(db)
		if err := nodesRepo.Init(context.Background()); err != nil {
			return err
		}

		linksRepo := links.NewProjectionRepository(db)
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
