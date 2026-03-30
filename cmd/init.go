package cmd

import (
	"context"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/domains/events/links"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize synapse",
	Long:  `Initializes the synapse memory system.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := openOrCreateDB(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		configRepo := config.NewRepository(db)
		if err := configRepo.Init(context.Background()); err != nil {
			return err
		}

		schemasRepo := schemas.NewProjectionRepository()
		if err := schemasRepo.Init(context.Background(), db); err != nil {
			return err
		}

		eventsRepo := events.NewRepository()
		if err := eventsRepo.Init(context.Background(), db); err != nil {
			return err
		}

		nodesRepo := nodes.NewProjectionRepository()
		if err := nodesRepo.Init(context.Background(), db); err != nil {
			return err
		}

		linksRepo := links.NewProjectionRepository()
		if err := linksRepo.Init(context.Background(), db); err != nil {
			return err
		}

		return writeOK("initialized", map[string]any{"db_path": dbPath})
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
