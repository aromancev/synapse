package cmd

import (
	"context"
	"fmt"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/services/synapse"
)

func runPostWriteWork(ctx context.Context, service *synapse.Synapse) error {
	cfg, err := getConfig(ctx)
	if err != nil {
		return err
	}

	if cfg.Replication.Mode == config.ReplicationModeAuto {
		if err := service.RunReplication(ctx); err != nil {
			return fmt.Errorf("run replication: %w", err)
		}
	}

	if err := service.RunProjections(ctx); err != nil {
		return fmt.Errorf("run projections: %w", err)
	}

	return nil
}
