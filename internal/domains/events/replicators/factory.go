package replicators

import (
	"fmt"

	"github.com/aromancev/synapse/internal/config"
)

func NewFromConfig(cfg config.Config) ([]Replicator, error) {
	if cfg.Replication.Replicator == nil {
		return nil, nil
	}

	replicator := *cfg.Replication.Replicator
	switch replicator.Type {
	case config.ReplicatorTypeFile:
		fileCfg, err := replicator.FileConfig()
		if err != nil {
			return nil, err
		}
		return []Replicator{NewFile(replicator.Name, fileCfg.Path)}, nil
	default:
		return nil, fmt.Errorf("unsupported replicator type %q", replicator.Type)
	}
}
