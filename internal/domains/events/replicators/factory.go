package replicators

import (
	"fmt"

	"github.com/aromancev/synapse/internal/config"
)

func NewFromConfig(cfg config.Config) ([]Replicator, error) {
	out := make([]Replicator, 0, len(cfg.Replicators))
	for _, replicator := range cfg.Replicators {
		switch replicator.Type {
		case config.ReplicatorTypeFile:
			fileCfg, err := replicator.FileConfig()
			if err != nil {
				return nil, err
			}
			out = append(out, NewFile(replicator.Name, fileCfg.Path))
		default:
			return nil, fmt.Errorf("unsupported replicator type %q", replicator.Type)
		}
	}
	return out, nil
}
