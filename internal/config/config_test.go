package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigNormalize(t *testing.T) {
	t.Run("defaults replication to disabled with empty replicators", func(t *testing.T) {
		cfg := (Config{}).Normalize()
		require.Equal(t, ReplicationModeDisabled, cfg.Replication.Mode)
		require.Nil(t, cfg.Replication.Replicator)
	})
}

func TestConfigValidate(t *testing.T) {
	t.Run("accepts normalized default config", func(t *testing.T) {
		cfg := (Config{}).Normalize()
		require.NoError(t, cfg.Validate())
	})

	t.Run("rejects empty logger path", func(t *testing.T) {
		cfg := Config{
			Logging:     Logging{Logger: Logger{Type: LoggerTypeFile, Config: []byte(`{"path":""}`)}},
			Replication: Replication{Mode: ReplicationModeDisabled, Replicator: nil},
		}
		err := cfg.Validate()
		require.ErrorContains(t, err, "validate config")
	})

	t.Run("rejects logger path with surrounding whitespace", func(t *testing.T) {
		cfg := Config{
			Logging:     Logging{Logger: Logger{Type: LoggerTypeFile, Config: []byte(`{"path":"  ./log.jsonl  "}`)}},
			Replication: Replication{Mode: ReplicationModeDisabled, Replicator: nil},
		}
		err := cfg.Validate()
		require.ErrorContains(t, err, "validate config")
	})

	t.Run("rejects invalid replicator name", func(t *testing.T) {
		cfg := Config{
			Logging: Logging{Logger: Logger{Type: LoggerTypeFile, Config: []byte(`{"path":".synapse/log.jsonl"}`)}},
			Replication: Replication{
				Mode: ReplicationModeManual,
				Replicator: &Replicator{
					Name:   "Events JSONL",
					Type:   ReplicatorTypeFile,
					Config: []byte(`{"path":".synapse/replica.jsonl"}`),
				},
			},
		}
		err := cfg.Validate()
		require.ErrorContains(t, err, "validate config")
	})

	t.Run("accepts missing replicator", func(t *testing.T) {
		cfg := Config{
			Logging: Logging{Logger: Logger{Type: LoggerTypeFile, Config: []byte(`{"path":".synapse/log.jsonl"}`)}},
			Replication: Replication{
				Mode:       ReplicationModeDisabled,
				Replicator: nil,
			},
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})
}
