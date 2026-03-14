package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DefaultLogPath            = ".synapse/log.jsonl"
	DefaultReplicatorFilePath = ".synapse/replica.jsonl"
)

type LoggerType string

type ReplicatorType string

const (
	LoggerTypeFile LoggerType = "file"

	ReplicatorTypeFile ReplicatorType = "file"
)

// Config contains global synapse configuration persisted in the database.
type Config struct {
	Logger      Logger       `json:"logger"`
	Replicators []Replicator `json:"replicators,omitempty"`
}

type Logger struct {
	Type   LoggerType      `json:"type"`
	Config json.RawMessage `json:"config"`
}

type Replicator struct {
	Name   string          `json:"name"`
	Type   ReplicatorType  `json:"type"`
	Config json.RawMessage `json:"config"`
}

type FileLoggerConfig struct {
	Path string `json:"path"`
}

type FileReplicatorConfig struct {
	Path string `json:"path"`
}

func newDefault() Config {
	return Config{
		Logger:      DefaultLogger(),
		Replicators: []Replicator{},
	}
}

func DefaultLogger() Logger {
	return mustNewLogger(LoggerTypeFile, FileLoggerConfig{Path: DefaultLogPath})
}

func mustNewLogger(t LoggerType, cfg any) Logger {
	payload, err := json.Marshal(cfg)
	if err != nil {
		panic(fmt.Errorf("marshal logger config: %w", err))
	}
	return Logger{Type: t, Config: payload}
}

func mustNewReplicator(name string, t ReplicatorType, cfg any) Replicator {
	payload, err := json.Marshal(cfg)
	if err != nil {
		panic(fmt.Errorf("marshal replicator config: %w", err))
	}
	return Replicator{Name: name, Type: t, Config: payload}
}

func (c Config) Normalize() (Config, error) {
	if c.Logger.Type == "" {
		c.Logger = newDefault().Logger
	}

	logger, err := c.Logger.Normalize()
	if err != nil {
		return Config{}, err
	}
	c.Logger = logger

	if c.Replicators == nil {
		c.Replicators = []Replicator{}
	}

	normalizedReplicators := make([]Replicator, 0, len(c.Replicators))
	seenNames := make(map[string]struct{}, len(c.Replicators))
	for _, r := range c.Replicators {
		normalized, err := r.Normalize()
		if err != nil {
			return Config{}, err
		}
		if _, exists := seenNames[normalized.Name]; exists {
			return Config{}, fmt.Errorf("duplicate replicator name %q", normalized.Name)
		}
		seenNames[normalized.Name] = struct{}{}
		normalizedReplicators = append(normalizedReplicators, normalized)
	}
	c.Replicators = normalizedReplicators

	return c, nil
}

func (l Logger) Normalize() (Logger, error) {
	if l.Type == "" {
		l.Type = LoggerTypeFile
	}

	switch l.Type {
	case LoggerTypeFile:
		cfg := FileLoggerConfig{Path: DefaultLogPath}
		if len(l.Config) > 0 {
			if err := json.Unmarshal(l.Config, &cfg); err != nil {
				return Logger{}, fmt.Errorf("decode file logger config: %w", err)
			}
		}
		if cfg.Path == "" {
			cfg.Path = DefaultLogPath
		}
		return mustNewLogger(LoggerTypeFile, cfg), nil
	default:
		return Logger{}, fmt.Errorf("unsupported logger type %q", l.Type)
	}
}

func (r Replicator) Normalize() (Replicator, error) {
	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" {
		return Replicator{}, fmt.Errorf("replicator name is required")
	}
	if r.Type == "" {
		r.Type = ReplicatorTypeFile
	}

	switch r.Type {
	case ReplicatorTypeFile:
		cfg := FileReplicatorConfig{Path: DefaultReplicatorFilePath}
		if len(r.Config) > 0 {
			if err := json.Unmarshal(r.Config, &cfg); err != nil {
				return Replicator{}, fmt.Errorf("decode file replicator config: %w", err)
			}
		}
		if cfg.Path == "" {
			cfg.Path = DefaultReplicatorFilePath
		}
		return mustNewReplicator(r.Name, ReplicatorTypeFile, cfg), nil
	default:
		return Replicator{}, fmt.Errorf("unsupported replicator type %q", r.Type)
	}
}

func (l Logger) FileConfig() (FileLoggerConfig, error) {
	if l.Type != LoggerTypeFile {
		return FileLoggerConfig{}, fmt.Errorf("logger type %q is not file", l.Type)
	}

	cfg := FileLoggerConfig{Path: DefaultLogPath}
	if len(l.Config) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(l.Config, &cfg); err != nil {
		return FileLoggerConfig{}, fmt.Errorf("decode file logger config: %w", err)
	}
	if cfg.Path == "" {
		cfg.Path = DefaultLogPath
	}
	return cfg, nil
}

func (r Replicator) FileConfig() (FileReplicatorConfig, error) {
	if r.Type != ReplicatorTypeFile {
		return FileReplicatorConfig{}, fmt.Errorf("replicator type %q is not file", r.Type)
	}

	cfg := FileReplicatorConfig{Path: DefaultReplicatorFilePath}
	if len(r.Config) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(r.Config, &cfg); err != nil {
		return FileReplicatorConfig{}, fmt.Errorf("decode file replicator config: %w", err)
	}
	if cfg.Path == "" {
		cfg.Path = DefaultReplicatorFilePath
	}
	return cfg, nil
}
