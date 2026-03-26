package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schema.json
var configSchemaJSON string

func Schema() string {
	return configSchemaJSON
}

const (
	DefaultLogPath            = ".synapse/log.jsonl"
	DefaultReplicatorFilePath = ".synapse/replica.jsonl"
	configSchemaURL           = "synapse-config.schema.json"
)

type LoggerType string

type ReplicatorType string

type ReplicationMode string

const (
	LoggerTypeFile LoggerType = "file"

	ReplicatorTypeFile ReplicatorType = "file"

	ReplicationModeDisabled ReplicationMode = "disabled"
	ReplicationModeAuto     ReplicationMode = "auto"
	ReplicationModeManual   ReplicationMode = "manual"
)

// Config contains global synapse configuration persisted in the database.
type Config struct {
	Logging     Logging     `json:"logging"`
	Replication Replication `json:"replication"`
}

type Logging struct {
	Logger Logger `json:"logger"`
}

type Replication struct {
	Mode       ReplicationMode `json:"mode"`
	Replicator *Replicator     `json:"replicator"`
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
		Logging:     Logging{Logger: mustNewLogger(LoggerTypeFile, FileLoggerConfig{Path: DefaultLogPath})},
		Replication: Replication{Mode: ReplicationModeDisabled, Replicator: nil},
	}
}

func DefaultConfig() Config {
	return newDefault()
}

func DefaultReplication() Replication {
	return Replication{Mode: ReplicationModeDisabled, Replicator: nil}
}

func DefaultLogger() Logger {
	return mustNewLogger(LoggerTypeFile, FileLoggerConfig{Path: DefaultLogPath})
}

func mustNewLogger(t LoggerType, cfg FileLoggerConfig) Logger {
	payload, _ := json.Marshal(cfg)
	return Logger{Type: t, Config: payload}
}

func NewFileLogger(cfg FileLoggerConfig) Logger {
	return mustNewLogger(LoggerTypeFile, cfg)
}

func NewFileReplicator(name string, cfg FileReplicatorConfig) Replicator {
	payload, _ := json.Marshal(cfg)
	return Replicator{Name: name, Type: ReplicatorTypeFile, Config: payload}
}

func (c Config) Normalize() Config {
	defaults := newDefault()
	if c.Logging.Logger.Type == "" {
		c.Logging.Logger = defaults.Logging.Logger
	}

	c.Logging.Logger = c.Logging.Logger.Normalize()
	c.Replication = c.Replication.Normalize()

	return c
}

func (r Replication) Normalize() Replication {
	if r.Mode == "" {
		r.Mode = ReplicationModeDisabled
	}

	if r.Replicator == nil {
		return r
	}

	normalized := r.Replicator.Normalize()
	r.Replicator = &normalized

	return r
}

func (l Logger) Normalize() Logger {
	if l.Type == "" {
		l.Type = LoggerTypeFile
	}

	switch l.Type {
	case LoggerTypeFile:
		cfg := FileLoggerConfig{Path: DefaultLogPath}
		if len(l.Config) > 0 {
			_ = json.Unmarshal(l.Config, &cfg)
		}
		if cfg.Path == "" {
			cfg.Path = DefaultLogPath
		}
		return NewFileLogger(cfg)
	default:
		// unknown type, return as-is for validation to catch
		return l
	}
}

func (r Replicator) Normalize() Replicator {
	r.Name = strings.TrimSpace(r.Name)
	if r.Type == "" {
		r.Type = ReplicatorTypeFile
	}

	switch r.Type {
	case ReplicatorTypeFile:
		cfg := FileReplicatorConfig{Path: DefaultReplicatorFilePath}
		if len(r.Config) > 0 {
			_ = json.Unmarshal(r.Config, &cfg)
		}
		if cfg.Path == "" {
			cfg.Path = DefaultReplicatorFilePath
		}
		return NewFileReplicator(r.Name, cfg)
	default:
		// unknown type, return as-is for validation to catch
		return r
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

func (c Config) Validate() error {
	schema, err := compileSchema()
	if err != nil {
		return err
	}

	payload, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal config for validation: %w", err)
	}

	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return fmt.Errorf("decode config for validation: %w", err)
	}
	if err := schema.Validate(doc); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	return nil
}

func compileSchema() (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()

	var schemaDoc any
	if err := json.Unmarshal([]byte(configSchemaJSON), &schemaDoc); err != nil {
		return nil, fmt.Errorf("decode config schema: %w", err)
	}
	if err := compiler.AddResource(configSchemaURL, schemaDoc); err != nil {
		return nil, fmt.Errorf("add config schema resource: %w", err)
	}
	schema, err := compiler.Compile(configSchemaURL)
	if err != nil {
		return nil, fmt.Errorf("compile config schema: %w", err)
	}
	return schema, nil
}
