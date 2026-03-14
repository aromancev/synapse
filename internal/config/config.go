package config

import (
	"encoding/json"
	"fmt"
)

const DefaultLogPath = ".synapse/log.jsonl"

type LoggerType string

const (
	LoggerTypeFile LoggerType = "file"
)

// Config contains global synapse configuration persisted in the database.
type Config struct {
	Logger Logger `json:"logger"`
}

type Logger struct {
	Type   LoggerType      `json:"type"`
	Config json.RawMessage `json:"config"`
}

type FileLoggerConfig struct {
	Path string `json:"path"`
}

func newDefault() Config {
	return Config{
		Logger: DefaultLogger(),
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

func (c Config) Normalize() (Config, error) {
	if c.Logger.Type == "" {
		c.Logger = newDefault().Logger
	}

	logger, err := c.Logger.Normalize()
	if err != nil {
		return Config{}, err
	}
	c.Logger = logger
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
