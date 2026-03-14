package config

const DefaultLogPath = ".synapse/log.jsonl"

// Config contains global synapse configuration persisted in the database.
type Config struct {
	LogPath string `json:"log_path"`
}

func newDefault() Config {
	return Config{
		LogPath: DefaultLogPath,
	}
}
