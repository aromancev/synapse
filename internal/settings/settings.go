package settings

const DefaultLogPath = ".synapse/log.jsonl"

// Settings contains global synapse configuration persisted in the database.
type Settings struct {
	LogPath string `json:"log_path"`
}

func newDefault() Settings {
	return Settings{
		LogPath: DefaultLogPath,
	}
}
