package settings

// Settings contains global synapse configuration persisted in the database.
type Settings struct{}

func newDefault() Settings {
	return Settings{}
}
