package nodes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSearchText(t *testing.T) {
	t.Run("flattens dynamic json deterministically", func(t *testing.T) {
		payload := json.RawMessage(`{
			"summary": "analytical engine pioneer",
			"name": "Ada Lovelace",
			"tags": ["math", "history"],
			"meta": {"source": "book notes", "year": 1843},
			"active": true,
			"ignored": null
		}`)

		assert.Equal(t, "meta source book notes name Ada Lovelace summary analytical engine pioneer tags math history", BuildSearchText(payload, nil))
	})

	t.Run("includes normalized keywords", func(t *testing.T) {
		assert.Equal(t, "name Ada math history science", BuildSearchText(json.RawMessage(`{"name":"Ada"}`), []string{"  Math  ", "history science", "math"}))
	})
}
