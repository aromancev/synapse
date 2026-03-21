package nodes

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// BuildSearchText converts dynamic JSON payloads into a deterministic,
// human-searchable text representation suitable for FTS indexing.
func BuildSearchText(payload json.RawMessage) string {
	payload = json.RawMessage(bytes.TrimSpace(payload))
	if len(payload) == 0 {
		return ""
	}

	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return strings.TrimSpace(string(payload))
	}

	parts := make([]string, 0, 16)
	appendSearchParts(&parts, value)
	return strings.Join(parts, " ")
}

func appendSearchParts(parts *[]string, value any) bool {
	before := len(*parts)

	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			nested := make([]string, 0, 8)
			if !appendSearchParts(&nested, v[key]) {
				continue
			}
			appendToken(parts, key)
			*parts = append(*parts, nested...)
		}
	case []any:
		for _, item := range v {
			appendSearchParts(parts, item)
		}
	case string:
		appendToken(parts, v)
	}

	return len(*parts) > before
}

func appendToken(parts *[]string, token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	token = strings.Join(strings.Fields(token), " ")
	if token == "" {
		return
	}
	*parts = append(*parts, token)
}
