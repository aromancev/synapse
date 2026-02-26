package nodes

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var schemaNameRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)

// Node is a stored payload bound to a schema.
type Node struct {
	ID         int64  `json:"id"`
	ExternalID string `json:"external_id"`
	Schema     string `json:"schema"`
	CreatedAt  int64  `json:"created_at"`
	Payload    string `json:"payload"`
}

// Normalized returns a normalized copy of the node.
func (n Node) Normalized() Node {
	n.ExternalID = strings.TrimSpace(n.ExternalID)
	n.Schema = strings.TrimSpace(n.Schema)
	n.Payload = strings.TrimSpace(n.Payload)

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(n.Payload)); err == nil {
		n.Payload = compact.String()
	}

	return n
}

// Validate checks whether Node is valid.
func (n Node) Validate() []error {
	var errs []error

	if n.ExternalID == "" {
		errs = append(errs, errors.New("external_id is required"))
	} else if _, err := uuid.Parse(n.ExternalID); err != nil {
		errs = append(errs, errors.New("external_id must be a valid UUID"))
	}

	if n.Schema == "" {
		errs = append(errs, errors.New("schema is required"))
	} else {
		if len(n.Schema) > 64 {
			errs = append(errs, errors.New("schema must not exceed 64 characters"))
		}
		if !schemaNameRe.MatchString(n.Schema) {
			errs = append(errs, errors.New("schema must be snake_case with lowercase latin letters and numbers only"))
		}
	}

	if n.CreatedAt <= 0 {
		errs = append(errs, errors.New("created_at is required"))
	} else if n.CreatedAt > time.Now().Add(24*time.Hour).Unix() {
		errs = append(errs, errors.New("created_at cannot be in the far future"))
	}

	if n.Payload == "" {
		errs = append(errs, errors.New("payload is required"))
	} else {
		if len(n.Payload) > 256*1024 {
			errs = append(errs, errors.New("payload must not exceed 256KB"))
		}
		var doc any
		if err := json.Unmarshal([]byte(n.Payload), &doc); err != nil {
			errs = append(errs, errors.New("payload must be valid JSON"))
		}
	}

	return errs
}
