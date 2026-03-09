package nodes

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const idPrefix = "node_"

var schemaNameRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)

// ID is a prefixed ULID-backed node identifier.
type ID ulid.ULID

// NewID generates a new node ID.
func NewID() (ID, error) {
	id, err := ulid.New(ulid.Now(), ulid.DefaultEntropy())
	if err != nil {
		return ID{}, fmt.Errorf("generate ulid: %w", err)
	}
	return ID(id), nil
}

func (id ID) String() string {
	return idPrefix + ulid.ULID(id).String()
}

func ParseID(s string) (ID, error) {
	if strings.TrimSpace(s) == "" {
		return ID{}, errors.New("id is required")
	}
	if !strings.HasPrefix(s, idPrefix) {
		return ID{}, fmt.Errorf("id must have prefix %q", idPrefix)
	}
	parsed, err := ulid.ParseStrict(strings.TrimPrefix(s, idPrefix))
	if err != nil {
		return ID{}, fmt.Errorf("parse ulid: %w", err)
	}
	return ID(parsed), nil
}

func (id *ID) Scan(src any) error {
	v, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into ID", src)
	}
	if len(v) != len(ulid.ULID{}) {
		return fmt.Errorf("invalid byte length for ID: %d", len(v))
	}
	var parsed ulid.ULID
	copy(parsed[:], v)
	*id = ID(parsed)
	return nil
}

func (id ID) Value() (driver.Value, error) {
	value := ulid.ULID(id)
	return value[:], nil
}

func (id ID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

func (id *ID) UnmarshalJSON(data []byte) error {
	parsed, err := ParseID(strings.Trim(string(data), `"`))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Node is a stored payload bound to a schema.
type Node struct {
	ID         int64           `json:"id"`
	UID        ID              `json:"uid"`
	SchemaName string          `json:"schema_name"`
	CreatedAt  int64           `json:"created_at"`
	Payload    json.RawMessage `json:"payload"`
}

// Normalized returns a normalized copy of the node.
func (n Node) Normalized() Node {
	n.SchemaName = strings.TrimSpace(n.SchemaName)
	n.Payload = json.RawMessage(strings.TrimSpace(string(n.Payload)))

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(n.Payload)); err == nil {
		n.Payload = json.RawMessage(compact.Bytes())
	}

	return n
}

// Validate checks whether Node is valid.
func (n Node) Validate() []error {
	var errs []error

	var emptyID ID
	if n.UID == emptyID {
		errs = append(errs, errors.New("uid is required"))
	}

	if n.SchemaName == "" {
		errs = append(errs, errors.New("schema_name is required"))
	} else {
		if len(n.SchemaName) > 64 {
			errs = append(errs, errors.New("schema_name must not exceed 64 characters"))
		}
		if !schemaNameRe.MatchString(n.SchemaName) {
			errs = append(errs, errors.New("schema_name must be snake_case with lowercase latin letters and numbers only"))
		}
	}

	if n.CreatedAt <= 0 {
		errs = append(errs, errors.New("created_at is required"))
	} else if n.CreatedAt > time.Now().Add(24*time.Hour).Unix() {
		errs = append(errs, errors.New("created_at cannot be in the far future"))
	}

	if len(n.Payload) == 0 {
		errs = append(errs, errors.New("payload is required"))
	} else {
		if len(n.Payload) > 256*1024 {
			errs = append(errs, errors.New("payload must not exceed 256KB"))
		}
		var doc any
		if err := json.Unmarshal(n.Payload, &doc); err != nil {
			errs = append(errs, errors.New("payload must be valid JSON"))
		}
	}

	return errs
}
