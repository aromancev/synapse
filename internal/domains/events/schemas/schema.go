package schemas

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/oklog/ulid/v2"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const idPrefix = "schema_"

var schemaNameRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)

// ID is a prefixed ULID-backed schema identifier.
type ID ulid.ULID

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

func (id ID) StreamID() events.StreamID {
	return events.StreamID(id.String())
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

// Schema describes a named JSON schema persisted in the database.
type Schema struct {
	ID     ID              `json:"id"`
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
}

// Normalized returns a normalized copy of the schema.
func (s Schema) Normalized() Schema {
	s.Name = strings.TrimSpace(s.Name)
	s.Schema = json.RawMessage(strings.TrimSpace(string(s.Schema)))

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(s.Schema)); err == nil {
		s.Schema = json.RawMessage(compact.Bytes())
	}

	return s
}

// Validate checks whether Schema is valid.
func (s Schema) Validate() []error {
	var errs []error
	var emptyID ID
	if s.ID == emptyID {
		errs = append(errs, errors.New("schema id is required"))
	}
	if s.Name == "" {
		errs = append(errs, errors.New("schema name is required"))
	} else {
		if len(s.Name) > 64 {
			errs = append(errs, errors.New("schema name must not exceed 64 characters"))
		}
		if !schemaNameRe.MatchString(s.Name) {
			errs = append(errs, errors.New("schema name must be snake_case with lowercase latin letters and numbers only"))
		}
	}
	if len(s.Schema) > 16*1024 {
		errs = append(errs, errors.New("schema must not exceed 16KB"))
	}

	compiler := jsonschema.NewCompiler()
	const resourceURL = "schema.json"

	var doc any
	if err := json.Unmarshal(s.Schema, &doc); err != nil {
		errs = append(errs, err)
		return errs
	}
	if err := compiler.AddResource(resourceURL, doc); err != nil {
		errs = append(errs, err)
		return errs
	}
	if _, err := compiler.Compile(resourceURL); err != nil {
		errs = append(errs, err)
		return errs
	}
	return errs
}
