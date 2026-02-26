package schemas

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

var schemaNameRe = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)

// Schema describes a named JSON schema persisted in the database.
type Schema struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
}

// Normalized returns a normalized copy of the schema.
func (s Schema) Normalized() Schema {
	s.Name = strings.TrimSpace(s.Name)
	s.Schema = strings.TrimSpace(s.Schema)

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(s.Schema)); err == nil {
		s.Schema = compact.String()
	}

	return s
}

// Validate checks whether Schema is valid.
func (s Schema) Validate() []error {
	var errs []error
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
	if err := json.Unmarshal([]byte(s.Schema), &doc); err != nil {
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
