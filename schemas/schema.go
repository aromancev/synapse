package schemas

import (
	"encoding/json"
	"errors"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Schema describes a named JSON schema persisted in the database.
type Schema struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
}

// Normalized returns a normalized copy of the schema.
func (s Schema) Normalized() Schema {
	s.Name = strings.TrimSpace(s.Name)
	return s
}

// Validate checks whether Schema is valid.
func (s Schema) Validate() []error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("schema name is required"))
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
