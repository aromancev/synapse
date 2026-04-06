package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aromancev/synapse/internal/config"
	"github.com/aromancev/synapse/internal/domains/events/nodes"
	"github.com/aromancev/synapse/internal/domains/events/replicators"
	"github.com/aromancev/synapse/internal/domains/events/schemas"
	"github.com/aromancev/synapse/internal/services/synapse"
)

func openSynapse() (*synapse.Synapse, config.Config, func() error, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, config.Config{}, nil, err
	}

	repo := config.NewRepository(db)
	cfg, err := repo.Get(rootCmd.Context())
	if err != nil {
		_ = db.Close()
		return nil, config.Config{}, nil, err
	}

	rep, err := replicators.NewFromConfig(cfg)
	if err != nil {
		_ = db.Close()
		return nil, config.Config{}, nil, err
	}

	cleanup := func() error {
		return db.Close()
	}

	return synapse.NewSynapse(db, rep), cfg, cleanup, nil
}

func writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = rootCmd.OutOrStdout().Write(append(b, '\n'))
	return err
}

func writeOK(action string, fields map[string]any) error {
	result := map[string]any{
		"status": "ok",
		"action": action,
	}
	for k, v := range fields {
		result[k] = v
	}
	return writeJSON(result)
}

func readJSONPayload(args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	info, err := os.Stdin.Stat()
	if err != nil {
		return "", fmt.Errorf("stat stdin: %w", err)
	}
	if info.Mode()&os.ModeCharDevice != 0 {
		return "", fmt.Errorf("provide json payload as an argument or pipe it via stdin")
	}

	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read stdin: %w", err)
	}
	if len(payload) == 0 {
		return "", fmt.Errorf("stdin payload is empty")
	}
	return string(payload), nil
}

func readNodeIDArrayPayload(args []string) ([]nodes.ID, error) {
	payloadJSON, err := readJSONPayload(args)
	if err != nil {
		return nil, err
	}

	var rawIDs []string
	if err := json.Unmarshal([]byte(payloadJSON), &rawIDs); err != nil {
		return nil, fmt.Errorf("decode node id array: %w", err)
	}

	ids := make([]nodes.ID, 0, len(rawIDs))
	for _, rawID := range rawIDs {
		id, err := nodes.ParseID(rawID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func readStringPayload(args []string) (string, error) {
	payloadJSON, err := readJSONPayload(args)
	if err != nil {
		return "", err
	}

	var value string
	if err := json.Unmarshal([]byte(payloadJSON), &value); err != nil {
		return "", fmt.Errorf("decode string payload: %w", err)
	}
	return value, nil
}

type queryInput struct {
	Query []string `json:"query"`
}

func readQueryInput(args []string) (queryInput, error) {
	payloadJSON, err := readJSONPayload(args)
	if err != nil {
		return queryInput{}, err
	}

	var input queryInput
	if err := json.Unmarshal([]byte(payloadJSON), &input); err != nil {
		return queryInput{}, fmt.Errorf("decode query input: %w", err)
	}

	input.Query = normalizeSearchTerms(input.Query)
	if len(input.Query) == 0 {
		return queryInput{}, fmt.Errorf("query is required")
	}

	return input, nil
}

func buildFTSQuery(terms []string) string {
	terms = normalizeSearchTerms(terms)
	if len(terms) == 0 {
		return ""
	}
	return buildKeywordGroupFTSQuery(terms)
}

func normalizeSearchTerms(terms []string) []string {
	if len(terms) == 0 {
		return nil
	}

	out := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		term = strings.Join(strings.Fields(term), " ")
		if term == "" {
			continue
		}
		out = append(out, term)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildKeywordGroupFTSQuery(terms []string) string {
	clauses := make([]string, 0, len(terms))
	for _, term := range terms {
		parts := strings.Fields(term)
		if len(parts) == 0 {
			continue
		}

		escapedParts := make([]string, 0, len(parts))
		for _, part := range parts {
			escapedParts = append(escapedParts, quoteFTSTerm(part))
		}

		clause := strings.Join(escapedParts, " ")
		if len(escapedParts) > 1 {
			clause = "(" + clause + ")"
		}
		clauses = append(clauses, clause)
	}
	return strings.Join(clauses, " OR ")
}

func quoteFTSTerm(term string) string {
	return `"` + strings.ReplaceAll(term, `"`, `""`) + `"`
}

func readNodeIDPayload(args []string) (nodes.ID, error) {
	value, err := readStringPayload(args)
	if err != nil {
		return nodes.ID{}, err
	}
	return nodes.ParseID(value)
}

func readSchemaIDPayload(args []string) (schemas.ID, error) {
	value, err := readStringPayload(args)
	if err != nil {
		return schemas.ID{}, err
	}
	return schemas.ParseID(value)
}
