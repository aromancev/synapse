package cmd_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cliHarness struct {
	t           *testing.T
	repoRoot    string
	root        string
	dbPath      string
	logPath     string
	replicaPath string
}

type cmdResult struct {
	Stdout string
	Stderr string
}

type configDoc struct {
	Logging struct {
		Logger struct {
			Type   string `json:"type"`
			Config struct {
				Path string `json:"path"`
			} `json:"config"`
		} `json:"logger"`
	} `json:"logging"`
	Replication struct {
		Mode       string `json:"mode"`
		Replicator *struct {
			Name   string `json:"name"`
			Type   string `json:"type"`
			Config struct {
				Path string `json:"path"`
			} `json:"config"`
		} `json:"replicator"`
	} `json:"replication"`
}

type schemaRecord struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Schema     json.RawMessage `json:"schema"`
	ArchivedAt string          `json:"archived_at,omitempty"`
}

type nodeRecord struct {
	ID         string          `json:"id"`
	SchemaID   string          `json:"schema_id"`
	CreatedAt  string          `json:"created_at"`
	Payload    json.RawMessage `json:"payload"`
	Keywords   []string        `json:"keywords"`
	ArchivedAt string          `json:"archived_at,omitempty"`
}

type eventRecord struct {
	ID             string          `json:"id"`
	StreamID       string          `json:"stream_id"`
	StreamType     string          `json:"stream_type"`
	StreamVersion  int64           `json:"stream_version"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	OccurredAt     string          `json:"occurred_at"`
	RecordedAt     string          `json:"recorded_at"`
	GlobalPosition int64           `json:"global_position"`
}

func newCLIHarness(t *testing.T) *cliHarness {
	t.Helper()
	root := t.TempDir()
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	return &cliHarness{
		t:           t,
		repoRoot:    repoRoot,
		root:        root,
		dbPath:      filepath.Join(root, "data", "synapse.db"),
		logPath:     filepath.Join(root, "logs", "synapse.jsonl"),
		replicaPath: filepath.Join(root, "replication", "replica.jsonl"),
	}
}

func (h *cliHarness) run(args ...string) cmdResult {
	h.t.Helper()
	cmd := exec.Command("go", append([]string{"run", "./synapse", "--db-path", h.dbPath}, args...)...)
	cmd.Dir = h.repoRoot
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(h.t, cmd.Run(), "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return cmdResult{Stdout: strings.TrimSpace(stdout.String()), Stderr: strings.TrimSpace(stderr.String())}
}

func (h *cliHarness) runWithStdin(stdin string, args ...string) cmdResult {
	h.t.Helper()
	cmd := exec.Command("go", append([]string{"run", "./synapse", "--db-path", h.dbPath}, args...)...)
	cmd.Dir = h.repoRoot
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(h.t, cmd.Run(), "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return cmdResult{Stdout: strings.TrimSpace(stdout.String()), Stderr: strings.TrimSpace(stderr.String())}
}

func (h *cliHarness) runErr(args ...string) string {
	h.t.Helper()
	cmd := exec.Command("go", append([]string{"run", "./synapse", "--db-path", h.dbPath}, args...)...)
	cmd.Dir = h.repoRoot
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	require.Error(h.t, err, "expected failure, stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return strings.TrimSpace(stdout.String() + "\n" + stderr.String())
}

func (h *cliHarness) init() {
	h.t.Helper()
	h.run("init")
}

func (h *cliHarness) updateConfig(cfg string) configDoc {
	h.t.Helper()
	out := h.run("config", "update", cfg)
	return decodeJSON[configDoc](h.t, out.Stdout)
}

func (h *cliHarness) updateConfigStdin(cfg string) configDoc {
	h.t.Helper()
	out := h.runWithStdin(cfg, "config", "update")
	return decodeJSON[configDoc](h.t, out.Stdout)
}

func (h *cliHarness) getConfig() configDoc {
	h.t.Helper()
	out := h.run("config", "get")
	return decodeJSON[configDoc](h.t, out.Stdout)
}

func (h *cliHarness) addSchema(name string, schema string) string {
	h.t.Helper()
	return h.run("schemas", "add", "--name", name, schema).Stdout
}

func (h *cliHarness) addSchemaStdin(name string, schema string) string {
	h.t.Helper()
	return h.runWithStdin(schema, "schemas", "add", "--name", name).Stdout
}

func (h *cliHarness) addNode(schemaID string, payload string) string {
	h.t.Helper()
	return h.run("nodes", "add", "--schema-id", schemaID, payload).Stdout
}

func (h *cliHarness) addNodeStdin(schemaID string, payload string) string {
	h.t.Helper()
	return h.runWithStdin(payload, "nodes", "add", "--schema-id", schemaID).Stdout
}

func (h *cliHarness) updateNode(nodeID string, payload string) {
	h.t.Helper()
	h.run("nodes", "update", nodeID, payload)
}

func (h *cliHarness) updateNodeStdin(nodeID string, payload string) {
	h.t.Helper()
	h.runWithStdin(payload, "nodes", "update", nodeID)
}

func (h *cliHarness) updateKeywords(nodeID string, keywords string) {
	h.t.Helper()
	h.run("nodes", "keywords", "update", nodeID, keywords)
}

func (h *cliHarness) updateKeywordsStdin(nodeID string, keywords string) {
	h.t.Helper()
	h.runWithStdin(keywords, "nodes", "keywords", "update", nodeID)
}

func (h *cliHarness) listSchemas(args ...string) []schemaRecord {
	h.t.Helper()
	out := h.run(append([]string{"schemas", "list"}, args...)...)
	return decodeJSON[[]schemaRecord](h.t, out.Stdout)
}

func (h *cliHarness) listNodes(args ...string) []nodeRecord {
	h.t.Helper()
	out := h.run(append([]string{"nodes", "list"}, args...)...)
	return decodeJSON[[]nodeRecord](h.t, out.Stdout)
}

func (h *cliHarness) getKeywords(nodeID string) []string {
	h.t.Helper()
	out := h.run("nodes", "keywords", "get", nodeID)
	return decodeJSON[[]string](h.t, out.Stdout)
}

func (h *cliHarness) searchNodes(args ...string) []string {
	h.t.Helper()
	out := h.run(append([]string{"nodes", "search"}, args...)...)
	return decodeJSON[[]string](h.t, out.Stdout)
}

func (h *cliHarness) graphGet(args ...string) []nodeRecord {
	h.t.Helper()
	out := h.run(append([]string{"graph", "get"}, args...)...)
	return decodeJSON[[]nodeRecord](h.t, out.Stdout)
}

func (h *cliHarness) graphSearch(args ...string) []nodeRecord {
	h.t.Helper()
	out := h.run(append([]string{"graph", "search"}, args...)...)
	return decodeJSON[[]nodeRecord](h.t, out.Stdout)
}

func (h *cliHarness) readReplicaEvents() []eventRecord {
	h.t.Helper()
	f, err := os.Open(h.replicaPath)
	require.NoError(h.t, err)
	defer f.Close()

	var events []eventRecord
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		events = append(events, decodeJSON[eventRecord](h.t, line))
	}
	require.NoError(h.t, s.Err())
	return events
}

func decodeJSON[T any](t *testing.T, raw string) T {
	t.Helper()
	var v T
	require.NoError(t, json.Unmarshal([]byte(raw), &v), "raw json: %s", raw)
	return v
}

func requireRFC3339(t *testing.T, value string) {
	t.Helper()
	_, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err, "expected RFC3339 timestamp, got %q", value)
}

func TestCLIEndToEnd(t *testing.T) {
	t.Run("commands fail before init and config lifecycle works after init", func(t *testing.T) {
		h := newCLIHarness(t)

		errText := h.runErr("config", "get")
		require.Contains(t, errText, "synapse is not initialized")
		errText = h.runErr("schemas", "list")
		require.Contains(t, errText, "synapse is not initialized")

		initOut := h.run("init")
		require.Equal(t, "synapse initialized", initOut.Stdout)

		cfg := h.getConfig()
		require.Equal(t, "file", cfg.Logging.Logger.Type)
		require.Equal(t, "disabled", cfg.Replication.Mode)
		require.Nil(t, cfg.Replication.Replicator)

		schemaOut := h.run("config", "schema")
		require.Contains(t, schemaOut.Stdout, `"$schema"`)
		require.Contains(t, schemaOut.Stdout, `"replication"`)

		updated := h.updateConfig(fmt.Sprintf(`{
			"logging": {"logger": {"type": "file", "config": {"path": %q}}},
			"replication": {
				"mode": "manual",
				"replicator": {"name": "events_jsonl", "type": "file", "config": {"path": %q}}
			}
		}`, h.logPath, h.replicaPath))
		require.Equal(t, h.logPath, updated.Logging.Logger.Config.Path)
		require.Equal(t, "manual", updated.Replication.Mode)
		require.NotNil(t, updated.Replication.Replicator)
		require.Equal(t, h.replicaPath, updated.Replication.Replicator.Config.Path)

		updated = h.updateConfigStdin(fmt.Sprintf(`{
			"logging": {"logger": {"type": "file", "config": {"path": %q}}},
			"replication": {"mode": "disabled", "replicator": null}
		}`, filepath.Join(h.root, "logs", "stdin.jsonl")))
		require.Equal(t, "disabled", updated.Replication.Mode)
		require.Nil(t, updated.Replication.Replicator)
		require.Equal(t, filepath.Join(h.root, "logs", "stdin.jsonl"), updated.Logging.Logger.Config.Path)

		errText = h.runErr("config", "update", `{"replication":{"mode":"weird"}}`)
		require.Contains(t, errText, "validate config")

		errText = h.runErr("config", "update")
		require.Contains(t, errText, "provide json payload as an argument or pipe it via stdin")
	})

	t.Run("schemas and nodes lifecycle with live archived list and stdin payloads", func(t *testing.T) {
		h := newCLIHarness(t)
		h.init()

		personSchemaID := h.addSchema("person", `{"type":"object","properties":{"name":{"type":"string"},"role":{"type":"string"}},"required":["name"]}`)
		companySchemaID := h.addSchemaStdin("company", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)

		schemas := h.listSchemas()
		require.Len(t, schemas, 2)
		require.ElementsMatch(t, []string{"person", "company"}, []string{schemas[0].Name, schemas[1].Name})

		adaID := h.addNode(personSchemaID, `{"name":"Ada","role":"engineer"}`)
		graceID := h.addNodeStdin(personSchemaID, `{"name":"Grace","role":"scientist"}`)
		_ = graceID
		acmeID := h.addNode(companySchemaID, `{"name":"Acme"}`)
		_ = acmeID

		nodes := h.listNodes("--schema-id", personSchemaID)
		require.Len(t, nodes, 2)
		require.Equal(t, personSchemaID, nodes[0].SchemaID)
		requireRFC3339(t, nodes[0].CreatedAt)
		require.Empty(t, nodes[0].ArchivedAt)

		h.updateNode(adaID, `{"name":"Ada Lovelace","role":"engineer"}`)
		h.updateNodeStdin(adaID, `{"name":"Ada Lovelace","role":"mathematician"}`)
		h.updateKeywords(adaID, `["ada","analysis engine"]`)
		h.updateKeywordsStdin(adaID, `["ada","math"]`)
		require.Equal(t, []string{"ada", "math"}, h.getKeywords(adaID))

		h.run("nodes", "archive", adaID)
		liveNodes := h.listNodes("--schema-id", personSchemaID)
		require.Len(t, liveNodes, 1)
		require.Equal(t, graceID, liveNodes[0].ID)
		archivedNodes := h.listNodes("--schema-id", personSchemaID, "--archived")
		require.Len(t, archivedNodes, 1)
		require.Equal(t, adaID, archivedNodes[0].ID)
		requireRFC3339(t, archivedNodes[0].ArchivedAt)

		h.run("schemas", "archive", companySchemaID)
		liveSchemas := h.listSchemas()
		require.Len(t, liveSchemas, 1)
		require.Equal(t, personSchemaID, liveSchemas[0].ID)
		archivedSchemas := h.listSchemas("--archived")
		require.Len(t, archivedSchemas, 1)
		require.Equal(t, companySchemaID, archivedSchemas[0].ID)
		requireRFC3339(t, archivedSchemas[0].ArchivedAt)
	})

	t.Run("node search and graph traversal through links", func(t *testing.T) {
		h := newCLIHarness(t)
		h.init()

		schemaID := h.addSchema("note", `{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"}},"required":["title"]}`)
		alphaID := h.addNode(schemaID, `{"title":"Alpha","body":"blue fox"}`)
		betaID := h.addNode(schemaID, `{"title":"Beta","body":"blue moon"}`)
		gammaID := h.addNode(schemaID, `{"title":"Gamma","body":"green field"}`)

		h.updateKeywords(alphaID, `["blue","fox"]`)
		h.updateKeywords(betaID, `["blue","moon"]`)
		h.updateKeywords(gammaID, `["green"]`)

		h.run("links", "add", alphaID, betaID)
		h.run("links", "add", betaID, gammaID)

		hits := h.searchNodes("blue")
		require.ElementsMatch(t, []string{alphaID, betaID}, hits)

		graph := h.graphGet(alphaID, "--depth", "2", "--breadth", "5")
		require.Len(t, graph, 3)
		require.ElementsMatch(t, []string{alphaID, betaID, gammaID}, []string{graph[0].ID, graph[1].ID, graph[2].ID})

		searchGraph := h.graphSearch("blue", "--search-limit", "1", "--depth", "1", "--breadth", "5")
		require.Len(t, searchGraph, 2)

		h.run("links", "remove", alphaID, betaID)
		graphAfterUnlink := h.graphGet(alphaID, "--depth", "2", "--breadth", "5")
		require.Len(t, graphAfterUnlink, 1)
		require.Equal(t, alphaID, graphAfterUnlink[0].ID)
	})

	t.Run("manual replication requires init and restore fails fast when not initialized", func(t *testing.T) {
		h := newCLIHarness(t)
		h.init()
		h.updateConfig(fmt.Sprintf(`{
			"logging": {"logger": {"type": "file", "config": {"path": %q}}},
			"replication": {
				"mode": "manual",
				"replicator": {"name": "events_jsonl", "type": "file", "config": {"path": %q}}
			}
		}`, h.logPath, h.replicaPath))

		schemaID := h.addSchema("person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)
		nodeID := h.addNode(schemaID, `{"name":"Ada"}`)
		h.updateKeywords(nodeID, `["ada"]`)
		h.run("replication", "run")

		events := h.readReplicaEvents()
		require.Len(t, events, 3)
		require.Equal(t, int64(1), events[0].GlobalPosition)
		require.Equal(t, int64(2), events[1].GlobalPosition)
		require.Equal(t, int64(3), events[2].GlobalPosition)
		requireRFC3339(t, events[0].OccurredAt)
		requireRFC3339(t, events[0].RecordedAt)

		fresh := newCLIHarness(t)
		errText := fresh.runErr("replication", "restore")
		require.Contains(t, errText, "synapse is not initialized")

		errText = fresh.runErr("config", "update", fmt.Sprintf(`{
			"logging": {"logger": {"type": "file", "config": {"path": %q}}},
			"replication": {
				"mode": "manual",
				"replicator": {"name": "events_jsonl", "type": "file", "config": {"path": %q}}
			}
		}`, fresh.logPath, h.replicaPath))
		require.Contains(t, errText, "synapse is not initialized")
	})

	t.Run("auto replication writes replica and explicit projections are harmless", func(t *testing.T) {
		h := newCLIHarness(t)
		h.init()
		h.updateConfig(fmt.Sprintf(`{
			"logging": {"logger": {"type": "file", "config": {"path": %q}}},
			"replication": {
				"mode": "auto",
				"replicator": {"name": "events_jsonl", "type": "file", "config": {"path": %q}}
			}
		}`, h.logPath, h.replicaPath))

		schemaID := h.addSchema("person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)
		nodeID := h.addNode(schemaID, `{"name":"Auto Ada"}`)
		h.updateKeywords(nodeID, `["auto"]`)
		h.run("projections", "run")

		events := h.readReplicaEvents()
		require.Len(t, events, 3)
		require.FileExists(t, h.logPath)
		storedNodes := h.listNodes("--schema-id", schemaID)
		require.Len(t, storedNodes, 1)
		require.Equal(t, []string{"auto"}, storedNodes[0].Keywords)
	})

	t.Run("errors are surfaced for invalid command usage and invalid payloads", func(t *testing.T) {
		h := newCLIHarness(t)
		h.init()
		schemaID := h.addSchema("person", `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)
		nodeID := h.addNode(schemaID, `{"name":"Ada"}`)

		errText := h.runErr("schemas", "add", `{"type":"object"}`)
		require.Contains(t, errText, "--name is required")

		errText = h.runErr("nodes", "add", `{"name":"Ada"}`)
		require.Contains(t, errText, "--schema-id is required")

		errText = h.runErr("nodes", "list")
		require.Contains(t, errText, "--schema-id is required")

		errText = h.runErr("nodes", "update", nodeID, `{"name":1}`)
		require.Contains(t, errText, "validate node payload against schema")

		errText = h.runErr("nodes", "keywords", "update", nodeID, `{"bad":true}`)
		require.Contains(t, errText, "cannot unmarshal object into Go value of type []string")

		errText = h.runErr("graph", "get", nodeID, "--depth", "-1")
		require.Contains(t, errText, "depth must be non-negative")

		errText = h.runErr("graph", "get", nodeID, "--breadth", "0")
		require.Contains(t, errText, "breadth must be positive")

		errText = h.runErr("links", "add", nodeID, nodeID)
		require.Contains(t, errText, "from and to must be different")

		h.run("replication", "run")
	})
}
