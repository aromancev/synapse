# Synapse

Synapse is a local event-sourced knowledge graph for agents.

It lets you define JSON Schemas at runtime, store typed nodes, connect them with links, search by keywords, traverse the graph, and replicate the append-only event log elsewhere.

## What it does well

- stores structured data locally in SQLite
- validates node payloads against runtime JSON Schemas
- keeps an append-only event log as the source of truth
- rebuilds queryable read models through projections
- supports simple file-based event replication
- plays nicely with `jq`

## What it is not

- not a vector database
- not a full-text search engine for arbitrary prose at massive scale
- not an ORM with partial patch semantics

The current CLI is intentionally boring: write events, rebuild projections, query JSON, and let shell tools do the rest.

## Installation

### Requirements

- Go 1.26+
- SQLite is embedded via `modernc.org/sqlite`, so you do not need a system SQLite dependency

### Install globally with Go

```bash
go install github.com/aromancev/synapse@latest
```

That installs the `synapse` binary into your Go bin directory.
Make sure that directory is on your `PATH`.

A common setup is:

```bash
export PATH="$(go env GOPATH)/bin:$PATH"
```

If you use `GOBIN`, the binary will be installed there instead.

### Build from source

```bash
git clone https://github.com/aromancev/synapse.git
cd synapse
go build -o synapse .
```

Or run it without building:

```bash
go run . --help
```

### Initialize a database

By default Synapse stores data in `.synapse/db`.

```bash
synapse init
```

Use a custom database path when needed:

```bash
synapse --db-path /path/to/synapse.db init
```

That creates the SQLite database and initializes:

- the event store
- projection iterators
- replication iterators
- config storage
- schema, node, and link projections

## Usage

### Command overview

```bash
synapse init
synapse config get
synapse config update
synapse config schema
synapse schemas add
synapse schemas list
synapse schemas archive
synapse nodes add
synapse nodes list
synapse nodes update
synapse nodes archive
synapse nodes search
synapse nodes keywords get
synapse nodes keywords update
synapse links add
synapse links remove
synapse graph get
synapse graph search
synapse projections run
synapse replication run
synapse replication restore
```

### How data flows

Write commands do three things:

1. validate and append events to the event store
2. optionally replicate those events if replication mode is `auto`
3. run projections to refresh queryable read models

Read commands query projections, not raw events.

Mutating commands return small JSON result envelopes like:

```json
{"status":"ok","action":"node_added","id":"node_...","schema_id":"schema_..."}
```

### JSON input

Commands that accept JSON can read it either:

- as a positional argument
- from `stdin`

Structured query commands use JSON too. For example:

```bash
synapse nodes search '{"query":"readme"}'
synapse graph get '["node_01...","node_02..."]'
synapse graph search '{"query":"engineering"}' --depth 2
```

Argument form:

```bash
synapse schemas add --name note '{"type":"object","properties":{"title":{"type":"string"}},"required":["title"]}'
```

`stdin` form:

```bash
cat schema.json | synapse schemas add --name note
```

### Default paths

- database: `.synapse/db`
- log file: `.synapse/log.jsonl`
- replica file: `.synapse/replica.jsonl`

## Examples

### Basic example

Create a schema, add a couple of nodes, attach keywords, and list them.

```bash
synapse init

TASK_SCHEMA_ID=$(synapse schemas add --name task '{
  "type": "object",
  "properties": {
    "title": {"type": "string"},
    "status": {"type": "string"},
    "due": {"type": "string"}
  },
  "required": ["title", "status"]
}')

echo "$TASK_SCHEMA_ID"

TASK_1=$(synapse nodes add --schema-id "$TASK_SCHEMA_ID" '{
  "title": "Write README",
  "status": "todo",
  "due": "2026-03-29"
}')

TASK_2=$(synapse nodes add --schema-id "$TASK_SCHEMA_ID" '{
  "title": "Review replication flow",
  "status": "doing"
}')

synapse nodes keywords update "$TASK_1" '["docs", "readme", "writing"]'
synapse nodes keywords update "$TASK_2" '["replication", "ops"]'

synapse schemas list
synapse nodes list --schema-id "$TASK_SCHEMA_ID"
synapse nodes keywords get "$TASK_1"
```

Typical `nodes list` output looks like this:

```json
[
  {
    "id": "node_...",
    "schema_id": "schema_...",
    "created_at": "2026-03-28T15:48:00Z",
    "payload": {
      "title": "Write README",
      "status": "todo",
      "due": "2026-03-29"
    },
    "keywords": ["docs", "readme", "writing"]
  }
]
```

### Using with jq to filter data

Synapse returns JSON. Lean into that.

List only todo task titles:

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -r '.[] | select(.payload.status == "todo") | .payload.title'
```

List overdue or due-today tasks if your payload uses ISO dates:

```bash
TODAY=$(date +%F)

synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq --arg today "$TODAY" '
      .[]
      | select(.payload.due != null and .payload.due <= $today)
    '
```

Find nodes by query through the built-in search, then inspect them:

```bash
synapse nodes search '{"query":"readme"}' \
  | jq -r '.[]'
```

### Using with jq to update data

`nodes update` replaces the full payload. That is deliberate. No server-side patch magic.

Update a node by fetching it, transforming the payload with `jq`, and writing the whole thing back:

```bash
NODE_ID="$TASK_1"

synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -c --arg id "$NODE_ID" '.[] | select(.id == $id) | .payload | .status = "done"' \
  | synapse nodes update "$NODE_ID"
```

Add a new field while preserving the rest:

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -c --arg id "$NODE_ID" '
      .[]
      | select(.id == $id)
      | .payload
      | .completed_at = "2026-03-28"
    ' \
  | synapse nodes update "$NODE_ID"
```

Update keywords with `jq` too:

```bash
synapse nodes keywords get "$NODE_ID" \
  | jq -c '. + ["completed"] | unique' \
  | synapse nodes keywords update "$NODE_ID"
```

### Adding schemas and nodes as tasks

A nice real-world pattern is to model tasks explicitly.

Create a task schema:

```bash
TASK_SCHEMA_ID=$(cat <<'JSON' | synapse schemas add --name task
{
  "type": "object",
  "properties": {
    "title": {"type": "string"},
    "status": {"type": "string", "enum": ["todo", "doing", "done"]},
    "due": {"type": "string"},
    "project": {"type": "string"},
    "notes": {"type": "string"}
  },
  "required": ["title", "status"]
}
JSON
)
```

Add tasks:

```bash
TASK_A=$(cat <<'JSON' | synapse nodes add --schema-id "$TASK_SCHEMA_ID"
{
  "title": "Rewrite README",
  "status": "doing",
  "due": "2026-03-28",
  "project": "synapse"
}
JSON
)

TASK_B=$(cat <<'JSON' | synapse nodes add --schema-id "$TASK_SCHEMA_ID"
{
  "title": "Configure replication",
  "status": "todo",
  "due": "2026-03-30",
  "project": "synapse"
}
JSON
)
```

Update task status with `jq`:

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -c --arg id "$TASK_A" '
      .[]
      | select(.id == $id)
      | .payload
      | .status = "done"
    ' \
  | synapse nodes update "$TASK_A"
```

Select due tasks with `jq`:

```bash
TODAY=$(date +%F)

synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq --arg today "$TODAY" '
      [ .[]
        | select(.payload.status != "done")
        | select(.payload.due != null and .payload.due <= $today)
      ]
    '
```

Select just the IDs of due tasks:

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -r --arg today "$TODAY" '
      .[]
      | select(.payload.status != "done")
      | select(.payload.due != null and .payload.due <= $today)
      | .id
    '
```

### Linking nodes and requesting graph

Links are undirected relationships between nodes. You connect node IDs, then traverse from one or more seed nodes.

Example: create a tiny graph of people and work items.

```bash
PERSON_SCHEMA_ID=$(synapse schemas add --name person '{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "role": {"type": "string"}
  },
  "required": ["name"]
}')

ADA=$(synapse nodes add --schema-id "$PERSON_SCHEMA_ID" '{"name":"Ada","role":"engineer"}')
GRACE=$(synapse nodes add --schema-id "$PERSON_SCHEMA_ID" '{"name":"Grace","role":"scientist"}')

synapse links add "$ADA" "$TASK_A"
synapse links add "$GRACE" "$TASK_B"
synapse links add "$ADA" "$GRACE"
```

Traverse by explicit IDs:

```bash
synapse graph get "[\"$ADA\"]" --depth 2 --breadth 10
```

That returns the seed node plus linked nodes discovered during traversal.

Traverse from search results instead of explicit IDs:

```bash
synapse graph search '{"query":"Ada"}' --search-limit 5 --depth 2 --breadth 10
```

A practical pattern is to search by keywords first, then expand context:

```bash
synapse nodes keywords update "$ADA" '["people", "engineering", "ada"]'
synapse nodes keywords update "$TASK_A" '["task", "docs", "readme"]'

synapse graph search '{"query":"engineering"}' --search-limit 3 --depth 2 --breadth 10
```

Search node IDs only:

```bash
synapse nodes search '{"query":"engineering"}' --limit 10
```

### Configuring replication

Synapse supports a file-based replicator that writes events as JSON Lines.

First inspect the current config:

```bash
synapse config get
```

Print the config JSON Schema:

```bash
synapse config schema
```

Set manual replication:

```bash
cat <<'JSON' | synapse config update
{
  "logging": {
    "logger": {
      "type": "file",
      "config": {
        "path": ".synapse/log.jsonl"
      }
    }
  },
  "replication": {
    "mode": "manual",
    "replicator": {
      "name": "events_jsonl",
      "type": "file",
      "config": {
        "path": ".synapse/replica.jsonl"
      }
    }
  }
}
JSON
```

In `manual` mode, writes do not replicate automatically. Run replication explicitly:

```bash
synapse replication run
```

In `auto` mode, every mutating command replicates after appending events:

```bash
cat <<'JSON' | synapse config update
{
  "logging": {
    "logger": {
      "type": "file",
      "config": {
        "path": ".synapse/log.jsonl"
      }
    }
  },
  "replication": {
    "mode": "auto",
    "replicator": {
      "name": "events_jsonl",
      "type": "file",
      "config": {
        "path": ".synapse/replica.jsonl"
      }
    }
  }
}
JSON
```

Disable replication entirely:

```bash
cat <<'JSON' | synapse config update
{
  "logging": {
    "logger": {
      "type": "file",
      "config": {
        "path": ".synapse/log.jsonl"
      }
    }
  },
  "replication": {
    "mode": "disabled",
    "replicator": null
  }
}
JSON
```

Restore from a replica into an empty database:

```bash
synapse --db-path /tmp/restored.db init
synapse --db-path /tmp/restored.db config update '{
  "logging": {"logger": {"type": "file", "config": {"path": "/tmp/restored.log.jsonl"}}},
  "replication": {
    "mode": "manual",
    "replicator": {
      "name": "events_jsonl",
      "type": "file",
      "config": {"path": ".synapse/replica.jsonl"}
    }
  }
}'
synapse --db-path /tmp/restored.db replication restore
```

Important:

- restore only works when the target event store is empty
- projections are rebuilt after restore
- replication is based on iterators, so reruns are incremental

## Architecture

Synapse uses event sourcing.

That means the source of truth is not the latest row in a `nodes` table. The source of truth is the ordered event stream.

### Core pieces

#### 1. Event store

All state-changing operations append immutable events to the `events` table.

Each event has:

- `id`
- `stream_id`
- `stream_type`
- `stream_version`
- `event_type`
- `event_version`
- `occurred_at` (UTC RFC3339 in external JSON; Unix seconds in storage)
- `recorded_at` (UTC RFC3339 in external JSON; Unix seconds in storage)
- `payload`
- `meta`
- `global_position`

Important properties:

- events are append-only
- each stream has optimistic concurrency via `(stream_id, stream_version)`
- the whole store is globally ordered by `global_position`

#### 2. Streams

Each aggregate instance has its own stream.

Examples:

- one schema stream per schema
- one node stream per node
- one link stream per normalized node pair

A stream is replayed to rebuild aggregate state before new events are recorded.

#### 3. Aggregates

Aggregates enforce invariants.

Examples:

- schema aggregates validate schema lifecycle and archive rules
- node aggregates validate payload updates and keyword updates
- link aggregates prevent bad link state transitions

The write path is roughly:

1. load stream events
2. replay aggregate
3. validate command against current state
4. record new events
5. append events transactionally

#### 4. Projections

Projections build queryable read models from the event store.

Current projections cover:

- schemas
- nodes
- links

Read commands like `schemas list`, `nodes list`, `nodes search`, and graph traversal query these projections.

Projection progress is tracked by per-projection iterators, so projections can resume incrementally.

#### 5. Replication

Replication reads the global event log in order and ships events to a configured replicator.

Current implementation:

- file replicator writing JSONL

Replication progress is tracked by a per-replicator iterator.

That gives you a simple and robust model:

- append events locally
- replicate incrementally
- restore elsewhere
- rebuild projections from the same event history

### Why this architecture

Because it keeps the rules honest.

A few concrete benefits:

- **auditability**: you can inspect the history of changes, not just the latest state
- **rebuildability**: projections are disposable and can be regenerated from events
- **replication friendliness**: append-only logs are much easier to copy than mutable tables
- **clear write semantics**: node updates are full-payload replacements, not hidden patch behavior
- **separation of concerns**: aggregates handle invariants, projections handle query shape

### Current retrieval model

Search is projection-based and keyword-oriented today.

Typical flow:

1. find seed nodes with `nodes search`
2. expand neighborhood with `graph search` or `graph get`
3. filter or reshape with `jq`

This keeps the CLI simple and composable.

## Exit codes and failure modes

A few sharp edges worth knowing:

- most commands fail before `init` with `synapse is not initialized`
- `nodes add` fails if `--schema-id` is missing
- `nodes list` currently requires `--schema-id`
- node payload updates are validated against the schema every time
- `graph get` rejects negative depth and non-positive breadth
- `links add` rejects self-links
- `replication restore` fails unless the target event store is empty

## Data model and IDs

Synapse uses prefixed IDs so streams and entities stay readable in shell scripts and JSON output.

- schemas look like `schema_<...>`
- nodes look like `node_<...>`
- events look like `event_<...>`

A few practical rules:

- each schema has its own event stream
- each node has its own event stream
- each link is stored as a stream for a normalized node pair
- archived schemas and nodes remain in history, but normal list and graph reads exclude archived nodes unless you ask for archived data explicitly

## Update semantics

This part matters.

### Node payload updates are full replacement

`nodes update` replaces the full `payload`.

It does **not** merge fields and it does **not** apply JSON Patch.

So this:

```bash
synapse nodes update "$NODE_ID" '{"status":"done"}'
```

is only valid if `{"status":"done"}` is the complete payload and still satisfies the schema.

If you want patch-like behavior, fetch the current payload, transform it with `jq`, then send the whole result back.

### Keyword updates are also full replacement

`nodes keywords update` replaces the complete keyword array.

If you want to append safely, do it client-side:

```bash
synapse nodes keywords get "$NODE_ID" \
  | jq -c '. + ["new-keyword"] | unique' \
  | synapse nodes keywords update "$NODE_ID"
```

## Search semantics

Current search is keyword/projection based.

That means:

- `nodes search` returns matching node IDs from the searchable node projection
- `graph search` uses search results as seed nodes, then expands through links
- this is not vector search or embedding-based semantic retrieval

The intended pattern today is simple:

1. index useful keywords on nodes
2. search for seed nodes
3. expand graph context
4. finish with `jq`

## Backup and restore workflow

The current happy path is:

1. initialize a local database
2. configure replication to `manual` or `auto`
3. write events normally through the CLI
4. keep the JSONL replica file somewhere durable
5. restore into a fresh empty database when needed

Recommended setup for boring reliability:

- use `manual` if you want explicit control over when replicas are updated
- use `auto` if you want every mutating command to replicate immediately
- treat the replica as a transport and backup artifact for the event history
- treat projections as disposable derived state

## CLI reference

### Global flag

| Flag | Meaning |
| --- | --- |
| `--db-path <path>` | Path to the SQLite database. Default: `.synapse/db` |

### Core commands

| Command | Purpose |
| --- | --- |
| `synapse init` | Initialize database and internal tables |
| `synapse config get` | Print current config as JSON |
| `synapse config update [json]` | Replace config |
| `synapse config schema` | Print config JSON Schema |
| `synapse schemas add --name <name> [json]` | Add a schema |
| `synapse schemas list [--archived]` | List schemas |
| `synapse schemas archive <schema-id>` | Archive a schema |
| `synapse nodes add --schema-id <schema-id> [json]` | Add a node |
| `synapse nodes list --schema-id <schema-id> [--archived]` | List nodes for a schema |
| `synapse nodes update <node-id> [json]` | Replace node payload |
| `synapse nodes archive <node-id>` | Archive a node |
| `synapse nodes search [json-query-object] [--limit N]` | Search node IDs |
| `synapse nodes keywords get <node-id>` | Read keywords |
| `synapse nodes keywords update <node-id> [json-array]` | Replace keywords |
| `synapse links add <from-node-id> <to-node-id>` | Create a link |
| `synapse links remove <from-node-id> <to-node-id>` | Remove a link |
| `synapse graph get [json-node-id-array] [--depth N] [--breadth N]` | Traverse from explicit IDs |
| `synapse graph search [json-query-object] [--search-limit N] [--depth N] [--breadth N]` | Search first, then traverse |
| `synapse projections run` | Rebuild projections incrementally |
| `synapse replication run` | Replicate pending events |
| `synapse replication restore` | Restore event store from configured replicator |

## Notes and sharp edges

- `synapse init` must be run before most commands
- `nodes list` currently requires `--schema-id`
- writes validate node payloads against the referenced schema
- archived nodes are excluded from normal graph traversal
- link creation rejects self-links
- restore requires an empty target event store
- projections and replication are iterator-based, so reruns are incremental

## License

MIT
