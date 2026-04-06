---
name: synapse
description: Associative structured local memory store. Use to remember and recall anything and retrieve linked memories.
---

# Synapse

Use the `synapse` CLI as a structured local memory and graph store.

Keep workflows boring and explicit:

- initialize once with `synapse init`
- define schemas before adding nodes
- treat node payload updates as full replacement
- use `jq` for filtering and patch-like transforms
- use search for seed IDs, then graph traversal for context expansion
- treat projections as derived state and events as source of truth

## Core model

Synapse has three main concepts:

1. **Schemas**
   - runtime JSON Schemas
   - define the allowed structure of node payloads
   - archived schemas remain in history

2. **Nodes**
   - typed records validated against a schema
   - have payload, keywords, and archive state
   - payload updates replace the entire payload

3. **Links**
   - connections between node pairs
   - used for graph traversal
   - self-links are invalid

IDs are prefixed and human-readable:

- `schema_<...>`
- `node_<...>`
- `event_<...>`

## Command map

### Initialize

```bash
synapse init
```

Use `--db-path` to point at a non-default SQLite file:

```bash
synapse --db-path /path/to/synapse.db init
```

Default DB path is `.synapse/db`.

### Config

Read current config:

```bash
synapse config get
```

Update config from JSON argument or stdin:

```bash
cat config.json | synapse config update
```

Print config schema:

```bash
synapse config schema
```

### Schemas

Add a schema:

```bash
cat schema.json | synapse schemas add --name task
```

List schemas:

```bash
synapse schemas list
synapse schemas list --archived
```

Archive a schema:

```bash
synapse schemas archive <schema-id>
```

### Nodes

Add a node:

```bash
cat payload.json | synapse nodes add --schema-id <schema-id>
```

List nodes for a schema:

```bash
synapse nodes list --schema-id <schema-id>
synapse nodes list --schema-id <schema-id> --archived
```

Update a node payload:

```bash
cat payload.json | synapse nodes update <node-id>
```

Archive a node:

```bash
synapse nodes archive <node-id>
```

Search node IDs:

```bash
synapse nodes search '{"query":"keyword phrase"}' --limit 20
```

### Keywords

Read node keywords:

```bash
synapse nodes keywords get <node-id>
```

Replace node keywords:

```bash
cat keywords.json | synapse nodes keywords update <node-id>
```

### Links and graph

Create or remove links:

```bash
synapse links add <from-node-id> <to-node-id>
synapse links remove <from-node-id> <to-node-id>
```

Traverse from explicit node IDs:

```bash
synapse graph get '["<node-id>"]' --depth 2 --breadth 10
```

Search first, then traverse:

```bash
synapse graph search '{"query":"keyword phrase"}' --search-limit 5 --depth 2 --breadth 10
```

### Projections and replication

Run projections explicitly:

```bash
synapse projections run
```

Run replication explicitly:

```bash
synapse replication run
```

Restore from configured replicator into an empty event store:

```bash
synapse replication restore
```

## JSON input and query shapes

Commands that accept JSON can read it either:

- as a positional argument
- from `stdin`

Structured query commands use JSON too.

Examples:

```bash
synapse schemas add --name note '{"type":"object","properties":{"title":{"type":"string"}},"required":["title"]}'
synapse nodes add --schema-id "$SCHEMA_ID" '{"title":"Write README","status":"todo"}'
synapse nodes update "$NODE_ID" '{"title":"Write README","status":"done"}'
synapse nodes search '{"query":"readme"}'
synapse graph get '["node_01..."]'
synapse graph search '{"query":"engineering"}' --depth 2
```

`stdin` form:

```bash
cat schema.json | synapse schemas add --name note
cat payload.json | synapse nodes add --schema-id "$SCHEMA_ID"
cat payload.json | synapse nodes update "$NODE_ID"
cat keywords.json | synapse nodes keywords update "$NODE_ID"
```

## jq workflow patterns

Use `jq` heavily. Synapse returns JSON, and updates are full replacement.

### Filter nodes

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -r '.[] | select(.payload.status == "todo") | .payload.title'
```

### Patch-like payload update

Do not send partial payloads unless the partial object is valid as the full payload.

Preferred pattern:

```bash
synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -c --arg id "$NODE_ID" '
      .[]
      | select(.id == $id)
      | .payload
      | .status = "done"
    ' \
  | synapse nodes update "$NODE_ID"
```

### Append keywords safely

```bash
synapse nodes keywords get "$NODE_ID" \
  | jq -c '. + ["new-keyword"] | unique' \
  | synapse nodes keywords update "$NODE_ID"
```

### Select due tasks

If task payloads use ISO dates:

```bash
TODAY=$(date +%F)

synapse nodes list --schema-id "$TASK_SCHEMA_ID" \
  | jq -r --arg today "$TODAY" '
      .[]
      | select(.payload.status != "done")
      | select(.payload.due != null and .payload.due <= $today)
      | .id
    '
```

## Typical task workflow

For task tracking or agent memory, use this order:

1. add a schema such as `task`, `person`, `project`, or `note`
2. add nodes with valid payloads
3. add keywords that make search practical
4. create links between related nodes
5. use `nodes search` or `graph search` to find starting points
6. use `graph get` to expand by explicit IDs
7. archive instead of deleting when something should disappear from normal reads

## Replication workflow

Replication is event-based, not projection-based.

Config has three modes:

- `disabled` — no replication
- `manual` — write events locally, run `synapse replication run` yourself
- `auto` — mutating commands replicate automatically after appending events

Current replicator type is file-based JSONL.

Recommended pattern:

1. configure logging and replication
2. use `manual` if you want explicit checkpointed backup behavior
3. use `auto` if immediate replication is worth the extra write cost
4. keep replica files somewhere durable
5. restore only into a fresh empty database

## Event sourcing architecture

Synapse is event-sourced.

The source of truth is the append-only event log, not the latest row in a mutable table.

Write path:

1. load the stream for the target aggregate
2. replay prior events into the aggregate
3. validate invariants and schema constraints
4. record new events
5. append events transactionally
6. run replication if configured
7. run projections so reads stay current

Read path:

1. query projections, not raw streams
2. use search to find seed nodes
3. traverse links for related context

Why this matters:

- history is auditable
- projections are rebuildable
- replication is straightforward
- restore is deterministic
- invariants stay inside aggregate logic instead of leaking into ad hoc update code

## Sharp edges

Remember these:

- run `synapse init` before almost anything else
- `nodes list` requires `--schema-id`
- `nodes update` replaces the full payload
- `nodes keywords update` replaces the full keyword array
- archived nodes are excluded from normal graph traversal
- `graph get` rejects negative depth and non-positive breadth
- `links add` rejects self-links
- `replication restore` requires an empty target event store

## Preferred style

Prefer simple shell pipelines over wrappers.

Good:

- `synapse ... | jq ...`
- JSON payloads via stdin
- explicit `--db-path` when scripting against multiple databases

Avoid:

- hidden partial updates
- assuming search is semantic/vector-based
- editing projection tables directly
- restoring into a non-empty database
