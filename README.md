# Synapse

Synapse is an agentic memory system for LLM agents. It provides structured, queryable long-term memory through a graph-based architecture inspired by the [A-MEM paper](https://arxiv.org/abs/2502.12110) and Zettelkasten method.

## Core Concept

Synapse separates memory into three layers:

1. **Schemas** — Dynamic type definitions that enforce structure on memory entities
2. **Nodes** — Immutable facts, observations, and structured knowledge
3. **Links** — Mutable connections that evolve as the agent learns

This design enables agents to accumulate structured knowledge while allowing the relationships between memories to adapt over time.

## Philosophy

- **Immutable facts**: Once stored, a memory node never changes. Evolution happens by creating new nodes and updating links.
- **Dynamic structure**: Node types can be defined at runtime via JSON Schema, allowing the memory system to grow with the agent's needs.
- **Graph-native**: Everything is connected. Retrieval starts with semantic similarity (via external index) and expands through traversable links.

## Retrieval Flow

```
User Query
    ↓
Semantic Search (external/indexed)
    ↓
First Frontier (candidate nodes)
    ↓
Graph Traversal (links)
    ↓
Ranked Context
```

The CLI focuses on graph manipulation. Semantic retrieval is handled by existing memory mechanisms, with the CLI emitting indexable text for each node.

## Status

Under active development. The core graph operations (schema management, node creation, link manipulation) are being implemented incrementally.

## Contributing

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for commit message formatting.

### Commit Message Format

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Common Types

- `feat` — new features
- `fix` — bug fixes
- `chore` — maintenance tasks
- `docs` — documentation changes
- `refactor` — code refactoring
- `test` — adding or updating tests

### Examples

```
feat: add user authentication
fix(api): resolve connection timeout issue
docs: update README with setup instructions
```

## License

MIT
