# AGENTS.md

## Development workflow (mandatory)

After **every code change**:

1. Run formatting:
   - `go fmt ./...`
2. Run tests:
   - `go test ./...`

Do not open/update a PR with failing format or test checks.

## Test style

- In tests, prefer table-driven or focused `t.Run(...)` subtests when there are multiple scenarios or lifecycle phases worth naming explicitly.
- Keep each subtest narrow: one behavior, one failure mode, or one lifecycle step.

## Domain package boundaries

- Domain packages under `internal/domains/events/*` may import only their parent package: `internal/domains/events`.
- They must not import sibling domain packages.
- Example: `nodes` may import `events`, but must not import `links` or `schemas`.
- If logic needs data from another domain, compose it in a higher layer (for example, the service layer) instead of coupling domain packages directly.
