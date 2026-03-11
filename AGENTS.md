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
