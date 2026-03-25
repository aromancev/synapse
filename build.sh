#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$ROOT_DIR/.bin"
OUT_FILE="$OUT_DIR/synapse"

mkdir -p "$OUT_DIR"

go build -o "$OUT_FILE" ./cmd/synapse

echo "Built $OUT_FILE"
