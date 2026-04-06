#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
BUILD_DIR="$ROOT_DIR/.bin"
OUT_FILE="$BUILD_DIR/synapse"
LINK_FILE="$BIN_DIR/synapse"

mkdir -p "$BUILD_DIR" "$BIN_DIR"

cd "$ROOT_DIR"
go build -o "$OUT_FILE" ./cmd/synapse
ln -sf "$OUT_FILE" "$LINK_FILE"

cat <<EOF
Installed synapse:
  binary: $OUT_FILE
  link:   $LINK_FILE
EOF

case ":${PATH}:" in
  *":$BIN_DIR:"*) ;;
  *)
    echo
    echo "Warning: $BIN_DIR is not on PATH in this shell."
    echo "Add this to your shell config:"
    echo "  export PATH=\"$BIN_DIR:\$PATH\""
    ;;
esac
