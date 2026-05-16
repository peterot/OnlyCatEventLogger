#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
exec "$ROOT/.venv/bin/python3" "$SCRIPT_DIR/export_from_sheets.py" "$@"
