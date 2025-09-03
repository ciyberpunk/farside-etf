#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/docs/data"
rsync -a --delete "$ROOT/data/" "$ROOT/docs/data/"
echo "Synced $ROOT/data -> $ROOT/docs/data"
