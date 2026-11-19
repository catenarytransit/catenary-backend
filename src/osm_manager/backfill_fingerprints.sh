#!/usr/bin/env bash
# Backfill published BLAKE3 + legacy SHA-256 OSR identities without recompilation.
set -euo pipefail
CONFIG="${1:-catenaryconfig.toml}"
MANAGER="src/osm_manager/target/release/osm_manager"
if [[ ! -x "$MANAGER" ]]; then
  cargo build --release --manifest-path src/osm_manager/Cargo.toml --bin osm_manager
fi
"$MANAGER" --config "$CONFIG" fingerprint
echo "OSR graph identity backfill complete. Restart Edelweiss after deployment."
