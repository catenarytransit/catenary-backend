#!/bin/bash
set -e

# Configuration
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_DB="catenary"
# Default to postgres/postgres, user can override
export PGPASSWORD="${PGPASSWORD:-postgres}"

WORK_DIR="$(pwd)"
OUTPUT_DIR="$WORK_DIR/output"

# Ensure output directory exists (should be populated by graph gen)
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Warning: $OUTPUT_DIR does not exist. Have you ran run_local_graph_gen.sh?"
    mkdir -p "$OUTPUT_DIR"
fi

echo "=== Checking Dependencies ==="
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed or not in PATH."
    exit 1
fi

echo "=== Building Edelweiss ==="
cargo build --release --bin edelweiss

EDELWEISS_BIN="./target/release/edelweiss"

echo "=== Starting Edelweiss ==="
export DATABASE_URL="postgres://$POSTGRES_USER:$PGPASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
export GRAPH_DIR="$OUTPUT_DIR"
export RUST_LOG="info,edelweiss=debug" # Helpful for debugging

echo "DATABASE_URL: $DATABASE_URL"
echo "GRAPH_DIR: $GRAPH_DIR"

"$EDELWEISS_BIN"
