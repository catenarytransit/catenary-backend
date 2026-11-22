#!/usr/bin/env bash
# Applies migrations/*/up.sql in order, exactly like the "Run Database
# Migrations" step in .github/workflows/full-test-pipeline.yml.
#
# Usage (from repo root, inside `pixi shell`, with docker-compose up):
#   bash docs/dev/run-migrations.sh
#   # or: pixi run migrate
#
# Override any of these if you changed docs/dev/docker-compose.yml:
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-catenary}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

for d in $(find migrations -mindepth 1 -maxdepth 1 -type d | sort); do
  echo "Running migration $d/up.sql..."
  psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$d/up.sql"
done
