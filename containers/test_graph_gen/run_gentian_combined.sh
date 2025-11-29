#!/bin/bash
set -e

# Wait for Postgres
until pg_isready -h postgres -U catenary; do
  echo "Waiting for postgres..."
  sleep 2
done

echo "Postgres is ready."

# Setup directories
mkdir -p /catenary/graph_output
# Ensure OSM chunks directory exists (should be populated by previous runs)
mkdir -p /catenary/osm_chunks

echo "Running Gentian (Graph Generation) for ALL Chateaux..."
export PGPASSWORD=catenary

# Fetch Chateaus that have routes
CHATEAUS=$(psql -h postgres -U catenary -d catenary -t -c "SELECT c.chateau FROM gtfs.chateaus c WHERE EXISTS (SELECT 1 FROM gtfs.routes r WHERE r.chateau = c.chateau);")

# Join with commas
# xargs trims whitespace and joins with spaces. tr converts spaces to commas.
CHATEAU_LIST=$(echo "$CHATEAUS" | xargs | tr ' ' ',')

if [ -n "$CHATEAU_LIST" ]; then
    echo "Running Gentian for: $CHATEAU_LIST"
    /catenary/output-binaries/gentian \
        --chateau "$CHATEAU_LIST" \
        --osm-chunks /catenary/osm_chunks \
        --output /catenary/graph_output
else
    echo "No chateaux found."
fi

echo "Gentian Run Complete!"
