#!/bin/bash
set -e

# Assume DB/OSM ready

# 1. Initial Full Generation
echo "Running Initial Generation (All Chateaux)..."
# Fetch all chateaux
CHATEAUS=$(psql -h postgres -U catenary -d catenary -t -c "SELECT c.chateau FROM gtfs.chateaus c WHERE EXISTS (SELECT 1 FROM gtfs.routes r WHERE r.chateau = c.chateau);")
# Join with comma
ALL_CHATEAUX=$(echo $CHATEAUS | tr '\n' ',' | sed 's/,$//')

# Use cargo run for gentian
GENTIAN_CMD="cargo run --release --bin gentian --"

echo "Running Gentian for: $ALL_CHATEAUX"
$GENTIAN_CMD \
    --chateau "$ALL_CHATEAUX" \
    --osm-chunks /catenary/osm_chunks \
    --output /catenary/graph_output

# Verify manifest exists
if [ ! -f "/catenary/graph_output/manifest.json" ]; then
    echo "Error: manifest.json not found!"
    exit 1
fi

# 2. Simulate Update
echo "Simulating Update for one chateau..."
# Pick first chateau
UPDATE_TARGET=$(echo $CHATEAUS | awk '{print $1}')
echo "Updating: $UPDATE_TARGET"

chmod +x /catenary/containers/test_graph_gen/update_graph.sh

/catenary/containers/test_graph_gen/update_graph.sh \
    /catenary/graph_output/manifest.json \
    "$UPDATE_TARGET" \
    /catenary/graph_output \
    /catenary/osm_chunks \
    "$GENTIAN_CMD"

echo "Update Workflow Test Complete!"
