#!/bin/bash
set -e

MANIFEST=$1
UPDATED_CHATEAUX=$2 # Comma separated
OUTPUT_DIR=$3
OSM_CHUNKS=$4
GENTIAN_BIN=$5

if [ -z "$GENTIAN_BIN" ]; then
    GENTIAN_BIN="cargo run --release --bin gentian --"
fi

echo "Updating chateaux: $UPDATED_CHATEAUX"

if [ ! -f "$MANIFEST" ]; then
    echo "Manifest not found at $MANIFEST"
    exit 1
fi

# Convert comma separated to array
IFS=',' read -ra UPDATED_ARR <<< "$UPDATED_CHATEAUX"

# Identify Affected Partitions
AFFECTED_PARTITIONS=""
for chateau in "${UPDATED_ARR[@]}"; do
    # Use jq to find partitions for this chateau
    # manifest.chateau_to_partitions["chateau"] -> [pids]
    PIDS=$(jq -r --arg c "$chateau" '.chateau_to_partitions[$c] | .[]?' "$MANIFEST")
    if [ -n "$PIDS" ]; then
        AFFECTED_PARTITIONS="$AFFECTED_PARTITIONS $PIDS"
    else
        echo "Warning: Chateau $chateau not found in manifest."
    fi
done

# Deduplicate partitions
AFFECTED_PARTITIONS=$(echo "$AFFECTED_PARTITIONS" | tr ' ' '\n' | sort -u | tr '\n' ' ')
echo "Affected partitions: $AFFECTED_PARTITIONS"

if [ -z "$AFFECTED_PARTITIONS" ]; then
    echo "No affected partitions found."
    exit 0
fi

# Identify Working Set of Chateaux
WORKING_SET=""
for pid in $AFFECTED_PARTITIONS; do
    # manifest.partition_to_chateaux[pid] -> [chateaux]
    CHATEAUX=$(jq -r --arg p "$pid" '.partition_to_chateaux[$p] | .[]?' "$MANIFEST")
    WORKING_SET="$WORKING_SET $CHATEAUX"
done

# Add updated chateaux explicitly
WORKING_SET="$WORKING_SET ${UPDATED_ARR[@]}"

# Deduplicate chateaux
WORKING_SET=$(echo "$WORKING_SET" | tr ' ' '\n' | sort -u | paste -sd "," -)
echo "Working set of chateaux: $WORKING_SET"

# Run Gentian
echo "Running Gentian..."
$GENTIAN_BIN --chateau "$WORKING_SET" --osm-chunks "$OSM_CHUNKS" --output "$OUTPUT_DIR"

# Run Stitch
echo "Running Stitch..."
$GENTIAN_BIN --stitch --output "$OUTPUT_DIR" --chateau dummy --osm-chunks "$OSM_CHUNKS"

echo "Update complete."
