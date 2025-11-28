#!/bin/bash
set -e

# Wait for Postgres
until pg_isready -h postgres -U catenary; do
  echo "Waiting for postgres..."
  sleep 2
done

echo "Postgres is ready."

echo "Running Diesel Migrations..."
diesel migration run

# Setup directories
mkdir -p /tmp/gtfs_zip
mkdir -p /tmp/gtfs_uncompressed
mkdir -p /data/dmfr_filtered/feeds
mkdir -p /data/dmfr_filtered/operators
mkdir -p /catenary/osm_chunks
mkdir -p /catenary/graph_output
mkdir -p /tmp/osm_temp

# List of feeds to import
FEEDS=(
"f-9mu-orangecountytransportationauthority"
"f-9mu-irvine~ca~us"
"f-9q5-metro~losangeles"
"f-9q5b-longbeachtransit"
"f-9qh-metrolinktrains"
"f-9qh1-foothilltransit"
"f-9qh2s-corona~ca~us"
"f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle"
"f-9mu-northcountytransitdistrict"
"f-9mu-mts"
"f-9qh-riversidetransitagency"
"f-9qh-omnitrans"
"f-9qhf-bigbear~ca~us"
"f-antelope~valley~transit~authority"
"f-9q54-goldcoasttransit"
"f-9q5-ladot"
"f-9q5c-bigbluebus"
"f-9q5c-culvercitybus"
"f-9q5b4-pvpta~ca~us"
"f-9muq-lagunabeach~ca~us"
"f-9q4g-santabarbaramtd"
)

# Join feeds into comma-separated string
printf -v ONLY_FEED_IDS "%s," "${FEEDS[@]}"
export ONLY_FEED_IDS="${ONLY_FEED_IDS%,}"
echo "ONLY_FEED_IDS: $ONLY_FEED_IDS"

if [ ! -d "/dmfr_source" ]; then
    echo "Error: /dmfr_source not found. Cannot import feeds."
    exit 1
fi

echo "Running Maple (GTFS Ingest)..."
export GTFS_ZIP_TEMP=/tmp/gtfs_zip
export GTFS_UNCOMPRESSED_TEMP=/tmp/gtfs_uncompressed
export DELETE_BEFORE_INGEST=true
export FORCE_INGEST_ALL=true

/catenary/output-binaries/maple \
    --transitland /dmfr_source \
    --no-elastic

echo "Running Avens (OSM Preprocessing)..."
/catenary/output-binaries/avens \
    --region north-america/us/california/socal \
    --output-dir /catenary/osm_chunks \
    --temp-dir /tmp/osm_temp

echo "Running Gentian (Graph Generation)..."
export PGPASSWORD=catenary
CHATEAUS=$(psql -h postgres -U catenary -d catenary -t -c "SELECT c.chateau FROM gtfs.chateaus c WHERE EXISTS (SELECT 1 FROM gtfs.routes r WHERE r.chateau = c.chateau);")

for chateau in $CHATEAUS; do
    # Trim whitespace
    chateau=$(echo "$chateau" | xargs)
    if [ -n "$chateau" ]; then
        echo "Running Gentian for $chateau"
        /catenary/output-binaries/gentian \
            --chateau "$chateau" \
            --osm-chunks /catenary/osm_chunks \
            --output /catenary/graph_output
    fi
done

echo "Test Complete!"
