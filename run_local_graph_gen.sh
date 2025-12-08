#!/bin/bash
set -e

# Configuration
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_DB="catenary"
# Assuming password is 'catenary' based on docker-compose, but locally it might be different or .pgpass used.
# We'll set it for now, user can override.
export PGPASSWORD="${PGPASSWORD:-postgres}"

WORK_DIR="$(pwd)"
BIN_DIR="$WORK_DIR/bin"
OUTPUT_DIR="$WORK_DIR/output"
OSM_CHUNKS_DIR="$WORK_DIR/osm_chunks"
TEMP_DIR="$WORK_DIR/temp"
PFAEDLE_BIN="$BIN_DIR/pfaedle"

# Ensure directories exist
mkdir -p "$BIN_DIR"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OSM_CHUNKS_DIR"
mkdir -p "$TEMP_DIR"

# Add local bin to PATH
export PATH="$BIN_DIR:$PATH"

# Argument parsing
# Argument parsing
GENTIAN_ONLY=false
SKIP_EXTRACT=false

for arg in "$@"; do
    case $arg in
        --gentian-only)
            GENTIAN_ONLY=true
            shift
            ;;
        --skip-extract)
            SKIP_EXTRACT=true
            shift
            ;;
        *)
            # unknown option
            ;;
    esac
done

echo "=== Checking Dependencies ==="

check_cmd() {
    if ! command -v "$1" &> /dev/null; then
        echo "Error: $1 is not installed or not in PATH."
        exit 1
    fi
}

check_cmd cargo
check_cmd diesel
check_cmd psql
check_cmd cmake
check_cmd git

# Check/Build pfaedle
if ! command -v pfaedle &> /dev/null; then
    if [ -f "$PFAEDLE_BIN" ]; then
        echo "Found pfaedle at $PFAEDLE_BIN"
    else
        echo "pfaedle not found. Building locally..."
        if [ -d "$TEMP_DIR/pfaedle" ]; then
            rm -rf "$TEMP_DIR/pfaedle"
        fi
        
        git clone https://github.com/ad-freiburg/pfaedle.git "$TEMP_DIR/pfaedle"
        
        pushd "$TEMP_DIR/pfaedle"
        mkdir -p build
        cd build
        cmake .. -DCMAKE_INSTALL_PREFIX="$WORK_DIR" 
        make -j$(nproc)
        # We copy manually to avoid installing to system directories if make install tries that
        cp pfaedle "$PFAEDLE_BIN"
        popd
        
        echo "pfaedle built and installed to $PFAEDLE_BIN"
    fi
else
    echo "Found system pfaedle."
fi

# Check/Clone transitland-atlas
if [ ! -d "$TEMP_DIR/transitland-atlas" ]; then
    echo "Cloning transitland-atlas..."
    git clone https://github.com/catenaryTransit/transitland-atlas.git "$TEMP_DIR/transitland-atlas"
else
    echo "Updating transitland-atlas..."
    pushd "$TEMP_DIR/transitland-atlas"
    git pull
    popd
fi

echo "=== Building Catenary Binaries ==="
cargo build --release --bin maple --bin avens --bin gentian

MAPLE_BIN="./target/release/maple"
AVENS_BIN="./target/release/avens"
GENTIAN_BIN="./target/release/gentian"

echo "=== Waiting for Postgres ==="
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  echo "Waiting for postgres at $POSTGRES_HOST:$POSTGRES_PORT..."
  sleep 2
done
echo "Postgres is ready."

echo "=== Running Migrations ==="
diesel migration run --database-url "postgres://$POSTGRES_USER:$PGPASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"

export DATABASE_URL="postgres://$POSTGRES_USER:$PGPASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"

if [ "$GENTIAN_ONLY" = false ]; then
    echo "=== Running Maple (GTFS Ingest) ==="
    # Setup temp dirs for maple
    GTFS_ZIP_TEMP="$TEMP_DIR/gtfs_zip"
    GTFS_UNCOMPRESSED_TEMP="$TEMP_DIR/gtfs_uncompressed"
    mkdir -p "$GTFS_ZIP_TEMP"
    mkdir -p "$GTFS_UNCOMPRESSED_TEMP"

    export GTFS_ZIP_TEMP
    export GTFS_UNCOMPRESSED_TEMP
    export DELETE_BEFORE_INGEST=true
    export FORCE_INGEST_ALL=true

    # Feeds list from run_test.sh
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
    "f-9q5-metro~losangeles~rail"
    "f-9q5-ladot"
    "f-9q5c-bigbluebus"
    "f-9q5c-culvercitybus"
    "f-9q5b4-pvpta~ca~us"
    "f-9muq-lagunabeach~ca~us"
    "f-9q4g-santabarbaramtd"
    "f-9q9-scmtdcom"
    "f-sf~bay~area~rg"
    "f-9qce-sacramentoregionaltransit"
    "f-9q9-modesto~ca~us"
    "f-9qd-mercedthebus~ca~us"
    "f-9q54-goldcoasttransit"
    "f-9qdc-gis4ufresnogov"
    "f-montebello~bus"
    "f-redondo~beach~cities~transit"
    "f-9q7-getbus"
    "f-9q7-kerncounty~ca~us"
    )

    # Join feeds into comma-separated string
    printf -v ONLY_FEED_IDS "%s," "${FEEDS[@]}"
    export ONLY_FEED_IDS="${ONLY_FEED_IDS%,}"
    echo "ONLY_FEED_IDS: $ONLY_FEED_IDS"

    "$MAPLE_BIN" \
        --transitland "$TEMP_DIR/transitland-atlas" \
        --no-elastic

    echo "=== Running Avens (OSM Preprocessing) ==="
    OSM_TEMP="$TEMP_DIR/osm_temp"
    mkdir -p "$OSM_TEMP"

    "$AVENS_BIN" \
     --region north-america/us/california \
        --output-dir "$OSM_CHUNKS_DIR" \
        --temp-dir "$OSM_TEMP"
else
    echo "Skipping Maple and Avens (Gentian Only mode)"
fi

echo "=== Running Gentian (Graph Generation) ==="
# Fetch Chateaus

CHATEAUS=$(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT c.chateau FROM gtfs.chateaus c WHERE EXISTS (SELECT 1 FROM gtfs.routes r WHERE r.chateau = c.chateau);")

# Convert newlines to commas
CHATEAU_LIST=$(echo "$CHATEAUS" | tr '\n' ',' | sed 's/,$//' | sed 's/ //g')

if [ -n "$CHATEAU_LIST" ]; then
    if [ "$SKIP_EXTRACT" = false ]; then
       echo "Running Gentian Extract for: $CHATEAU_LIST"
       "$GENTIAN_BIN" \
            extract \
            --output "$OUTPUT_DIR" \
           --osm-chunks "$OSM_CHUNKS_DIR" \
            --chateau "$CHATEAU_LIST"
    else
        echo "Skipping Gentian Extract (--skip-extract)"
    fi

    echo "Running Gentian Cluster"
    "$GENTIAN_BIN" \
        cluster \
        --output "$OUTPUT_DIR"

    echo "Running Gentian Update GTFS"
    IFS=',' read -ra CHATEAU_ARR <<< "$CHATEAU_LIST"
    for chateau in "${CHATEAU_ARR[@]}"; do
        echo "Updating GTFS for chateau: $chateau"
        "$GENTIAN_BIN" \
            update-gtfs \
            --chateau "$chateau" \
            --output "$OUTPUT_DIR"
    done

    echo "Running Gentian Rebuild Patterns"
    "$GENTIAN_BIN" \
        rebuild-patterns \
        --output "$OUTPUT_DIR" 
else
    echo "No chateaus found to process."
fi

echo "=== Local Graph Generation Complete! ==="
