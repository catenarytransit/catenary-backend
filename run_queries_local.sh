#!/bin/bash
set -e

# Ensure output directory exists
mkdir -p ./graph_output/benchmark_results

# Define queries: Name, StartLat, StartLon, EndLat, EndLon
QUERIES=(
    "UCI_to_UCLA 33.6437421 -117.8444668 34.0741683 -118.4437634"
   "UCLA_to_UCI 34.0741683 -118.4437634 33.6437421 -117.8444668"
  "USC_Roski_to_Ruts 34.0191038 -118.2879474 34.0729518 -118.3093591"
   "Ruts_to_USC_Roski 34.0729518 -118.3093591 34.0191038 -118.2879474"
   "USC_To_UnionStation 34.0191038 -118.2879474 34.055916 -118.234248"
  "UCSanDiego_to_UCLA 32.877894 -117.222512 34.069453 -118.4448664"
  "USC_to_UCLA 34.0191038 -118.2879474 34.069453 -118.4448664"
  "LittleTokyo_to_7th 34.04922588787933 -118.23918201268017 34.04865129201799 -118.2586985274647"
)

# Server address
# Default to 127.0.0.1:9090 if ERVER env var is not set
SERVER="${SERVER:-127.0.0.1:9090}"

# Build the client first (if not already built in the container)
# Assuming the container has cargo installed
cargo build --release --bin test_edelweiss_client

CLIENT_BIN="./target/release/test_edelweiss_client"

# Calculate next noon
current_ts=$(date +%s)
today_noon_ts=$(date -d "12:00" +%s)

if [ "$current_ts" -lt "$today_noon_ts" ]; then
    TARGET_TIME=$today_noon_ts
else
    TARGET_TIME=$(date -d "tomorrow 12:00" +%s)
fi
echo "Using target time: $(date -d @$TARGET_TIME)"

for query in "${QUERIES[@]}"; do
    read -r NAME START_LAT START_LON END_LAT END_LON <<< "$query"
    echo "Running query: $NAME"
    
    # Text output
    $CLIENT_BIN \
        --server "$SERVER" \
        --time "$TARGET_TIME" \
        --start-lat="$START_LAT" --start-lon="$START_LON" \
        --end-lat="$END_LAT" --end-lon="$END_LON" \
        > "./graph_output/benchmark_results/${NAME}.txt"
        
    # GeoJSON output
    $CLIENT_BIN \
        --server "$SERVER" \
        --time "$TARGET_TIME" \
        --start-lat="$START_LAT" --start-lon="$START_LON" \
        --end-lat="$END_LAT" --end-lon="$END_LON" \
        --geojson \
        > "./graph_output/benchmark_results/${NAME}.geojson"
done

echo "Benchmark complete. Results in ./graph_output/benchmark_results/"
