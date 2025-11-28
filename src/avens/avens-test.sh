#!/bin/bash
set -e

# Command provided by user to reproduce crash
cargo run --release --bin avens -- --region north-america/us/california/socal --output-dir ./osm_chunks --temp-dir /tmp/osm_temp
