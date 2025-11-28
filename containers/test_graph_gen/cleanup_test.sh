#!/bin/bash
cd "$(dirname "$0")"

echo "Stopping and removing containers..."
docker compose down -v

echo "Cleaning up output directories..."
sudo rm -rf output
sudo rm -rf osm_chunks

echo "Cleanup complete."
