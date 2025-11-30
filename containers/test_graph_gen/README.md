# Graph Generation Test Container Walkthrough

This document explains how to use the Docker container to test the graph generation pipeline (`maple`, `avens`, `gentian`).

## Prerequisites

- Docker and Docker Compose installed.

## Setup

1.  Navigate to the test container directory:
    ```bash
    cd containers/test_graph_gen
    ```

## Running the Test

1.  Build and run the container:
    ```bash
    docker compose up --build
    ```

    This will:
    -   Start a PostgreSQL instance.
    -   Build the `maple`, `avens`, and `gentian` binaries (and `pfaedle`).
    -   Wait for Postgres to be ready.
    -   Import the specified Southern California feeds using `maple`.
    -   Generate OSM chunks for California using `avens`.
    -   Generate the graph using `gentian`.

2.  Monitor the logs. The process will exit when complete.

## Output

-   **Graph Output**: Generated graph files (`.pbf`, `.ch`) will be in `containers/test_graph_gen/output`.
-   **OSM Chunks**: Generated OSM chunks will be in `containers/test_graph_gen/osm_chunks`.

## rerun only gentian

```bash
docker compose -f containers/test_graph_gen/docker-compose.yml run --rm test_runner /catenary/containers/test_graph_gen/run_gentian_only.sh
```


## Cleanup

To stop the containers and remove the postgres volume and output files:

```bash
./cleanup_test.sh
```



---------------

## Benchmark instructions

1. Ensure the `edelweiss` service is running.
   ```bash
   docker compose -f containers/edelweiss/docker-compose.yml up -d
   ```

2. Run the benchmark script inside the `test_graph_gen` container (which is defined as `test_runner` in its compose file).
   Note: We need to ensure `test_runner` can talk to `edelweiss`. They should be on the same network.
   
   If they are not in the same compose file, you might need to use `--network` or combine them.
   
   Assuming `edelweiss` is running on host port 9090, and we run this locally:
   
   ```bash
   # Make script executable
   chmod +x containers/test_graph_gen/run_queries.sh
   
   # Run locally (if cargo is installed)
   ./containers/test_graph_gen/run_queries.sh
   ```
   
   OR, if running inside the `test_graph_gen` container:
   
   ```bash
   # You might need to adjust the docker-compose to include edelweiss or link networks.
   # For now, let's assume we run it via the existing test_runner service.
   docker compose -f containers/test_graph_gen/docker-compose.yml run --rm test_runner bash /catenary/containers/test_graph_gen/run_queries.sh
   ```

   **Note**: The script assumes `edelweiss:9090` is reachable. If running locally against localhost, change `SERVER` in `run_queries.sh` to `127.0.0.1:9090`.