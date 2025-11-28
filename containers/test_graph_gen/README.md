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

## Cleanup

To stop the containers and remove the postgres volume and output files:

```bash
./cleanup_test.sh
```
