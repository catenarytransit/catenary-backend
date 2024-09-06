# How to run

Example of running a task:
```bash
 DATABASE_URL=postgresql://postgres:PASSWORD@localhost/postgres THREADS_GTFS=2 GTFS_ZIP_TEMP=./temp-zip/ GTFS_UNCOMPRESSED_TEMP=./temp-uncompressed/ ./target/release/maple
 ```