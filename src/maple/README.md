# How to run

Example of running a task:
```bash
 DATABASE_URL=postgresql://postgres:PASSWORD@localhost/postgres THREADS_GTFS=2 GTFS_ZIP_TEMP=./temp-zip/ GTFS_UNCOMPRESSED_TEMP=./temp-uncompressed/ ./target/release/maple
 ```

 ### Env flags

ingest 1 feed
 `ONLY_FEED_ID = f~myfeedid`
 ingest multiple feeds
 `ONLY_FEED_IDS = f~myfeedid,f~myotherfeedid`

ingest all feeds
`FORCE_INGEST_ALL=true`

delete everything in feed before ingest
`DELETE_BEFORE_INGEST=true`