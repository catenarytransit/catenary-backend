# How to run

## Configuration

look to `example.catenaryconfig.toml`. Rename it to `catenaryconfig.toml`

To run maple to insert items into Postgres and Elasticsearch, configure the URLs.

If you want maple to only use certain feed ids on insertion, you can write them in such a format like:

```toml
only_feed_ids = ["f-gtfs~de", "f-vvs~de"]
```

You can also configure the discord_log webhook url to ping your discord channel whenever the task starts or finishes.

Girolle is an online automated github action microservice that pre-downloads and calculates hashes for known GTFS files to prevent your system from downloading them. This should be ignored during development. `use_girolle = false` is set to false by default.

To skip elastic search insertion, set `no_elastic = true`

 ### Env flags override

Example of running a task without using the env file:
```bash
 DATABASE_URL=postgresql://postgres:PASSWORD@localhost/postgres THREADS_GTFS=2 GTFS_ZIP_TEMP=./temp-zip/ GTFS_UNCOMPRESSED_TEMP=./temp-uncompressed/ ./target/release/maple
 ```

ingest 1 feed
 `ONLY_FEED_ID = f~myfeedid`
 ingest multiple feeds
 `ONLY_FEED_IDS = f~myfeedid,f~myotherfeedid`

ingest all feeds
`FORCE_INGEST_ALL=true`

delete everything in feed before ingest
`DELETE_BEFORE_INGEST=true`