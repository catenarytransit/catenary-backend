# Maple standalone cleanup

```sh
cargo run --release --bin maple -- cleanup --dry-run
cargo run --release --bin maple -- cleanup
cargo run --release --bin maple -- cleanup --feed-id 'f-bus~dft~gov~uk~england'
```

The command only needs the normal PostgreSQL configuration (DATABASE_URL or the
configured database URL). It exits before normal download, country-index,
Elasticsearch and temporary-file initialization. It never reimports a feed.
Run it from a separate process or a periodic service/cron job.

Cleanup selects feeds with multiple successful production attempts and uses
Maple's shared five-day historical retention policy and single-ingest exceptions.
It keeps the latest successful attempt. Failed/in-progress attempts are not
candidates. A feed with pending production assignment, an in-progress marker,
no currently applicable spatially active replacement, or a still-spatially-active
obsolete attempt is skipped rather than removing visible service. This command
does not activate future schedules or reconcile spatial flags.

Both ingestion and cleanup must use this version of Maple: they share a
per-feed PostgreSQL session advisory lock. Cleanup skips busy feeds and proceeds
with others; an ingest waits only for a cleanup already holding its feed lock.
Each ingest task uses one additional unpooled database connection for the lock,
without opening a long-running database transaction. Use a direct PostgreSQL or
session-mode connection, not a transaction-pooling proxy. Older binaries and
manual SQL do not participate in this protocol.

Cleanup deletes one exact attempt at a time in a transaction, retains its
ingestion history with production=false and deleted=true, and logs each
retirement. It does not use the broad delete-stale-attempts command. A failed
attempt deletion rolls back, other feeds are processed, and the command exits
nonzero if any feed failed. The operation is PostgreSQL-only, matching the
normal successful-attempt cleanup scope; it does not clear downloads or search
indexes. Deletions generate WAL and dead tuples; check disk headroom before
running on very large feeds. Ordinary vacuum can reclaim the space for reuse.

Tests:
```sh
cargo test --bin maple standalone_cleanup
cargo test --bin maple locks_are_session_scoped_and_independent -- --ignored
```
The second test requires a PostgreSQL connection.
