# Maple Syrup v2 unified stop rollout (expand phase)

The new table copies the original stop-major payload and stop metadata without changing encoding, keys, or public APIs. Old stop tables and all existing compatibility views remain authoritative during this phase. The new reader opts in per feed attempt only when the ready marker exists. All other attempts use the original two-table query.

## Deployment

1. Check available disk, including WAL retention and sort space. Reserve several GB for the current v2 population. Run `diesel migration run` using the normal production database configuration **before** deploying new binaries. Schedule DDL for low traffic; trigger creation needs short locks on old tables.
2. Deploy the new Maple (and any importer using the common v2 persist code). It automatically creates a sorted unified copy at the end of each successful v2 ingest in the same transaction. Older writers are compatible: their inserts/updates invalidate readiness, causing reader fallback until backfilled. Cleanup cascades through the existing stop-major FK. Do not manually modify the unified table. The additional copy increases import time and WAL temporarily.
3. Backfill National Rail first, from repo root:
   ```sh
   psql "$DATABASE_URL" -v feed=f-networkrail -f scripts/maple_v2_backfill_unified.sql
   ```
   Each attempt commits independently; retry after any lock/cleanup conflict. For initial production backfill, avoid concurrent ingestion/cleanup of the same feed if possible. The script is resumable and skips ready attempts.
4. List attempts using the backfill output, then verify each migrated attempt:
   ```sh
   psql "$DATABASE_URL" -v feed=f-networkrail -v attempt=ACTUAL_ATTEMPT_ID -f tests/sql/maple_v2_unified_parity.sql
   ```
   Also run the existing maple_v2_production.sql for v2-only attempts. Compare old/new API responses at fixed query timestamps, including repeated stops, overnight departures and realtime. Compare both warm and cold query timings and heap footprints.
5. Set `MAPLE_V2_USE_UNIFIED_STOPS=true` in Birch/Ramonda and other native-reader services, restart them in the normal rolling deployment. Default is off. Only ready attempts switch; old readers and compatibility views continue using old tables.
6. Backfill the remaining feeds by running the same script without `-v feed=...`, then verify them. Monitor database size, WAL, ingestion time and API debug timings.

## Rollback

Unset the environment flag and restart native-reader services. The old layout has continued to receive all writes. For a schema rollback, first deploy old binaries for all writers and readers, then revert the migration. Do not drop old tables or change compatibility views in this phase.

## Contract phase (separate PR)

After all readers and writers are upgraded and parity is proven, update Gentian/structural/trip compatibility views and direct trip-major metadata reads, update Diesel schema/models and ingestion, replace the unified table's transitional FK with a structure FK, and only then retire the old tables. Until that phase, storage is intentionally duplicated. Do not delete original v2 rows manually to save space.

## Phase 2: stable reader/writer cutover

Apply the cutover migration before deploying binaries. It adds the direct
structure FK, stable metadata/column views, updates the four public
compatibility views in place, and adds the transactional publication function.
It does **not** drop either original table or the transitional FK.

Build and deploy every native reader/writer:

```sh
cargo build --release --bin maple --bin birch --bin ramonda --bin spruce --bin aspen --bin alpenrose
```

Restart all deployed instances, including workers and scheduled jobs. The
new native reader always uses the stable views; the phase-1 environment flag
is no longer needed. Unready attempts still fall back automatically. Maple
stages unified rows in a transaction-local temp table; the database publisher
mirrors the old layout and inserts the unified rows in global stop-ID order.
Old binaries continue to work during this rolling deployment.

On staging, run `tests/sql/maple_v2_unified_cutover.sql` with an existing
v2 structure, the existing parity tests, and `cargo test --lib maple_v2_reader::tests`.
Compare fixed-time nearby, single-trip and stop-board responses, and check
EXPLAIN plans for the stable stop-column view. In particular, verify that
the fallback branch does not fetch old payloads for ready attempts; planner
behavior can differ from the phase-1 explicit branch query.

Before the separate contract migration, stop legacy writers, ensure all
instances have been restarted on this revision, backfill remaining attempts,
verify exact row parity for each attempt, and verify that there are no external
SQL consumers referencing old tables. Do not manually drop either table:
the stable views and publisher intentionally still reference them. The
contract migration must replace those definitions and remove the transitional
FK before dropping the originals. Rollback of this phase requires deploying
phase-1 binaries before reverting the migration.
