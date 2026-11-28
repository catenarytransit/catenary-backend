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

## Phase 3: retire legacy storage (final contract)

**Do not apply this migration until every deployed native reader and writer is
running phase 2 (PR #288) or newer.** Verify all instances, workers, scheduled
jobs and external SQL clients. Phase-1 binaries cannot use the retired tables.

1. Back up the database, ensure adequate disk/WAL headroom, pause static
   ingestion and cleanup, and backfill any remaining attempts using
   `scripts/maple_v2_backfill_unified.sql` before retirement.
2. Review the per-attempt parity checks. The contract migration itself checks
   every structure's contiguous stop indices, all readiness markers, both
   original tables against unified rows using NULL-safe exact comparisons, and
   the direct validated cascade FK. It locks writes during validation and uses
   RESTRICT drops; unexpected dependencies or failed checks abort the entire
   transaction. Schedule a low-traffic window for validation and DDL.
3. Explicitly authorize retirement on a populated database:

   ```sh
   PGOPTIONS="-c catenary.maple_v2_contract=on" diesel migration run
   ```

   Preserve any other required connection options. Empty databases (CI/fresh
   installations) do not require this opt-in. Never use it to bypass a failed
   parity check; fix the mismatch before retrying.

4. Run `tests/sql/maple_v2_unified_cutover.sql` on staging with an existing
   v2 structure, and `tests/sql/maple_v2_unified_parity.sql` and the existing
   production acceptance test for migrated v2-only attempts. Check fixed-time
   nearby, stop-board, single-trip and routing results. Resume ingestion and
   confirm that fresh attempts publish successfully and cleanup cascades.

The two stable views and publisher keep their exact names, column order/types
and signature. Existing phase-2 binaries therefore need no additional rebuild
solely for this migration. The two original stop tables, readiness table,
transitional FK, invalidation triggers and backfill function are removed.
The trip-major table and GTFS v1 fallback remain unchanged.

Rollback recreates both old tables and indexes from the current unified data,
including attempts imported after retirement, reinstates the transitional
FK, readiness state, triggers, backfill and dual-writing publisher, and restores
the transitional views. Pause writers, reserve space for the reconstructed
tables/indexes and WAL, and run the migration revert **before** deploying
phase-1 binaries. This is a full data reconstruction, not a fast rollback.
