\set ON_ERROR_STOP on
SELECT to_regclass('gtfs.maple_v2_stop_unified_attempt') IS NOT NULL AS dual_storage \gset
\if :dual_storage
\set ON_ERROR_STOP on
\timing on
-- Run with psql -v feed=f-networkrail -f scripts/maple_v2_backfill_unified.sql
-- Each attempt commits separately; rerunning skips ready attempts.
\if :{?feed}
\else
\set feed ''
\endif
SET jit = off;
SET work_mem = '64MB';
SET statement_timeout = '0';
SET lock_timeout = '10s';
SELECT format('SELECT %L AS feed, %L AS attempt, gtfs.maple_v2_backfill_unified(%L, %L) AS copied;',
              s.onestop_feed_id, s.attempt_id, s.onestop_feed_id, s.attempt_id)
FROM (SELECT DISTINCT onestop_feed_id, attempt_id FROM gtfs.maple_v2_structure) s
LEFT JOIN gtfs.maple_v2_stop_unified_attempt a USING (onestop_feed_id, attempt_id)
WHERE NOT COALESCE(a.ready, false)
  AND (:'feed' = '' OR s.onestop_feed_id = :'feed')
ORDER BY s.onestop_feed_id, s.attempt_id
\gexec
ANALYZE gtfs.maple_v2_stop_unified;
ANALYZE gtfs.maple_v2_stop_unified_attempt;
SELECT s.onestop_feed_id, s.attempt_id, COALESCE(a.ready, false) AS ready,
       a.migrated_at, s.expected
FROM (SELECT onestop_feed_id, attempt_id, sum(stop_count) AS expected
      FROM gtfs.maple_v2_structure GROUP BY 1,2) s
LEFT JOIN gtfs.maple_v2_stop_unified_attempt a USING (onestop_feed_id, attempt_id)
WHERE :'feed' = '' OR s.onestop_feed_id = :'feed'
ORDER BY 1,2;
\else
\echo 'Legacy tables retired; no backfill is required.'
\endif
