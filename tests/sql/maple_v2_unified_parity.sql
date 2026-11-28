\set ON_ERROR_STOP on
SELECT to_regclass('gtfs.maple_v2_stop_unified_attempt') IS NOT NULL AS dual_storage \gset
\if :dual_storage
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ;
CREATE TEMP TABLE unified_scope ON COMMIT DROP AS
SELECT :'feed'::text AS onestop_feed_id, :'attempt'::text AS attempt_id;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM gtfs.maple_v2_stop_unified_attempt a
        JOIN unified_scope q USING (onestop_feed_id, attempt_id) WHERE a.ready
    ) THEN RAISE EXCEPTION 'Attempt is not ready'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM gtfs.maple_v2_structure s
        JOIN unified_scope q USING (onestop_feed_id, attempt_id)
    ) THEN RAISE EXCEPTION 'Attempt has no structures'; END IF;
    IF EXISTS (
        WITH old_rows AS (
            SELECT c.onestop_feed_id, c.attempt_id, c.structure_id, c.stop_index,
                   c.base, c.unit, c.bit_width, c.payload, c.exception_indices,
                   c.exception_arrivals, c.exception_departures, c.exception_interpolated,
                   c.min_departure_offset, c.max_departure_offset,
                   s.stop_id, s.gtfs_stop_sequence, s.metadata
            FROM gtfs.maple_v2_stop s
            JOIN gtfs.maple_v2_stop_major c USING
                (onestop_feed_id, attempt_id, structure_id, stop_index)
            JOIN unified_scope q USING (onestop_feed_id, attempt_id)
        ), new_rows AS (
            SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
                   u.base, u.unit, u.bit_width, u.payload, u.exception_indices,
                   u.exception_arrivals, u.exception_departures, u.exception_interpolated,
                   u.min_departure_offset, u.max_departure_offset,
                   u.stop_id, u.gtfs_stop_sequence, u.metadata
            FROM gtfs.maple_v2_stop_unified u
            JOIN unified_scope q USING (onestop_feed_id, attempt_id)
        )
        (SELECT * FROM old_rows EXCEPT ALL SELECT * FROM new_rows)
        UNION ALL
        (SELECT * FROM new_rows EXCEPT ALL SELECT * FROM old_rows)
    ) THEN RAISE EXCEPTION 'Unified stop rows differ'; END IF;
END
$$;
ROLLBACK;
\else
\echo 'Legacy storage retired; validating unified structure coverage instead.'
BEGIN ISOLATION LEVEL REPEATABLE READ;
CREATE TEMP TABLE unified_scope ON COMMIT DROP AS
SELECT :'feed'::text AS onestop_feed_id, :'attempt'::text AS attempt_id;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM gtfs.maple_v2_structure s
        JOIN unified_scope q USING (onestop_feed_id, attempt_id)
    ) THEN RAISE EXCEPTION 'Attempt has no structures'; END IF;
    IF EXISTS (
        SELECT 1 FROM gtfs.maple_v2_structure s
        JOIN unified_scope q USING (onestop_feed_id, attempt_id)
        LEFT JOIN (
            SELECT u.onestop_feed_id, u.attempt_id, u.structure_id,
                   count(*) AS n, min(stop_index) AS lo, max(stop_index) AS hi
            FROM gtfs.maple_v2_stop_unified u
            JOIN unified_scope q USING (onestop_feed_id, attempt_id)
            GROUP BY 1,2,3
        ) u USING (onestop_feed_id, attempt_id, structure_id)
        WHERE u.n IS DISTINCT FROM s.stop_count::bigint
           OR u.lo IS DISTINCT FROM 0 OR u.hi IS DISTINCT FROM s.stop_count - 1
    ) THEN RAISE EXCEPTION 'Unified structure coverage mismatch'; END IF;
END
$$;
ROLLBACK;
\endif
