-- Read-only acceptance checks for one completed v2-only feed attempt.
-- Supply -v feed=... -v attempt=... to psql.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ;
CREATE TEMP TABLE maple_v2_check_scope ON COMMIT DROP AS
SELECT :'feed'::text AS onestop_feed_id, :'attempt'::text AS attempt_id;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM gtfs.itinerary_pattern_meta m
        JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
    ) THEN RAISE EXCEPTION 'No itinerary metadata for requested attempt'; END IF;

    IF EXISTS (
        SELECT 1 FROM gtfs.itinerary_pattern r
        JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
    ) THEN RAISE EXCEPTION 'V2-only attempt still has legacy timing rows'; END IF;

    IF EXISTS (
        SELECT 1 FROM gtfs.itinerary_pattern_meta m
        JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        LEFT JOIN gtfs.maple_v2_trip_major t USING (onestop_feed_id, attempt_id, itinerary_pattern_id)
        LEFT JOIN gtfs.maple_v2_structure s
          ON s.onestop_feed_id = t.onestop_feed_id AND s.attempt_id = t.attempt_id
         AND s.structure_id = t.structure_id
        WHERE t.itinerary_pattern_id IS NULL OR s.stop_count IS DISTINCT FROM m.row_count
    ) THEN RAISE EXCEPTION 'Missing v2 mapping or inconsistent metadata row count'; END IF;

    IF EXISTS (
        SELECT 1 FROM gtfs.itinerary_pattern_meta m
        JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        LEFT JOIN (
            SELECT r.onestop_feed_id, r.attempt_id, r.itinerary_pattern_id, count(*) AS n
            FROM gtfs.itinerary_pattern_trip_reader r
            JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
            GROUP BY r.onestop_feed_id, r.attempt_id, r.itinerary_pattern_id
        ) c USING (onestop_feed_id, attempt_id, itinerary_pattern_id)
        WHERE c.n IS DISTINCT FROM m.row_count::bigint
    ) THEN RAISE EXCEPTION 'Trip reader row count does not match metadata'; END IF;

    IF EXISTS (
        WITH trip AS (
            SELECT r.* FROM gtfs.itinerary_pattern_trip_reader r
            JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        ), dep AS (
            SELECT r.* FROM gtfs.itinerary_pattern_departures r
            JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        )
        (SELECT * FROM trip EXCEPT ALL SELECT * FROM dep)
        UNION ALL
        (SELECT * FROM dep EXCEPT ALL SELECT * FROM trip)
    ) THEN RAISE EXCEPTION 'Trip-major and stop-major rows differ'; END IF;

    IF EXISTS (
        WITH trip AS (
            SELECT r.onestop_feed_id, r.attempt_id, r.itinerary_pattern_id,
                   r.stop_sequence, r.stop_id, r.chateau, r.gtfs_stop_sequence
            FROM gtfs.itinerary_pattern_trip_reader r
            JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        ), structural AS (
            SELECT r.* FROM gtfs.itinerary_pattern_stop_reader r
            JOIN maple_v2_check_scope q USING (onestop_feed_id, attempt_id)
        )
        (SELECT * FROM trip EXCEPT ALL SELECT * FROM structural)
        UNION ALL
        (SELECT * FROM structural EXCEPT ALL SELECT * FROM trip)
    ) THEN RAISE EXCEPTION 'Structural reader differs from trip reader'; END IF;
END
$$;
ROLLBACK;
