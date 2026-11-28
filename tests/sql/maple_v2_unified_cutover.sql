\set ON_ERROR_STOP on
-- Run on an isolated/staging database after the cutover migration.
-- Synthetic fixtures use an existing v2 metadata row only as a template.
-- No fixture changes survive this transaction.
BEGIN;
CREATE TEMP TABLE cutover_source ON COMMIT DROP AS
SELECT m.* FROM gtfs.itinerary_pattern_meta m
JOIN gtfs.maple_v2_structure s
  ON s.onestop_feed_id=m.onestop_feed_id AND s.attempt_id=m.attempt_id
 AND s.structure_id=m.itinerary_pattern_id
LIMIT 1;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cutover_source) THEN
        RAISE EXCEPTION 'Test requires an existing v2 structure';
    END IF;
END
$$;

-- Avoid manufacturing unknown mandatory metadata fields: exercise the
-- actual publisher using a new, isolated structure ID scoped by a new attempt.
UPDATE cutover_source SET attempt_id = '__unified_cutover_test__';
INSERT INTO gtfs.itinerary_pattern_meta SELECT * FROM cutover_source;
INSERT INTO gtfs.maple_v2_structure
    (onestop_feed_id, attempt_id, structure_id, stop_count, timing_count)
SELECT onestop_feed_id, attempt_id, itinerary_pattern_id, 2, 1
FROM cutover_source;
INSERT INTO gtfs.maple_v2_trip_major
    (onestop_feed_id, attempt_id, structure_id, timing_index, itinerary_pattern_id,
     byte_width, payload, exception_indices, exception_arrivals, exception_departures,
     exception_interpolated)
SELECT onestop_feed_id, attempt_id, itinerary_pattern_id, 0, itinerary_pattern_id,
       2, decode('00003c00','hex'), ARRAY[1], ARRAY[NULL]::integer[],
       ARRAY[60], ARRAY[59]
FROM cutover_source;

CREATE TEMP TABLE maple_v2_unified_stage
    (LIKE gtfs.maple_v2_stop_unified INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
    ON COMMIT DROP;

INSERT INTO maple_v2_unified_stage
    (onestop_feed_id, attempt_id, structure_id, stop_index, base, unit, bit_width,
     payload, exception_indices, exception_arrivals, exception_departures,
     exception_interpolated, min_departure_offset, max_departure_offset,
     stop_id, gtfs_stop_sequence, metadata)
SELECT onestop_feed_id, attempt_id, itinerary_pattern_id, 1, 60, 1, 0,
       ''::bytea, ARRAY[0], ARRAY[NULL]::integer[], ARRAY[60], ARRAY[59],
       60, 60, '__cutover_stop__', 20,
       '{"pickup_type":0,"drop_off_type":0,"timepoint":null,"stop_headsign_idx":null}'::jsonb
FROM cutover_source
UNION ALL
SELECT onestop_feed_id, attempt_id, itinerary_pattern_id, 0, 0, 1, 0,
       ''::bytea, '{}'::integer[], '{}'::integer[], '{}'::integer[], '{}'::integer[],
       0, 0, '__cutover_stop__', 10,
       '{"pickup_type":0,"drop_off_type":0,"timepoint":true,"stop_headsign_idx":null}'::jsonb
FROM cutover_source;

SELECT gtfs.maple_v2_publish_unified(onestop_feed_id, attempt_id) FROM cutover_source;

DO $$
BEGIN
    IF (SELECT count(*) FROM gtfs.maple_v2_stop_columns_reader
        WHERE attempt_id='__unified_cutover_test__') <> 2
       OR (SELECT count(*) FROM gtfs.maple_v2_stop_metadata_reader
        WHERE attempt_id='__unified_cutover_test__') <> 2
       OR NOT EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_unified_attempt
        WHERE attempt_id='__unified_cutover_test__' AND ready)
    THEN RAISE EXCEPTION 'Published attempt missing or duplicated'; END IF;
    IF EXISTS (
        WITH a AS (SELECT * FROM gtfs.itinerary_pattern_trip_reader
                   WHERE attempt_id='__unified_cutover_test__'),
             b AS (SELECT * FROM gtfs.itinerary_pattern_departures
                   WHERE attempt_id='__unified_cutover_test__')
        (SELECT * FROM a EXCEPT ALL SELECT * FROM b)
        UNION ALL
        (SELECT * FROM b EXCEPT ALL SELECT * FROM a)
    ) THEN RAISE EXCEPTION 'Trip-major/stop-major parity failure'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM gtfs.itinerary_pattern_departures
        WHERE attempt_id='__unified_cutover_test__' AND stop_sequence=1
          AND arrival_time_since_start IS NULL AND departure_time_since_start=60
          AND interpolated_time_since_start=59
    ) THEN RAISE EXCEPTION 'Nullable timing exception lost'; END IF;
END
$$;

-- Simulate an old writer changing metadata: readiness must be invalidated
-- atomically, and both compatibility views must use the original rows.
UPDATE gtfs.maple_v2_stop
SET metadata=jsonb_set(metadata,'{pickup_type}','1'::jsonb)
WHERE attempt_id='__unified_cutover_test__' AND stop_index=0;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_unified_attempt
               WHERE attempt_id='__unified_cutover_test__' AND ready)
       OR NOT EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_columns_reader
               WHERE attempt_id='__unified_cutover_test__' AND stop_index=0
                 AND metadata->>'pickup_type'='1')
       OR NOT EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_metadata_reader
               WHERE attempt_id='__unified_cutover_test__' AND stop_index=0
                 AND metadata->>'pickup_type'='1')
    THEN RAISE EXCEPTION 'Legacy fallback failure'; END IF;
END
$$;

SELECT gtfs.maple_v2_backfill_unified(onestop_feed_id, attempt_id) FROM cutover_source;
DELETE FROM gtfs.maple_v2_structure WHERE attempt_id='__unified_cutover_test__';
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_unified WHERE attempt_id='__unified_cutover_test__')
       OR EXISTS (SELECT 1 FROM gtfs.maple_v2_stop WHERE attempt_id='__unified_cutover_test__')
       OR EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_major WHERE attempt_id='__unified_cutover_test__')
    THEN RAISE EXCEPTION 'Structure cleanup did not cascade'; END IF;
END
$$;
ROLLBACK;
