-- Reader/writer cutover. Keep rollback tables and phase-1 binaries working.
-- The final contract migration will replace these stable read/write interfaces
-- with unified-only definitions; no application SQL needs to change again.
SET LOCAL lock_timeout = '10s';

ALTER TABLE gtfs.maple_v2_stop_unified
    ADD CONSTRAINT maple_v2_stop_unified_structure_fk
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id)
    REFERENCES gtfs.maple_v2_structure ON DELETE CASCADE NOT VALID;
ALTER TABLE gtfs.maple_v2_stop_unified
    VALIDATE CONSTRAINT maple_v2_stop_unified_structure_fk;

-- An INSERT must also advance an already-false marker so a concurrent
-- repeatable-read rebuild cannot publish stale readiness.
CREATE OR REPLACE FUNCTION gtfs.maple_v2_unified_invalidate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id)
        SELECT onestop_feed_id, attempt_id FROM (
            SELECT onestop_feed_id, attempt_id FROM new_rows
            UNION
            SELECT onestop_feed_id, attempt_id FROM old_rows
        ) keys ORDER BY onestop_feed_id, attempt_id
        ON CONFLICT (onestop_feed_id, attempt_id) DO UPDATE
        SET ready = false, migrated_at = NULL;
    ELSIF TG_OP = 'DELETE' THEN
        -- Cascades remove unified rows, but a partial source deletion also
        -- invalidates completeness. Status rows are intentionally independent.
        UPDATE gtfs.maple_v2_stop_unified_attempt a
        SET ready = false, migrated_at = NULL
        FROM (SELECT DISTINCT onestop_feed_id, attempt_id FROM old_rows) k
        WHERE a.onestop_feed_id = k.onestop_feed_id AND a.attempt_id = k.attempt_id;
    ELSE
        INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id)
        SELECT DISTINCT onestop_feed_id, attempt_id FROM new_rows
        ORDER BY onestop_feed_id, attempt_id
        ON CONFLICT (onestop_feed_id, attempt_id) DO UPDATE
        SET ready = false, migrated_at = NULL;
    END IF;
    RETURN NULL;
END
$$;


-- All v2 read paths select one representation per complete feed attempt.
-- Missing or invalidated readiness keeps older imports usable. Preserve
-- metadata-only reads independently of timing payloads for trip consumers.
CREATE VIEW gtfs.maple_v2_stop_metadata_reader AS
SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
       u.stop_id, u.gtfs_stop_sequence, u.metadata
FROM gtfs.maple_v2_stop_unified u
JOIN gtfs.maple_v2_stop_unified_attempt a USING (onestop_feed_id, attempt_id)
WHERE a.ready
UNION ALL
SELECT s.onestop_feed_id, s.attempt_id, s.structure_id, s.stop_index,
       s.stop_id, s.gtfs_stop_sequence, s.metadata
FROM gtfs.maple_v2_stop s
WHERE NOT EXISTS (
    SELECT 1 FROM gtfs.maple_v2_stop_unified_attempt a
    WHERE a.onestop_feed_id = s.onestop_feed_id AND a.attempt_id = s.attempt_id AND a.ready
);

CREATE VIEW gtfs.maple_v2_stop_columns_reader AS
SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
       u.base, u.unit, u.bit_width, u.payload, u.exception_indices,
       u.exception_arrivals, u.exception_departures, u.exception_interpolated,
       u.min_departure_offset, u.max_departure_offset,
       u.stop_id, u.gtfs_stop_sequence, u.metadata
FROM gtfs.maple_v2_stop_unified u
JOIN gtfs.maple_v2_stop_unified_attempt a USING (onestop_feed_id, attempt_id)
WHERE a.ready
UNION ALL
SELECT c.onestop_feed_id, c.attempt_id, c.structure_id, c.stop_index,
       c.base, c.unit, c.bit_width, c.payload, c.exception_indices,
       c.exception_arrivals, c.exception_departures, c.exception_interpolated,
       c.min_departure_offset, c.max_departure_offset,
       s.stop_id, s.gtfs_stop_sequence, s.metadata
FROM gtfs.maple_v2_stop s
JOIN gtfs.maple_v2_stop_major c USING (onestop_feed_id, attempt_id, structure_id, stop_index)
WHERE NOT EXISTS (
    SELECT 1 FROM gtfs.maple_v2_stop_unified_attempt a
    WHERE a.onestop_feed_id = s.onestop_feed_id AND a.attempt_id = s.attempt_id AND a.ready
);

CREATE OR REPLACE VIEW gtfs.itinerary_pattern_gentian AS
SELECT t.onestop_feed_id, t.attempt_id, t.itinerary_pattern_id,
       s.stop_index AS stop_sequence, s.stop_id,
       CASE WHEN e.pos IS NULL THEN gtfs.maple_v2_primary(t.payload,t.byte_width,s.stop_index)
            ELSE t.exception_arrivals[e.pos] END AS arrival_time_since_start,
       CASE WHEN e.pos IS NULL THEN gtfs.maple_v2_primary(t.payload,t.byte_width,s.stop_index)
            ELSE t.exception_departures[e.pos] END AS departure_time_since_start,
       CASE WHEN e.pos IS NULL THEN NULL::integer
            ELSE t.exception_interpolated[e.pos] END AS interpolated_time_since_start,
       s.gtfs_stop_sequence
FROM gtfs.maple_v2_trip_major t
JOIN gtfs.maple_v2_stop_metadata_reader s USING (onestop_feed_id, attempt_id, structure_id)
CROSS JOIN LATERAL (SELECT array_position(t.exception_indices,s.stop_index) AS pos) e
UNION ALL
SELECT r.onestop_feed_id, r.attempt_id, r.itinerary_pattern_id,
       r.stop_sequence, r.stop_id, r.arrival_time_since_start,
       r.departure_time_since_start, r.interpolated_time_since_start,
       r.gtfs_stop_sequence::bigint
FROM gtfs.itinerary_pattern r
WHERE NOT EXISTS (
    SELECT 1 FROM gtfs.maple_v2_trip_major t
    WHERE t.onestop_feed_id=r.onestop_feed_id AND t.attempt_id=r.attempt_id
      AND t.itinerary_pattern_id=r.itinerary_pattern_id
);

CREATE OR REPLACE VIEW gtfs.itinerary_pattern_departures AS
SELECT t.onestop_feed_id, t.attempt_id, t.itinerary_pattern_id,
       c.stop_index AS stop_sequence,
       CASE WHEN e.pos IS NULL THEN p.value ELSE c.exception_arrivals[e.pos] END AS arrival_time_since_start,
       CASE WHEN e.pos IS NULL THEN p.value ELSE c.exception_departures[e.pos] END AS departure_time_since_start,
       CASE WHEN e.pos IS NULL THEN NULL::integer ELSE c.exception_interpolated[e.pos] END AS interpolated_time_since_start,
       c.stop_id, m.chateau, c.gtfs_stop_sequence::oid AS gtfs_stop_sequence,
       (c.metadata->>'timepoint')::boolean AS timepoint,
       (c.metadata->>'stop_headsign_idx')::smallint AS stop_headsign_idx,
       (c.metadata->>'pickup_type')::smallint AS pickup_type,
       (c.metadata->>'drop_off_type')::smallint AS drop_off_type
FROM gtfs.maple_v2_stop_columns_reader c
JOIN gtfs.maple_v2_trip_major t USING (onestop_feed_id, attempt_id, structure_id)
JOIN gtfs.itinerary_pattern_meta m USING (onestop_feed_id, attempt_id, itinerary_pattern_id)
CROSS JOIN LATERAL (SELECT array_position(c.exception_indices,t.timing_index) AS pos) e
CROSS JOIN LATERAL (SELECT gtfs.maple_v2_column_primary(c.payload,c.base,c.unit,c.bit_width,t.timing_index) AS value) p
UNION ALL
SELECT r.onestop_feed_id,r.attempt_id,r.itinerary_pattern_id,r.stop_sequence,
       r.arrival_time_since_start,r.departure_time_since_start,r.interpolated_time_since_start,
       r.stop_id,r.chateau,r.gtfs_stop_sequence,r.timepoint,r.stop_headsign_idx,r.pickup_type,r.drop_off_type
FROM gtfs.itinerary_pattern r
WHERE NOT EXISTS (
    SELECT 1 FROM gtfs.maple_v2_trip_major t
    WHERE t.onestop_feed_id=r.onestop_feed_id AND t.attempt_id=r.attempt_id
      AND t.itinerary_pattern_id=r.itinerary_pattern_id
);

CREATE OR REPLACE VIEW gtfs.itinerary_pattern_trip_reader AS
SELECT g.onestop_feed_id,g.attempt_id,g.itinerary_pattern_id,g.stop_sequence,
       g.arrival_time_since_start,g.departure_time_since_start,g.interpolated_time_since_start,
       g.stop_id,m.chateau,g.gtfs_stop_sequence::oid AS gtfs_stop_sequence,
       CASE WHEN t.itinerary_pattern_id IS NULL THEN r.timepoint ELSE (s.metadata->>'timepoint')::boolean END AS timepoint,
       CASE WHEN t.itinerary_pattern_id IS NULL THEN r.stop_headsign_idx ELSE (s.metadata->>'stop_headsign_idx')::smallint END AS stop_headsign_idx,
       CASE WHEN t.itinerary_pattern_id IS NULL THEN r.pickup_type ELSE (s.metadata->>'pickup_type')::smallint END AS pickup_type,
       CASE WHEN t.itinerary_pattern_id IS NULL THEN r.drop_off_type ELSE (s.metadata->>'drop_off_type')::smallint END AS drop_off_type
FROM gtfs.itinerary_pattern_gentian g
JOIN gtfs.itinerary_pattern_meta m USING (onestop_feed_id,attempt_id,itinerary_pattern_id)
LEFT JOIN gtfs.maple_v2_trip_major t USING (onestop_feed_id,attempt_id,itinerary_pattern_id)
LEFT JOIN gtfs.maple_v2_stop_metadata_reader s ON s.onestop_feed_id=t.onestop_feed_id
 AND s.attempt_id=t.attempt_id AND s.structure_id=t.structure_id AND s.stop_index=g.stop_sequence
LEFT JOIN gtfs.itinerary_pattern r ON t.itinerary_pattern_id IS NULL
 AND r.onestop_feed_id=g.onestop_feed_id AND r.attempt_id=g.attempt_id
 AND r.itinerary_pattern_id=g.itinerary_pattern_id AND r.stop_sequence=g.stop_sequence;

CREATE OR REPLACE VIEW gtfs.itinerary_pattern_stop_reader AS
SELECT m.onestop_feed_id, m.attempt_id, m.itinerary_pattern_id,
       s.stop_index AS stop_sequence, s.stop_id, m.chateau,
       s.gtfs_stop_sequence::oid AS gtfs_stop_sequence
FROM gtfs.itinerary_pattern_meta m
JOIN gtfs.maple_v2_trip_major t USING (onestop_feed_id, attempt_id, itinerary_pattern_id)
JOIN gtfs.maple_v2_stop_metadata_reader s USING (onestop_feed_id, attempt_id, structure_id)
UNION ALL
SELECT r.onestop_feed_id, r.attempt_id, r.itinerary_pattern_id,
       r.stop_sequence, r.stop_id, r.chateau, r.gtfs_stop_sequence
FROM gtfs.itinerary_pattern r
WHERE NOT EXISTS (
    SELECT 1 FROM gtfs.maple_v2_trip_major t
    WHERE t.onestop_feed_id = r.onestop_feed_id
      AND t.attempt_id = r.attempt_id
      AND t.itinerary_pattern_id = r.itinerary_pattern_id
);

-- Maple writes a single unified row stream to a transaction-local staging
-- table. This publication function temporarily mirrors the two old tables
-- before writing unified rows in stop-ID order. The contract migration will
-- remove the mirror operations from the function.
CREATE FUNCTION gtfs.maple_v2_publish_unified(p_feed text, p_attempt text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    expected bigint;
    staged bigint;
    copied bigint;
BEGIN
    INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id)
    VALUES (p_feed, p_attempt)
    ON CONFLICT (onestop_feed_id, attempt_id) DO UPDATE
    SET ready = false, migrated_at = NULL;

    SELECT COALESCE(sum(stop_count), 0) INTO expected
    FROM gtfs.maple_v2_structure
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    SELECT count(*) INTO staged FROM pg_temp.maple_v2_unified_stage;
    IF staged <> expected OR EXISTS (
        SELECT 1 FROM pg_temp.maple_v2_unified_stage
        WHERE onestop_feed_id IS DISTINCT FROM p_feed
           OR attempt_id IS DISTINCT FROM p_attempt
    ) THEN
        RAISE EXCEPTION 'Invalid staged v2 attempt %/%: staged %, expected %',
            p_feed, p_attempt, staged, expected;
    END IF;
    IF expected = 0 THEN
        DELETE FROM gtfs.maple_v2_stop_unified_attempt
        WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
        RETURN 0;
    END IF;

    -- Validate coverage per structure, not merely total row count. The
    -- primary keys on the target tables reject duplicate stop indices.
    IF EXISTS (
        SELECT 1
        FROM gtfs.maple_v2_structure s
        LEFT JOIN (
            SELECT structure_id, count(*) AS n, min(stop_index) AS lo, max(stop_index) AS hi
            FROM pg_temp.maple_v2_unified_stage GROUP BY structure_id
        ) q USING (structure_id)
        WHERE s.onestop_feed_id = p_feed AND s.attempt_id = p_attempt
          AND (q.n IS DISTINCT FROM s.stop_count::bigint
               OR q.lo IS DISTINCT FROM 0 OR q.hi IS DISTINCT FROM s.stop_count - 1)
    ) THEN
        RAISE EXCEPTION 'Invalid staged v2 structure coverage for %/%', p_feed, p_attempt;
    END IF;

    INSERT INTO gtfs.maple_v2_stop (
        onestop_feed_id, attempt_id, structure_id, stop_index,
        stop_id, gtfs_stop_sequence, metadata
    )
    SELECT onestop_feed_id, attempt_id, structure_id, stop_index,
           stop_id, gtfs_stop_sequence, metadata
    FROM pg_temp.maple_v2_unified_stage;

    INSERT INTO gtfs.maple_v2_stop_major (
        onestop_feed_id, attempt_id, structure_id, stop_index,
        base, unit, bit_width, payload, exception_indices, exception_arrivals,
        exception_departures, exception_interpolated, min_departure_offset, max_departure_offset
    )
    SELECT onestop_feed_id, attempt_id, structure_id, stop_index,
           base, unit, bit_width, payload, exception_indices, exception_arrivals,
           exception_departures, exception_interpolated, min_departure_offset, max_departure_offset
    FROM pg_temp.maple_v2_unified_stage;

    INSERT INTO gtfs.maple_v2_stop_unified (
        onestop_feed_id, attempt_id, structure_id, stop_index,
        base, unit, bit_width, payload, exception_indices, exception_arrivals,
        exception_departures, exception_interpolated, min_departure_offset,
        max_departure_offset, stop_id, gtfs_stop_sequence, metadata
    )
    SELECT onestop_feed_id, attempt_id, structure_id, stop_index,
           base, unit, bit_width, payload, exception_indices, exception_arrivals,
           exception_departures, exception_interpolated, min_departure_offset,
           max_departure_offset, stop_id, gtfs_stop_sequence, metadata
    FROM pg_temp.maple_v2_unified_stage
    ORDER BY stop_id, structure_id, stop_index;
    GET DIAGNOSTICS copied = ROW_COUNT;
    IF copied <> expected THEN
        RAISE EXCEPTION 'Unified publication copied %, expected %', copied, expected;
    END IF;

    UPDATE gtfs.maple_v2_stop_unified_attempt SET ready = true, migrated_at = clock_timestamp()
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    RETURN copied;
END
$$;
