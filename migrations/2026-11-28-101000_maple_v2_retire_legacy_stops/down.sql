-- Reconstruct both original tables from the authoritative unified data,
-- including attempts ingested after retirement. Pause writers and reserve
-- sufficient disk/WAL space before running this transactional rollback.
SET LOCAL lock_timeout = '10s';
LOCK TABLE gtfs.maple_v2_structure, gtfs.maple_v2_stop_unified
    IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE gtfs.maple_v2_stop (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    structure_id text NOT NULL,
    stop_index integer NOT NULL CHECK (stop_index >= 0),
    stop_id text NOT NULL,
    gtfs_stop_sequence bigint NOT NULL CHECK (gtfs_stop_sequence BETWEEN 0 AND 4294967295),
    metadata jsonb NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, structure_id, stop_index),
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id)
        REFERENCES gtfs.maple_v2_structure ON DELETE CASCADE
);
CREATE INDEX maple_v2_stop_lookup ON gtfs.maple_v2_stop(stop_id, onestop_feed_id, attempt_id);

CREATE TABLE gtfs.maple_v2_stop_major (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    structure_id text NOT NULL,
    stop_index integer NOT NULL,
    base bigint NOT NULL CHECK (base >= 0),
    unit bigint NOT NULL CHECK (unit > 0),
    bit_width integer NOT NULL CHECK (bit_width BETWEEN 0 AND 32),
    payload bytea NOT NULL,
    exception_indices integer[] NOT NULL,
    exception_arrivals integer[] NOT NULL,
    exception_departures integer[] NOT NULL,
    exception_interpolated integer[] NOT NULL,
    min_departure_offset integer,
    max_departure_offset integer,
    CHECK (min_departure_offset <= max_departure_offset),
    CHECK (cardinality(exception_indices) = cardinality(exception_arrivals)
       AND cardinality(exception_indices) = cardinality(exception_departures)
       AND cardinality(exception_indices) = cardinality(exception_interpolated)),
    PRIMARY KEY (onestop_feed_id, attempt_id, structure_id, stop_index),
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id, stop_index)
        REFERENCES gtfs.maple_v2_stop ON DELETE CASCADE
);

CREATE TABLE gtfs.maple_v2_stop_unified_attempt (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    ready boolean NOT NULL DEFAULT false,
    migrated_at timestamptz,
    PRIMARY KEY (onestop_feed_id, attempt_id)
);

INSERT INTO gtfs.maple_v2_stop
    (onestop_feed_id, attempt_id, structure_id, stop_index, stop_id, gtfs_stop_sequence, metadata)
SELECT onestop_feed_id, attempt_id, structure_id, stop_index, stop_id, gtfs_stop_sequence, metadata
FROM gtfs.maple_v2_stop_unified
ORDER BY stop_id, onestop_feed_id, attempt_id, structure_id, stop_index;

INSERT INTO gtfs.maple_v2_stop_major
    (onestop_feed_id, attempt_id, structure_id, stop_index, base, unit, bit_width, payload,
     exception_indices, exception_arrivals, exception_departures, exception_interpolated,
     min_departure_offset, max_departure_offset)
SELECT onestop_feed_id, attempt_id, structure_id, stop_index, base, unit, bit_width, payload,
       exception_indices, exception_arrivals, exception_departures, exception_interpolated,
       min_departure_offset, max_departure_offset
FROM gtfs.maple_v2_stop_unified
ORDER BY onestop_feed_id, attempt_id, structure_id, stop_index;

INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id, ready, migrated_at)
SELECT DISTINCT onestop_feed_id, attempt_id, true, now()
FROM gtfs.maple_v2_stop_unified;

ALTER TABLE gtfs.maple_v2_stop_unified
    ADD FOREIGN KEY (onestop_feed_id, attempt_id, structure_id, stop_index)
        REFERENCES gtfs.maple_v2_stop_major ON DELETE CASCADE;
ALTER TABLE gtfs.maple_v2_stop_unified DROP CONSTRAINT maple_v2_stop_unified_nonnegative_index;

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

CREATE TRIGGER maple_v2_stop_unified_insert
AFTER INSERT ON gtfs.maple_v2_stop
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();
CREATE TRIGGER maple_v2_stop_unified_update
AFTER UPDATE ON gtfs.maple_v2_stop
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();
CREATE TRIGGER maple_v2_major_unified_insert
AFTER INSERT ON gtfs.maple_v2_stop_major
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();
CREATE TRIGGER maple_v2_major_unified_update
AFTER UPDATE ON gtfs.maple_v2_stop_major
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();
CREATE TRIGGER maple_v2_stop_unified_delete
AFTER DELETE ON gtfs.maple_v2_stop
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();
CREATE TRIGGER maple_v2_major_unified_delete
AFTER DELETE ON gtfs.maple_v2_stop_major
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION gtfs.maple_v2_unified_invalidate();

CREATE FUNCTION gtfs.maple_v2_backfill_unified(p_feed text, p_attempt text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    expected bigint;
    n_stops bigint;
    n_columns bigint;
    copied bigint;
BEGIN
    INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id)
    VALUES (p_feed, p_attempt)
    ON CONFLICT (onestop_feed_id, attempt_id) DO NOTHING;

    PERFORM 1 FROM gtfs.maple_v2_stop_unified_attempt
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt FOR UPDATE;

    SELECT COALESCE(sum(stop_count), 0) INTO expected
    FROM gtfs.maple_v2_structure
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    IF expected = 0 THEN
        DELETE FROM gtfs.maple_v2_stop_unified_attempt
        WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
        RETURN 0;
    END IF;

    SELECT count(*) INTO n_stops FROM gtfs.maple_v2_stop
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    SELECT count(*) INTO n_columns FROM gtfs.maple_v2_stop_major
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    IF n_stops <> expected OR n_columns <> expected THEN
        RAISE EXCEPTION 'Incomplete v2 attempt %/%: expected %, stops %, columns %',
            p_feed, p_attempt, expected, n_stops, n_columns;
    END IF;

    UPDATE gtfs.maple_v2_stop_unified_attempt SET ready = false, migrated_at = NULL
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    DELETE FROM gtfs.maple_v2_stop_unified
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;

    INSERT INTO gtfs.maple_v2_stop_unified (
        onestop_feed_id, attempt_id, structure_id, stop_index,
        base, unit, bit_width, payload, exception_indices, exception_arrivals,
        exception_departures, exception_interpolated, min_departure_offset,
        max_departure_offset, stop_id, gtfs_stop_sequence, metadata
    )
    SELECT c.onestop_feed_id, c.attempt_id, c.structure_id, c.stop_index,
           c.base, c.unit, c.bit_width, c.payload, c.exception_indices,
           c.exception_arrivals, c.exception_departures, c.exception_interpolated,
           c.min_departure_offset, c.max_departure_offset,
           s.stop_id, s.gtfs_stop_sequence, s.metadata
    FROM gtfs.maple_v2_stop s
    JOIN gtfs.maple_v2_stop_major c USING
        (onestop_feed_id, attempt_id, structure_id, stop_index)
    WHERE s.onestop_feed_id = p_feed AND s.attempt_id = p_attempt
    ORDER BY s.stop_id, s.structure_id, s.stop_index;
    GET DIAGNOSTICS copied = ROW_COUNT;
    IF copied <> expected THEN
        RAISE EXCEPTION 'Unified v2 attempt %/%: copied %, expected %',
            p_feed, p_attempt, copied, expected;
    END IF;

    UPDATE gtfs.maple_v2_stop_unified_attempt SET ready = true, migrated_at = clock_timestamp()
    WHERE onestop_feed_id = p_feed AND attempt_id = p_attempt;
    RETURN copied;
END
$$;

CREATE OR REPLACE VIEW gtfs.maple_v2_stop_metadata_reader AS
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

CREATE OR REPLACE VIEW gtfs.maple_v2_stop_columns_reader AS
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

CREATE OR REPLACE FUNCTION gtfs.maple_v2_publish_unified(p_feed text, p_attempt text)
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

ANALYZE gtfs.maple_v2_stop;
ANALYZE gtfs.maple_v2_stop_major;
ANALYZE gtfs.maple_v2_stop_unified_attempt;
