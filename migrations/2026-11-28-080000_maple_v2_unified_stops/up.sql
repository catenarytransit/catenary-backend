-- Expand phase: keep both original stop tables and all compatibility views.
-- The new table is a transactional, ordered copy of complete feed attempts.
CREATE TABLE gtfs.maple_v2_stop_unified (
    LIKE gtfs.maple_v2_stop_major INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    stop_id text NOT NULL,
    gtfs_stop_sequence bigint NOT NULL CHECK (gtfs_stop_sequence BETWEEN 0 AND 4294967295),
    metadata jsonb NOT NULL,
    PRIMARY KEY (onestop_feed_id, attempt_id, structure_id, stop_index),
    -- During dual storage, even an independent legacy stop-major deletion
    -- must cascade. Replace this FK with the structure FK at contract time.
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id, stop_index)
      REFERENCES gtfs.maple_v2_stop_major ON DELETE CASCADE
);
CREATE INDEX maple_v2_stop_unified_lookup ON gtfs.maple_v2_stop_unified
    (stop_id, onestop_feed_id, attempt_id);

CREATE TABLE gtfs.maple_v2_stop_unified_attempt (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    ready boolean NOT NULL DEFAULT false,
    migrated_at timestamptz,
    PRIMARY KEY (onestop_feed_id, attempt_id)
);

-- Legacy writers can still ingest new attempts. Invalidate readiness in
-- the same transaction as any insertion or update to either source table.
-- The upsert also serializes mutations with a concurrent rebuild of the
-- same attempt. Deletes are mirrored by the FK cascade above.
CREATE FUNCTION gtfs.maple_v2_unified_invalidate() RETURNS trigger
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
        SET ready = false, migrated_at = NULL
        WHERE maple_v2_stop_unified_attempt.ready;
    ELSIF TG_OP = 'DELETE' THEN
        -- Cascades remove unified rows, but a partial source deletion also
        -- invalidates completeness. Status rows are intentionally independent.
        UPDATE gtfs.maple_v2_stop_unified_attempt a
        SET ready = false, migrated_at = NULL
        FROM (SELECT DISTINCT onestop_feed_id, attempt_id FROM old_rows) k
        WHERE a.onestop_feed_id = k.onestop_feed_id AND a.attempt_id = k.attempt_id
          AND a.ready;
    ELSE
        INSERT INTO gtfs.maple_v2_stop_unified_attempt (onestop_feed_id, attempt_id)
        SELECT DISTINCT onestop_feed_id, attempt_id FROM new_rows
        ORDER BY onestop_feed_id, attempt_id
        ON CONFLICT (onestop_feed_id, attempt_id) DO UPDATE
        SET ready = false, migrated_at = NULL
        WHERE maple_v2_stop_unified_attempt.ready;
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

-- Rebuild one attempt atomically. No new GTFS decode or extra Rust buffer.
-- Source rows are immutable in normal ingestion. Concurrent mutation of
-- the same attempt is serialized by its status row; an FK conflict/deadlock
-- during concurrent cleanup aborts the transaction safely and can be retried.
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
