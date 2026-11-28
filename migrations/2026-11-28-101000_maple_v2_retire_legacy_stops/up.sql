-- Contract phase. Diesel executes this migration in one transaction.
-- Explicit opt-in for populated databases after every phase-2 binary has
-- been deployed. Empty CI databases can migrate without the setting.
SET LOCAL lock_timeout = '10s';
SET LOCAL jit = off;

-- Block ingestion and cleanup for the duration of the parity check, but
-- permit reads until the final DDL takes its short exclusive locks.
LOCK TABLE gtfs.maple_v2_structure, gtfs.maple_v2_stop,
    gtfs.maple_v2_stop_major, gtfs.maple_v2_stop_unified,
    gtfs.maple_v2_stop_unified_attempt IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF current_setting('catenary.maple_v2_contract', true) IS DISTINCT FROM 'on'
       AND (EXISTS (SELECT 1 FROM gtfs.maple_v2_structure)
            OR EXISTS (SELECT 1 FROM gtfs.maple_v2_stop)
            OR EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_major)
            OR EXISTS (SELECT 1 FROM gtfs.maple_v2_stop_unified)) THEN
        RAISE EXCEPTION 'Deploy/restart all phase-2 binaries before retiring old v2 tables. Set PGOPTIONS=-c catenary.maple_v2_contract=on only when ready.';
    END IF;

    IF EXISTS (
        SELECT 1 FROM gtfs.maple_v2_structure s
        LEFT JOIN gtfs.maple_v2_stop_unified_attempt a USING (onestop_feed_id, attempt_id)
        WHERE a.ready IS DISTINCT FROM true
    ) THEN
        RAISE EXCEPTION 'Unready Maple v2 attempts remain; run the backfill before retiring old tables';
    END IF;

    -- A ready flag and total row count alone cannot establish coverage.
    -- Verify each structure has exactly the expected contiguous stop indices.
    IF EXISTS (
        SELECT 1 FROM gtfs.maple_v2_structure s
        LEFT JOIN (
            SELECT onestop_feed_id, attempt_id, structure_id,
                   count(*) AS n, min(stop_index) AS lo, max(stop_index) AS hi
            FROM gtfs.maple_v2_stop_unified GROUP BY 1,2,3
        ) u USING (onestop_feed_id, attempt_id, structure_id)
        WHERE u.n IS DISTINCT FROM s.stop_count::bigint
           OR u.lo IS DISTINCT FROM 0 OR u.hi IS DISTINCT FROM s.stop_count - 1
    ) THEN
        RAISE EXCEPTION 'Unified stop structure coverage mismatch';
    END IF;

    -- Compare both physical source tables independently with the unified
    -- representation. FULL JOIN catches missing/extra rows in either
    -- direction, including a structural stop without a timing column.
    IF EXISTS (
        SELECT 1 FROM gtfs.maple_v2_stop s
        FULL JOIN gtfs.maple_v2_stop_unified u USING
            (onestop_feed_id, attempt_id, structure_id, stop_index)
        WHERE s.onestop_feed_id IS NULL OR u.onestop_feed_id IS NULL
           OR ROW(s.stop_id, s.gtfs_stop_sequence, s.metadata)
              IS DISTINCT FROM ROW(u.stop_id, u.gtfs_stop_sequence, u.metadata)
    ) THEN
        RAISE EXCEPTION 'Unified structural stop rows differ from original storage';
    END IF;
    IF EXISTS (
        SELECT 1 FROM gtfs.maple_v2_stop_major c
        FULL JOIN gtfs.maple_v2_stop_unified u USING
            (onestop_feed_id, attempt_id, structure_id, stop_index)
        WHERE c.onestop_feed_id IS NULL OR u.onestop_feed_id IS NULL
           OR ROW(c.base, c.unit, c.bit_width, c.payload, c.exception_indices,
                  c.exception_arrivals, c.exception_departures, c.exception_interpolated,
                  c.min_departure_offset, c.max_departure_offset)
              IS DISTINCT FROM
              ROW(u.base, u.unit, u.bit_width, u.payload, u.exception_indices,
                  u.exception_arrivals, u.exception_departures, u.exception_interpolated,
                  u.min_departure_offset, u.max_departure_offset)
    ) THEN
        RAISE EXCEPTION 'Unified timing columns differ from original storage';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'gtfs.maple_v2_stop_unified'::regclass
          AND confrelid = 'gtfs.maple_v2_structure'::regclass
          AND conname = 'maple_v2_stop_unified_structure_fk'
          AND contype = 'f' AND convalidated AND confdeltype = 'c'
    ) THEN
        RAISE EXCEPTION 'Validated direct structure cascade FK is required';
    END IF;
END
$$;

-- Keep view identities, column names/types and publisher signature stable.
CREATE OR REPLACE VIEW gtfs.maple_v2_stop_metadata_reader AS
SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
       u.stop_id, u.gtfs_stop_sequence, u.metadata
FROM gtfs.maple_v2_stop_unified u;

CREATE OR REPLACE VIEW gtfs.maple_v2_stop_columns_reader AS
SELECT u.onestop_feed_id, u.attempt_id, u.structure_id, u.stop_index,
       u.base, u.unit, u.bit_width, u.payload, u.exception_indices,
       u.exception_arrivals, u.exception_departures, u.exception_interpolated,
       u.min_departure_offset, u.max_departure_offset,
       u.stop_id, u.gtfs_stop_sequence, u.metadata
FROM gtfs.maple_v2_stop_unified u;

CREATE OR REPLACE FUNCTION gtfs.maple_v2_publish_unified(p_feed text, p_attempt text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    expected bigint;
    staged bigint;
    copied bigint;
BEGIN
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

    RETURN copied;
END
$$;

-- The generated name of the transitional FK is not an API; resolve it
-- by its referenced table and verify exactly one dependency is removed.
DO $$
DECLARE
    fk_name text;
BEGIN
    SELECT conname INTO STRICT fk_name FROM pg_constraint
    WHERE conrelid = 'gtfs.maple_v2_stop_unified'::regclass
      AND confrelid = 'gtfs.maple_v2_stop_major'::regclass
      AND contype = 'f';
    EXECUTE format('ALTER TABLE gtfs.maple_v2_stop_unified DROP CONSTRAINT %I', fk_name);
END
$$;

ALTER TABLE gtfs.maple_v2_stop_unified
    ADD CONSTRAINT maple_v2_stop_unified_nonnegative_index CHECK (stop_index >= 0);

DROP TRIGGER maple_v2_stop_unified_insert ON gtfs.maple_v2_stop;
DROP TRIGGER maple_v2_stop_unified_update ON gtfs.maple_v2_stop;
DROP TRIGGER maple_v2_stop_unified_delete ON gtfs.maple_v2_stop;
DROP TRIGGER maple_v2_major_unified_insert ON gtfs.maple_v2_stop_major;
DROP TRIGGER maple_v2_major_unified_update ON gtfs.maple_v2_stop_major;
DROP TRIGGER maple_v2_major_unified_delete ON gtfs.maple_v2_stop_major;
DROP FUNCTION gtfs.maple_v2_unified_invalidate();
DROP FUNCTION gtfs.maple_v2_backfill_unified(text, text);

-- No CASCADE: an unexpected dependent object must abort the migration.
DROP TABLE gtfs.maple_v2_stop_major RESTRICT;
DROP TABLE gtfs.maple_v2_stop RESTRICT;
DROP TABLE gtfs.maple_v2_stop_unified_attempt RESTRICT;
