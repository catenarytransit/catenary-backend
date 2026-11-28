-- Additive pilot: legacy itinerary metadata and trip IDs remain authoritative.
CREATE TABLE gtfs.maple_v2_structure (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    structure_id text NOT NULL,
    stop_count integer NOT NULL CHECK (stop_count > 0),
    timing_count integer NOT NULL CHECK (timing_count > 0),
    encoding_version smallint NOT NULL DEFAULT 1 CHECK (encoding_version = 1),
    PRIMARY KEY (onestop_feed_id, attempt_id, structure_id),
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id)
        REFERENCES gtfs.itinerary_pattern_meta(onestop_feed_id, attempt_id, itinerary_pattern_id)
        ON DELETE CASCADE
);
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
CREATE TABLE gtfs.maple_v2_trip_major (
    onestop_feed_id text NOT NULL,
    attempt_id text NOT NULL,
    structure_id text NOT NULL,
    timing_index integer NOT NULL CHECK (timing_index >= 0),
    itinerary_pattern_id text NOT NULL,
    byte_width integer NOT NULL CHECK (byte_width IN (2, 4)),
    payload bytea NOT NULL,
    exception_indices integer[] NOT NULL,
    exception_arrivals integer[] NOT NULL,
    exception_departures integer[] NOT NULL,
    exception_interpolated integer[] NOT NULL,
    CHECK (cardinality(exception_indices) = cardinality(exception_arrivals)
       AND cardinality(exception_indices) = cardinality(exception_departures)
       AND cardinality(exception_indices) = cardinality(exception_interpolated)),
    PRIMARY KEY (onestop_feed_id, attempt_id, structure_id, timing_index),
    UNIQUE (onestop_feed_id, attempt_id, itinerary_pattern_id),
    FOREIGN KEY (onestop_feed_id, attempt_id, structure_id)
        REFERENCES gtfs.maple_v2_structure ON DELETE CASCADE,
    FOREIGN KEY (onestop_feed_id, attempt_id, itinerary_pattern_id)
        REFERENCES gtfs.itinerary_pattern_meta ON DELETE CASCADE
);
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
-- Chateau remains authoritative in existing itinerary metadata; reassignment
-- needs no rewrite of v2 payloads. Existing trip-ID indexes remain usable.
CREATE VIEW gtfs.maple_v2_itinerary_lookup AS
SELECT m.chateau, t.onestop_feed_id, t.attempt_id, t.itinerary_pattern_id,
       t.structure_id, t.timing_index
FROM gtfs.maple_v2_trip_major t
JOIN gtfs.itinerary_pattern_meta m USING (onestop_feed_id, attempt_id, itinerary_pattern_id);

-- Decode one little-endian fixed-width primary value without changing Gentian's
-- row contract. Signed or nullable values are carried in sparse exceptions.
CREATE FUNCTION gtfs.maple_v2_primary(data bytea, byte_width integer, idx integer)
RETURNS integer LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT (get_byte(data, idx * byte_width)::bigint
         + (get_byte(data, idx * byte_width + 1)::bigint << 8)
         + CASE WHEN byte_width = 4 THEN
             (get_byte(data, idx * byte_width + 2)::bigint << 16)
           + (get_byte(data, idx * byte_width + 3)::bigint << 24)
           ELSE 0 END)::integer
$$;

-- Per-itinerary fallback supports old attempts and feeds outside the allowlist.
CREATE VIEW gtfs.itinerary_pattern_gentian AS
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
JOIN gtfs.maple_v2_stop s USING (onestop_feed_id, attempt_id, structure_id)
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

-- Stop-major decoder: at most five bytes per value, including unaligned
-- 32-bit values. Width zero denotes a constant column.
CREATE FUNCTION gtfs.maple_v2_column_primary(data bytea, base bigint, unit bigint,
    width integer, idx integer)
RETURNS integer LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    bit_offset bigint := idx::bigint * width;
    byte_offset integer := (bit_offset / 8)::integer;
    shift integer := (bit_offset % 8)::integer;
    word bigint := 0;
    n integer;
BEGIN
    IF width = 0 THEN RETURN base::integer; END IF;
    FOR n IN 0..((shift + width - 1) / 8) LOOP
        word := word | (get_byte(data, byte_offset + n)::bigint << (8*n));
    END LOOP;
    RETURN (base + unit * ((word >> shift) & ((1::bigint << width)-1)))::integer;
END
$$;

-- Full itinerary row contract for APIs. Stop discovery uses shared stop rows
-- and expands only matching stop columns; legacy rows remain the fallback.
CREATE VIEW gtfs.itinerary_pattern_departures AS
SELECT t.onestop_feed_id, t.attempt_id, t.itinerary_pattern_id,
       s.stop_index AS stop_sequence,
       CASE WHEN e.pos IS NULL THEN p.value ELSE c.exception_arrivals[e.pos] END AS arrival_time_since_start,
       CASE WHEN e.pos IS NULL THEN p.value ELSE c.exception_departures[e.pos] END AS departure_time_since_start,
       CASE WHEN e.pos IS NULL THEN NULL::integer ELSE c.exception_interpolated[e.pos] END AS interpolated_time_since_start,
       s.stop_id, m.chateau, s.gtfs_stop_sequence::oid AS gtfs_stop_sequence,
       (s.metadata->>'timepoint')::boolean AS timepoint,
       (s.metadata->>'stop_headsign_idx')::smallint AS stop_headsign_idx,
       (s.metadata->>'pickup_type')::smallint AS pickup_type,
       (s.metadata->>'drop_off_type')::smallint AS drop_off_type
FROM gtfs.maple_v2_stop s
JOIN gtfs.maple_v2_stop_major c USING (onestop_feed_id, attempt_id, structure_id, stop_index)
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

-- Complete trip retrieval uses the trip-major decoder, not the transposed
-- columns. The full key prevents mixing identical IDs across feed attempts.
CREATE VIEW gtfs.itinerary_pattern_trip_reader AS
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
LEFT JOIN gtfs.maple_v2_stop s ON s.onestop_feed_id=t.onestop_feed_id
 AND s.attempt_id=t.attempt_id AND s.structure_id=t.structure_id AND s.stop_index=g.stop_sequence
LEFT JOIN gtfs.itinerary_pattern r ON t.itinerary_pattern_id IS NULL
 AND r.onestop_feed_id=g.onestop_feed_id AND r.attempt_id=g.attempt_id
 AND r.itinerary_pattern_id=g.itinerary_pattern_id AND r.stop_sequence=g.stop_sequence;
