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
