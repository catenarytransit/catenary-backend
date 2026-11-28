-- Roll back new application binaries before reverting this migration.
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

CREATE OR REPLACE VIEW gtfs.itinerary_pattern_departures AS
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
LEFT JOIN gtfs.maple_v2_stop s ON s.onestop_feed_id=t.onestop_feed_id
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
JOIN gtfs.maple_v2_stop s USING (onestop_feed_id, attempt_id, structure_id)
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

DROP VIEW gtfs.maple_v2_stop_columns_reader;
DROP VIEW gtfs.maple_v2_stop_metadata_reader;
DROP FUNCTION gtfs.maple_v2_publish_unified(text, text);
ALTER TABLE gtfs.maple_v2_stop_unified DROP CONSTRAINT maple_v2_stop_unified_structure_fk;
-- The correction to marker invalidation is deliberately retained.
