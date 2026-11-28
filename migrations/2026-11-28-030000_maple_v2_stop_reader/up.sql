-- Geometry/stop-membership consumers do not need to decode timing payloads.
-- Metadata remains the canonical source of chateau and itinerary identity.
CREATE VIEW gtfs.itinerary_pattern_stop_reader AS
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
