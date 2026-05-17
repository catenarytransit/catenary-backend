CREATE INDEX IF NOT EXISTS trips_compressed_nyct_rt_suffix_idx
ON gtfs.trips_compressed (
  route_id,
  (substr(trip_id, strpos(trip_id, '_') + 1)),
  service_id
)
WHERE chateau = 'nyct';