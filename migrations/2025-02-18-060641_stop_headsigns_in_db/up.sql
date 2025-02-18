ALTER TABLE gtfs.itinerary_pattern ADD stop_headsign_idx smallint;
ALTER TABLE gtfs.direction_pattern ADD stop_headsign_idx smallint;
ALTER TABLE gtfs.direction_pattern_meta ADD stop_headsigns_unique_list text[];