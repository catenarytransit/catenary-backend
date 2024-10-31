-- Your SQL goes here
ALTER TABLE gtfs.realtime_feeds ADD COLUMN realtime_vehicle_positions text;
ALTER TABLE gtfs.realtime_feeds ADD COLUMN realtime_trip_updates text;
ALTER TABLE gtfs.realtime_feeds ADD COLUMN realtime_alerts text;