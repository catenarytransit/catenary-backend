-- This file should undo anything in `up.sql`
-- Your SQL goes here
ALTER TABLE gtfs.realtime_feeds DROP COLUMN realtime_vehicle_positions;
ALTER TABLE gtfs.realtime_feeds DROP COLUMN realtime_trip_updates;
ALTER TABLE gtfs.realtime_feeds DROP COLUMN realtime_alerts;