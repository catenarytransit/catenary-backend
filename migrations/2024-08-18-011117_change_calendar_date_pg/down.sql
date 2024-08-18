-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.calendar_dates DROP CONSTRAINT calendar_dates_pkey;
ALTER TABLE gtfs.calendar_dates ADD PRIMARY KEY (onestop_feed_id, service_id, gtfs_date);