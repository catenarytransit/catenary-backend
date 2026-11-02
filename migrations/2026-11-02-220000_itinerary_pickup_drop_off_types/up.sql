-- Preserve the original per-stop GTFS boarding and alighting methods.
-- Existing imports did not retain these fields; reimport feeds to recover them.
-- Keep unknown i16 values accepted by gtfs-structures, just as Maple Syrup does.
ALTER TABLE gtfs.itinerary_pattern
    ADD COLUMN pickup_type smallint NOT NULL DEFAULT 0,
    ADD COLUMN drop_off_type smallint NOT NULL DEFAULT 0;
