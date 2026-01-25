-- Add is_derivative column to track computed centroid points from ways/relations
ALTER TABLE gtfs.osm_stations ADD COLUMN is_derivative BOOLEAN NOT NULL DEFAULT FALSE;
