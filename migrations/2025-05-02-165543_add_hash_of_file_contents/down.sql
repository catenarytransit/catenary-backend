-- This file should undo anything in `up.sql`
ALTER TABLE gtfs.ingested_static DROP COLUMN hash_of_file_contents;