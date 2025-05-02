-- Your SQL goes here
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ingested_static' AND column_name = 'hash_of_file_contents') THEN
        ALTER TABLE gtfs.ingested_static ADD hash_of_file_contents TEXT;
    END IF;
END $$;