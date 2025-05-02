-- Your SQL goes here
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_chateau_route') THEN
        CREATE INDEX idx_chateau_route ON gtfs.routes (chateau, route_id);
    END IF;
END $$;