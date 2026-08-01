-- This schema should Only be used for individual vehicle history data, 
--not for full train formations that require individual identification, or for any european complex data.
CREATE TABLE gtfs.basic_vehicle_history (
    realtime_feed_id TEXT NOT NULL,
    chateau TEXT NOT NULL,
    route_id TEXT NOT NULL,
    agency_id TEXT,
    unified_agency_id TEXT,
    vehicle_label TEXT,
    trip_id TEXT,
    block_id TEXT,
    operation_date DATE NOT NULL,
    PRIMARY KEY (realtime_feed_id, vehicle_label, operation_date, trip_id)
);

CREATE TABLE gtfs.basic_vehicles (
    unified_agency_id TEXT,
    vehicle_label TEXT,
    trip_id TEXT,
    block_id TEXT,
    model TEXT,
    manufacturer TEXT,
    manufacture_year INTEGER,
    PRIMARY KEY (unified_agency_id, vehicle_label)
);

ALTER TABLE gtfs.unified_agency ADD COLUMN has_vehicle_histories BOOLEAN NOT NULL DEFAULT FALSE;