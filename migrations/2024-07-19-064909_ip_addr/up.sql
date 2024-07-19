-- Your SQL goes here
CREATE TABLE gtfs.ip_addr_to_geo (
    is_ipv6 boolean NOT NULL,
    range_start inet NOT NULL,
    range_end inet NOT NULL,
    country_code text,
    geo_state text,
    geo_state2 text,
    city text,
    postcode text,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    timezone text,
    PRIMARY KEY (range_start, range_end)
);

CREATE INDEX ip_geo_addr_start_idx ON gtfs.ip_addr_to_geo (range_start);
CREATE INDEX ip_geo_addr_end_idx ON gtfs.ip_addr_to_geo (range_end);