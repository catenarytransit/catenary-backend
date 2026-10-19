-- Globeflower seeds direction patterns by the full feed/attempt/route key.
-- Previous direction_onestop_feed_id_route_id indexes only (feed, route),
-- leaving a costly attempt_id filter for feeds containing multiple ingests.
-- direction_pattern_meta's PK is keyed by direction_pattern_id, not route_id.
CREATE INDEX IF NOT EXISTS idx_globeflower_dpm_feed_attempt_route
    ON gtfs.direction_pattern_meta
    (onestop_feed_id, attempt_id, route_id)
    INCLUDE (direction_pattern_id, route_type);
