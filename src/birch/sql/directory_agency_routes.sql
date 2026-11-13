-- Keep feed counts outside the route loop. An inlined aggregate can be rescanned
-- once for every route, especially when a generic plan underestimates the result.
WITH target_agencies AS MATERIALIZED (
    SELECT DISTINCT chateau, static_onestop_id, attempt_id, agency_id
    FROM gtfs.agencies
    WHERE unified_agency_id = $1
),
target_feeds AS (
    SELECT DISTINCT chateau, static_onestop_id, attempt_id
    FROM target_agencies
),
feed_agency_counts AS MATERIALIZED (
    SELECT a.chateau, a.static_onestop_id, a.attempt_id,
           COUNT(DISTINCT a.agency_id) AS agency_count
    FROM gtfs.agencies a
    JOIN target_feeds f
      ON f.chateau = a.chateau
     AND f.static_onestop_id = a.static_onestop_id
     AND f.attempt_id = a.attempt_id
    GROUP BY a.chateau, a.static_onestop_id, a.attempt_id
),
-- Resolve membership once and reuse these narrow rows for cards and slugs.
-- EXISTS avoids multiplying routes by every target agency in a shared feed.
agency_routes AS MATERIALIZED (
    SELECT r.onestop_feed_id, r.attempt_id, r.chateau, r.route_id,
           r.short_name, r.long_name, r.gtfs_desc, r.route_type, r.url,
           r.agency_id, r.color, r.text_color, r.gtfs_order,
           r.url_slug_for_unified_agency
    FROM gtfs.routes r
    JOIN feed_agency_counts c
      ON c.chateau = r.chateau
     AND c.static_onestop_id = r.onestop_feed_id
     AND c.attempt_id = r.attempt_id
    WHERE (
        (r.agency_id IS NULL AND c.agency_count = 1)
        OR EXISTS (
            SELECT 1 FROM target_agencies a
            WHERE a.chateau = r.chateau
              AND a.static_onestop_id = r.onestop_feed_id
              AND a.attempt_id = r.attempt_id
              AND a.agency_id = r.agency_id
        )
    )
),
route_slugs AS (
    SELECT DISTINCT ON (route_id) route_id, url_slug_for_unified_agency
    FROM agency_routes
    WHERE url_slug_for_unified_agency IS NOT NULL
    ORDER BY route_id, gtfs_order NULLS LAST, onestop_feed_id, attempt_id
)
SELECT DISTINCT ON (r.chateau, r.route_id)
    r.onestop_feed_id, r.chateau, r.route_id, r.short_name, r.long_name,
    r.gtfs_desc, r.route_type, r.url, r.agency_id, r.color, r.text_color,
    r.gtfs_order::bigint AS gtfs_order, s.url_slug_for_unified_agency
FROM agency_routes r
LEFT JOIN route_slugs s ON s.route_id = r.route_id
ORDER BY r.chateau, r.route_id, r.gtfs_order NULLS LAST,
         r.short_name NULLS LAST, r.onestop_feed_id, r.attempt_id
