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
)
SELECT r.chateau, r.route_id
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
    AND (
        r.url_slug_for_unified_agency = $2
        OR (r.url_slug_for_unified_agency IS NULL AND r.route_id = $2)
    )
ORDER BY CASE WHEN r.url_slug_for_unified_agency = $2 THEN 0 ELSE 1 END,
         r.gtfs_order NULLS LAST, r.onestop_feed_id, r.attempt_id,
         r.chateau, r.route_id
LIMIT 1
