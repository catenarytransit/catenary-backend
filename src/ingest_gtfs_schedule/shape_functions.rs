pub async fn render_vector_tile_functions(client: &tokio_postgres::Client) {
    client.batch_execute(format!("
        CREATE OR REPLACE
        FUNCTION gtfs.busonly(z integer, x integer, y integer)
        RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'busonly', 4096, 'geom') FROM (
        SELECT
        ST_AsMVTGeom(
            ST_Transform(linestring, 3857),
            ST_TileEnvelope(z, x, y),
            4096, 64, true) AS geom,
            onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
        FROM gtfs.shapes
        WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 3 OR route_type = 11)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION gtfs.notbus(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'notbus', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND route_type != 3 AND route_type != 11
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION gtfs.localrail(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'localrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 0 OR route_type = 1 OR route_type = 5 OR route_type = 12)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client
        .batch_execute(
            format!(
                "
    CREATE OR REPLACE
    FUNCTION gtfs.intercityrail(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 2)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    "
            )
            .as_str(),
        )
        .await
        .unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION gtfs.intercityrail(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'intercityrail', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(linestring, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, shape_id, color, routes, route_type, route_label, text_color
    FROM gtfs.shapes
    WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 4 OR route_type = 6 OR route_type = 7)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();
}
