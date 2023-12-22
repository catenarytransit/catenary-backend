pub async fn render_vector_tile_functions(client: &tokio_postgres::Client, schemaname: &String) {
    client.batch_execute(format!("
        CREATE OR REPLACE
        FUNCTION {schemaname}.busonly(z integer, x integer, y integer)
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
        WHERE (linestring && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (route_type = 3 OR route_type = 11 OR route_type = 200)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION {schemaname}.notbus(z integer, x integer, y integer)
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
    FUNCTION {schemaname}.localrail(z integer, x integer, y integer)
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
    FUNCTION {schemaname}.intercityrail(z integer, x integer, y integer)
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
    FUNCTION {schemaname}.other(z integer, x integer, y integer)
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

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION {schemaname}.stationfeatures(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'stationfeatures', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (location_type=2 OR location_type=3 OR location_type=4)
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION {schemaname}.busstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'busstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id,  REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[3,11,200,1700,1500,1702]::smallint[] && route_types::smallint[] OR ARRAY[3,11,200,1700,1500,1702]::smallint[] && children_route_types::smallint[]) AND hidden = false
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION {schemaname}.railstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'railstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types, hidden
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]) AND hidden = false
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();

    client.batch_execute(format!("
    CREATE OR REPLACE
    FUNCTION {schemaname}.otherstops(z integer, x integer, y integer)
    RETURNS bytea AS $$
    DECLARE
    mvt bytea;
    BEGIN
    SELECT INTO mvt ST_AsMVT(tile, 'otherstops', 4096, 'geom') FROM (
    SELECT
    ST_AsMVTGeom(
        ST_Transform(point, 3857),
        ST_TileEnvelope(z, x, y),
        4096, 64, true) AS geom,
        onestop_feed_id, REPLACE (name, 'Station','') as name, displayname, code, gtfs_desc, location_type, parent_station, zone_id, url, timezone, wheelchair_boarding, level_id, platform_code, routes, route_types, children_ids, children_route_types
    FROM gtfs.stops
    WHERE (point && ST_Transform(ST_TileEnvelope(z, x, y), 4326)) AND (ARRAY[4,6,7]::smallint[] && route_types::smallint[] OR ARRAY[4,6,7]::smallint[] && children_route_types::smallint[])
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
    END
    $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
    ").as_str()).await.unwrap();
}
