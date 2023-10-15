use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    let postgresstring = arguments::parse(std::env::args())
        .unwrap()
        .get::<String>("postgres");

    let postgresstring = match postgresstring {
        Some(s) => s,
        None => {
            panic!("You need a postgres string");
        }
    };

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(&postgresstring, NoTls)
        .await
        .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    println!("Connected to database\nSwapping...");

    client
        .batch_execute(
            "
        BEGIN;
         DROP SCHEMA IF EXISTS gtfs CASCADE;
         ALTER SCHEMA gtfs_stage RENAME TO gtfs;
         COMMIT;",
        )
        .await
        .unwrap();

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

    println!("Making custom tiler");
    
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

    println!("Done!");
}
