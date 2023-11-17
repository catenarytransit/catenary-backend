pub async fn make_prod_index(client: &tokio_postgres::Client, schemaname: &String) {
    println!("making index");
    
    client.batch_execute(format!("
    CREATE INDEX IF NOT EXISTS gtfs_static_geom_idx ON {schemaname}.shapes USING GIST (linestring);

    CREATE INDEX IF NOT EXISTS gtfs_static_stops_geom_idx ON {schemaname}.stops USING GIST (point);

    CREATE INDEX IF NOT EXISTS gtfs_static_stoptimes_geom_idx ON {schemaname}.stoptimes USING GIST (point);

    CREATE INDEX IF NOT EXISTS gtfs_static_feed_id ON {schemaname}.shapes (onestop_feed_id);

    CREATE INDEX IF NOT EXISTS gtfs_static_feed ON {schemaname}.routes (onestop_feed_id);

    CREATE INDEX IF NOT EXISTS gtfs_static_route_type ON {schemaname}.routes (route_type);
    
    CREATE INDEX IF NOT EXISTS static_hulls ON {schemaname}.static_feeds USING GIST (hull);").as_str(),
        )
        .await
        .unwrap();

        println!("make static hulls...");

        client
            .batch_execute(
                format!(
                    "
            
            CREATE INDEX IF NOT EXISTS static_hulls ON {schemaname}.static_feeds USING GIST (hull);"
                )
                .as_str(),
            )
            .await
            .unwrap();
}
