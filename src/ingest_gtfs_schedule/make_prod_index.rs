pub async fn make_prod_index(client: &tokio_postgres::Client, schemaname: &String) {
    println!("making shapes geospacial index");
    
    client.batch_execute(format!("
    CREATE INDEX IF NOT EXISTS gtfs_static_geom_idx ON {schemaname}.shapes USING GIST (linestring);").as_str()).await.unwrap();
    println!("making geo index for stops");

    client.batch_execute(format!("
    CREATE INDEX IF NOT EXISTS gtfs_static_stops_geom_idx ON {schemaname}.stops USING GIST (point);").as_str()).await.unwrap();
    println!("making geo index for stop points");

    client.batch_execute(format!("
    CREATE INDEX IF NOT EXISTS gtfs_static_stoptimes_geom_idx ON {schemaname}.stoptimes USING GIST (point);").as_str()).await.unwrap();
    println!("making index for shapes on onestop feed id");

    client.batch_execute(format!("

    CREATE INDEX IF NOT EXISTS gtfs_static_feed_id ON {schemaname}.shapes (onestop_feed_id);").as_str()).await.unwrap();
    
    println!("text index on routes by onestop feed id");

    client.batch_execute(format!("

    CREATE INDEX IF NOT EXISTS gtfs_static_feed ON {schemaname}.routes (onestop_feed_id);

    ").as_str()).await.unwrap();
    
    println!("route index by route type");

    client.batch_execute(format!("

    CREATE INDEX IF NOT EXISTS gtfs_static_route_type ON {schemaname}.routes (route_type);
    ").as_str()).await.unwrap();

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
