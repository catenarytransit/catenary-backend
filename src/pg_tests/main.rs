use catenary::postgres_tools::make_async_pool;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use dotenvy::dotenv;
use std::error::Error;
use std::f64::consts::E;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
    dotenv().ok();
    //test trip insertion

    let mut conn = AsyncPgConnection::establish(&std::env::var("DATABASE_URL")?).await?;

    use catenary::schema::gtfs::trips::dsl::*;

    conn.test_transaction::<_, diesel::result::Error, _>(|conn| {
        async move {
            let frequencies_vec = Some(vec![
                Some(catenary::models::TripFrequencyModel {
                    start_time: 0,
                    end_time: 0,
                    headway_secs: 10,
                    exact_times: false,
                }),
                Some(catenary::models::TripFrequencyModel {
                    start_time: 1,
                    end_time: 5,
                    headway_secs: 1000,
                    exact_times: true,
                }),
            ]);

            let trip_pg = catenary::models::Trip {
                onestop_feed_id: "feed_id".to_string(),
                trip_id: "trip_id".to_string(),
                attempt_id: "attempt_id".to_string(),
                service_id: "service_id".to_string(),
                trip_headsign: Some("University of California, Irvine".to_string()),
                trip_headsign_translations: None,
                route_id: "00000".to_string(),
                has_stop_headsigns: true,
                stop_headsigns: Some(vec![Some("University of California, Irvine".to_string())]),
                trip_short_name: Some("UCI".to_string()),
                direction_id: Some(1),
                bikes_allowed: 1,
                block_id: Some("block_id".to_string()),
                shape_id: Some("shape_id".to_string()),
                wheelchair_accessible: 0,
                chateau: "chateau".to_string(),
                frequencies: None,
                has_frequencies: false,
            };

            let insert_trip = diesel::insert_into(trips)
                .values(trip_pg)
                .execute(conn)
                .await;

            match insert_trip {
                Ok(_) => println!("Inserted trip"),
                Err(e) => {
                    println!("Error inserting trip: {:?}", e);
                    return Err(e.into());
                }
            }

            Ok(())
        }
        .scope_boxed()
    })
    .await;

    Ok(())
}
