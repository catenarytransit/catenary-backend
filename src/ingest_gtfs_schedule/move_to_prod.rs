use tokio_postgres::NoTls;

mod make_prod_index;
mod shape_functions;

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

    println!("Connected to database");

    println!("Building indexes");

    make_prod_index::make_prod_index(&client, &String::from("gtfs_stage")).await;

    println!("Building martin functions");

    shape_functions::render_vector_tile_functions(&client, &String::from("gtfs_stage")).await;

    println!("Swapping tables");

    client
        .batch_execute(
            "BEGIN; DROP SCHEMA IF EXISTS gtfs CASCADE; ALTER SCHEMA gtfs_stage RENAME TO gtfs; COMMIT;",
        )
        .await
        .unwrap();

    println!("Adding Postgis to search path");

    client.batch_execute("ALTER DATABASE gtfs SET search_path = public, postgis;")
    .await.unwrap();

    println!("Done!");
}
