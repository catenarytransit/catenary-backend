use tokio_postgres::{Error as PostgresError, NoTls};

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

    client.batch_execute(
        "
        BEGIN;
         DROP SCHEMA IF EXISTS gtfs CASCADE;
         ALTER SCHEMA gtfs_stage RENAME TO gtfs;
         COMMIT;",
    ).await.unwrap();

    println!("Done!");
}
