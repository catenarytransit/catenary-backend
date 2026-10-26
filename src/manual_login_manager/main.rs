use clap::Parser;

#[derive(Parser)]
struct Flags {
    #[clap(long)]
    email: String,
    #[clap(long)]
    password: String,
}

use argon2::{Argon2, password_hash::PasswordHasher};
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use diesel_async::RunQueryDsl;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let flags: Flags = Flags::parse();

    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let conn = &mut conn_pool.get().await?;

    println!("Connected to postgres");

    let email = flags.email;
    let password = flags.password;

    // Argon2 0.6 automatically generates a random salt and embeds it in the PHC hash.
    let password_hash = Argon2::default().hash_password(password.as_bytes())?;

    // Retain the existing NOT NULL salt column for schema compatibility.
    let serialised_salt = password_hash
        .salt
        .as_ref()
        .expect("Argon2-generated password hash must include a salt")
        .to_string();
    let serialised_hash = password_hash.to_string();

    /*
    CREATE TABLE gtfs.admin_credentials (
    email text NOT NULL PRIMARY KEY,
    hash text NOT NULL,
    salt text NOT NULL,
    last_updated_ms bigint NOT NULL
    );
    */

    use catenary::models::AdminCredentials;
    use catenary::schema::gtfs::admin_credentials as ac_table;

    let unix_time = catenary::duration_since_unix_epoch().as_millis() as i64;

    let new_admin = AdminCredentials {
        email,
        hash: serialised_hash,
        salt: serialised_salt,
        last_updated_ms: unix_time,
    };

    let insert_result = diesel::insert_into(ac_table::dsl::admin_credentials)
        .values(&new_admin)
        .execute(conn)
        .await;

    match insert_result {
        Ok(_) => println!("Successfully inserted new admin"),
        Err(e) => println!("Error inserting new admin: {:?}", e),
    }

    Ok(())
}
