use clap::Parser;

#[derive(Parser)]
struct Flags {
    #[clap(long)]
    email: String,
    #[clap(long)]
    password: String,
}

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use std::error::Error;
use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, PasswordVerifier, SaltString
    },
    Argon2
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {

    let flags: Flags = Flags::parse();

    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let mut conn = &mut conn_pool.get().await?;

    let email = flags.email;
    let password = flags.password;

    //generate a salted Password
    let password_bytes = password.as_bytes();
    let salt = SaltString::generate(&mut OsRng);

    let argon2 = Argon2::default();

    let password_hash = argon2.hash_password(password_bytes, &salt).unwrap();

    let serialised_hash = password_hash.to_string();
    let serialised_salt = salt.to_string();

    /*
    CREATE TABLE gtfs.admin_credentials (
    email text NOT NULL PRIMARY KEY,
    hash text NOT NULL,
    salt text NOT NULL,
    last_updated_ms bigint NOT NULL
    );
    */

    use catenary::schema::gtfs::admin_credentials as ac_table;
    use catenary::models::AdminCredentials;

    let unix_time = catenary::duration_since_unix_epoch().as_millis() as i64;

    let new_admin = AdminCredentials {
        email: email,
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