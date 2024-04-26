use actix_web::dev::Service;
use actix_web::middleware::DefaultHeaders;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct EachPasswordRow {
    pub passwords: Option<catenary::agency_secret::PasswordFormat>,
    pub fetch_interval_ms: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct KeyResponse {
    passwords: HashMap<String, EachPasswordRow>,
}

// Admin Credential login
pub async fn login(
    pool: Arc<CatenaryPostgresPool>,
    email: &str,
    password: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    use catenary::schema::gtfs::admin_credentials as admin_credentials_table;

    let admin_credentials = admin_credentials_table::table
        .filter(admin_credentials_table::email.eq(email))
        .select(admin_credentials_table::all_columns)
        .load::<catenary::models::AdminCredentials>(conn)
        .await?;

    match admin_credentials.len() {
        0 => Ok(false),
        _ => {
            let admin_credentials = admin_credentials.get(0).unwrap();

            let db_hash = admin_credentials.hash.clone();
            let db_salt = SaltString::from_b64(admin_credentials.salt.as_str()).unwrap();

            let argon2 = Argon2::default();
            let user_submitted_hash = argon2
                .hash_password(password.as_bytes(), &db_salt)
                .unwrap()
                .to_string();

            let is_authorised = user_submitted_hash == db_hash;

            Ok(is_authorised)
        }
    }
}

#[actix_web::post("/setrealtimekey/{feed_id}/")]
pub async fn set_realtime_key(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
    feed_id: web::Path<String>,
    data: web::Json<EachPasswordRow>,
) -> impl Responder {
    let feed_id = feed_id.into_inner();

    let email = req.headers().get("email").unwrap().to_str().unwrap();
    let password = req.headers().get("password").unwrap().to_str().unwrap();

    let is_authorised = login(pool.as_ref().clone(), email, password).await.unwrap();

    if !is_authorised {
        println!("User not authenticated! {}", email);
        return HttpResponse::Unauthorized().finish();
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    //convert password format to js value
    let password_format = serde_json::to_value(data.passwords.clone());

    if password_format.is_err() {
        return HttpResponse::InternalServerError().body(format!("{:#?}", password_format.unwrap_err()));
    }

    let password_format = password_format.unwrap();

    use catenary::schema::gtfs::realtime_passwords as realtime_passwords_table;

    //insert or update the password

    let insert_result = diesel::insert_into(realtime_passwords_table::table)
        .values((
            realtime_passwords_table::onestop_feed_id.eq(&feed_id),
            realtime_passwords_table::passwords.eq(password_format.clone()),
        ))
        .on_conflict(realtime_passwords_table::onestop_feed_id)
        .do_update()
        .set(realtime_passwords_table::passwords.eq(password_format))
        .execute(conn)
        .await;

    if let Err(insert_result) = &insert_result {
        eprintln!("could not insert / update realtime passwords\n{}", insert_result);

        return HttpResponse::InternalServerError().body("insert into realtime passwords failed");
    }

    //upload the fetch interval

    use catenary::schema::gtfs::realtime_feeds as realtime_feeds_table;

    let update_result = diesel::update(
        realtime_feeds_table::table.filter(realtime_feeds_table::onestop_feed_id.eq(&feed_id)),
    )
    .set(realtime_feeds_table::fetch_interval_ms.eq(data.fetch_interval_ms))
    .execute(conn)
    .await;

    if let Err(update_result) = &update_result {
        eprintln!("could not insert / update realtime update interval\n{}", update_result);

        return HttpResponse::InternalServerError().body("insert update interval fail");
    }

    HttpResponse::Ok().finish()
}

#[actix_web::get("/getrealtimekeys/")]
pub async fn get_realtime_keys(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
) -> impl Responder {
    //check if the user is authorised
    let email = req.headers().get("email").unwrap().to_str().unwrap();
    let password = req.headers().get("password").unwrap().to_str().unwrap();

    let is_authorised = login(pool.as_ref().clone(), email, password).await.unwrap();

    if !is_authorised {
        return HttpResponse::Unauthorized().finish();
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    use catenary::schema::gtfs::realtime_passwords as realtime_passwords_table;

    let realtime_passwords = realtime_passwords_table::table
        .select((catenary::models::RealtimePasswordRow::as_select()))
        .load::<catenary::models::RealtimePasswordRow>(conn)
        .await;

    match realtime_passwords {
        Ok(passwords) => {
            let realtime_feeds = catenary::schema::gtfs::realtime_feeds::table
                .select(catenary::models::RealtimeFeed::as_select())
                .load::<catenary::models::RealtimeFeed>(conn)
                .await;

            match realtime_feeds {
                Ok(realtime_feeds) => {
                    //sort the passwords into a BTreeMap
                    let mut raw_password_data: HashMap<
                        String,
                        Option<catenary::agency_secret::PasswordFormat>,
                    > = HashMap::new();
                    for password in passwords {
                        let password_formatted = match &password.passwords {
                            Some(value) => Some(
                                serde_json::from_value::<catenary::agency_secret::PasswordFormat>(
                                    value.clone(),
                                )
                                .unwrap(),
                            ),
                            None => None,
                        };

                        raw_password_data
                            .insert(password.onestop_feed_id.clone(), password_formatted);
                    }

                    //sort the feeds into a BTreeMap

                    let mut passwords: HashMap<String, EachPasswordRow> = HashMap::new();

                    for feed in realtime_feeds {
                        let password = raw_password_data.get(&feed.onestop_feed_id);
                        let fetch_interval_ms = feed.fetch_interval_ms;

                        passwords.insert(
                            feed.onestop_feed_id.clone(),
                            EachPasswordRow {
                                passwords: match password {
                                    Some(value) => value.clone(),
                                    None => None,
                                },
                                fetch_interval_ms: fetch_interval_ms,
                            },
                        );
                    }

                    HttpResponse::Ok().json(KeyResponse {
                        passwords: passwords,
                    })
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return HttpResponse::InternalServerError().finish();
                }
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
