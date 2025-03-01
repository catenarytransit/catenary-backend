use actix_web::{HttpRequest, HttpResponse, Responder, web};
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use catenary::postgres_tools::CatenaryPostgresPool;
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
            let admin_credentials = admin_credentials[0].clone();

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
    input_data: String,
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

    let time = catenary::duration_since_unix_epoch().as_millis() as i64;

    let data = ron::from_str::<EachPasswordRow>(&input_data);

    if data.is_err() {
        return HttpResponse::InternalServerError().body("Deserialise password failed");
    }

    let data = data.unwrap();

    //convert password format to js value
    let password_for_postgres = data
        .passwords
        .as_ref()
        .map(|x| serde_json::to_value(x).unwrap());

    use catenary::schema::gtfs::realtime_passwords as realtime_passwords_table;

    //insert or update the password
    use catenary::models::RealtimePasswordRow;

    let password_row = RealtimePasswordRow {
        onestop_feed_id: feed_id.clone(),
        passwords: password_for_postgres.clone(),
        last_updated_ms: time,
    };

    let insert_result = diesel::insert_into(realtime_passwords_table::table)
        .values(password_row)
        .on_conflict(realtime_passwords_table::onestop_feed_id)
        .do_update()
        .set((
            realtime_passwords_table::passwords.eq(password_for_postgres),
            realtime_passwords_table::last_updated_ms.eq(time),
        ))
        .execute(conn)
        .await;

    if let Err(insert_result) = &insert_result {
        eprintln!(
            "could not insert / update realtime passwords\n{}",
            insert_result
        );

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
        eprintln!(
            "could not insert / update realtime update interval\n{}",
            update_result
        );

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("insert update interval fail");
    }

    HttpResponse::Ok()
        .append_header(("Cache-Control", "no-cache"))
        .finish()
}

#[actix_web::get("/exportrealtimekeys/")]
pub async fn export_realtime_keys(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
) -> impl Responder {
    let email = req.headers().get("email");
    let password = req.headers().get("password");

    if email.is_none() || password.is_none() {
        return HttpResponse::Unauthorized().finish();
    }

    let email = email.unwrap().to_str().unwrap();
    let password = password.unwrap().to_str().unwrap();

    println!("email: {}, password: {}", email, password);

    let is_authorised = login(pool.as_ref().clone(), email, password).await.unwrap();

    if !is_authorised {
        return HttpResponse::Unauthorized().finish();
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    use catenary::schema::gtfs::realtime_passwords as realtime_passwords_table;

    let realtime_passwords = realtime_passwords_table::table
        .select(catenary::models::RealtimePasswordRow::as_select())
        .load::<catenary::models::RealtimePasswordRow>(conn)
        .await;

    //convert to csv
    use csv::WriterBuilder;

    let mut wtr = WriterBuilder::new().from_writer(vec![]);

    wtr.serialize(("onestop_feed_id", "passwords", "last_updated_ms"))
        .unwrap();

    for row in realtime_passwords.unwrap() {
        wtr.serialize((row.onestop_feed_id, row.passwords, row.last_updated_ms))
            .unwrap();
    }

    let data_str = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

    HttpResponse::Ok()
        .append_header(("Cache-Control", "no-cache"))
        .body(data_str)
}

#[derive(Deserialize)]
struct RetrieveReq {
    email: String,
    password: String,
}

#[actix_web::post("/getrealtimekeys")]
pub async fn get_realtime_keys(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
    web::Form(RetrieveReq { email, password }): web::Form<RetrieveReq>,
) -> impl Responder {
    //check if the user is authorised

    println!("login attempt: email: {}, password: {}", email, password);

    let is_authorised = login(pool.as_ref().clone(), &email, &password)
        .await
        .unwrap();

    if !is_authorised {
        return HttpResponse::Unauthorized().finish();
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    use catenary::schema::gtfs::realtime_passwords as realtime_passwords_table;

    let realtime_passwords = realtime_passwords_table::table
        .select(catenary::models::RealtimePasswordRow::as_select())
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
                        let password_formatted = password.passwords.as_ref().map(|value| {
                            serde_json::from_value::<catenary::agency_secret::PasswordFormat>(
                                value.clone(),
                            )
                            .unwrap()
                        });

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
                                fetch_interval_ms,
                            },
                        );
                    }

                    HttpResponse::Ok()
                        .append_header(("Cache-Control", "no-cache"))
                        .json(KeyResponse { passwords })
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return HttpResponse::InternalServerError()
                        .append_header(("Cache-Control", "no-cache"))
                        .finish();
                }
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            HttpResponse::InternalServerError()
                .append_header(("Cache-Control", "no-cache"))
                .finish()
        }
    }
}
