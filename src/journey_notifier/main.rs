//! Standalone journey-notifier: authenticated device subscriptions and persistent FCM outbox.
//! Run migrations before launching. Expose only behind a TLS reverse proxy.
mod firebase;
mod monitor;

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use anyhow::{Context, Result};
use catenary::catenaryconfig::CatenaryConfig;
use clap::Parser;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "catenaryconfig.toml")]
    config: PathBuf,
    #[arg(long)]
    database_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Leg {
    chateau: String,
    trip_id: String,
    trip_start_date: String,
    trip_start_time: Option<String>,
    from_stop_sequence: Option<u16>,
    to_stop_sequence: Option<u16>,
    from_stop_id: Option<String>,
    to_stop_id: Option<String>,
    from_name: String,
    to_name: String,
    route_name: String,
    trip_short_name: Option<String>,
    time_zone: Option<String>,
    scheduled_departure: i64,
    scheduled_arrival: i64,
    expected_departure: i64,
    expected_arrival: i64,
    expected_platform: Option<String>,
    transfer_seconds: Option<i64>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JourneySpec {
    journey_id: String,
    scheduled_end: i64,
    legs: Vec<Leg>,
}
#[derive(Deserialize)]
struct SyncBody { journeys: Vec<JourneySpec> }
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenBody { fcm_token: String }
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Registration { device_id: String, device_secret: String }

struct State {
    db: PgPool,
    firebase: Arc<firebase::Firebase>,
    config: catenary::catenaryconfig::JourneyNotifierConfig,
    aspen: catenary::catenaryconfig::AspenConfig,
}
fn unix() -> i64 { chrono::Utc::now().timestamp() }
fn hash_secret(s: &str) -> String { hex::encode(Sha256::digest(s.as_bytes())) }

async fn device_from_request(req: &HttpRequest, db: &PgPool) -> Result<String, HttpResponse> {
    let device = req.headers().get("X-Device-Id").and_then(|h| h.to_str().ok())
        .filter(|id| Uuid::parse_str(id).is_ok())
        .ok_or_else(|| HttpResponse::Unauthorized().finish())?;
    let secret = req.headers().get("Authorization").and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|secret| secret.len() >= 40)
        .ok_or_else(|| HttpResponse::Unauthorized().finish())?;
    let expected: Option<String> = sqlx::query_scalar("SELECT secret_hash FROM journey_notifier_devices WHERE id=$1")
        .bind(device).fetch_optional(db).await.map_err(|_| HttpResponse::InternalServerError().finish())?;
    let Some(expected) = expected else { return Err(HttpResponse::Unauthorized().finish()); };
    let supplied = hash_secret(secret);
    // Constant-time comparison of the fixed-size SHA256 hexadecimal digests.
    let mut mismatch = 0u8;
    for (a, b) in expected.as_bytes().iter().zip(supplied.as_bytes()) { mismatch |= a ^ b; }
    if expected.len() != supplied.len() || mismatch != 0 {
        return Err(HttpResponse::Unauthorized().finish());
    }
    Ok(device.to_owned())
}

async fn register(body: web::Json<TokenBody>, state: web::Data<State>) -> impl Responder {
    if body.fcm_token.is_empty() || body.fcm_token.len() > 4096 {
        return HttpResponse::BadRequest().finish();
    }
    let id = Uuid::new_v4().to_string();
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let result = sqlx::query("INSERT INTO journey_notifier_devices (id,secret_hash,fcm_token,created_at,updated_at) VALUES ($1,$2,$3,$4,$4)")
        .bind(&id).bind(hash_secret(&secret)).bind(&body.fcm_token).bind(unix())
        .execute(&state.db).await;
    match result {
        Ok(_) => HttpResponse::Created().json(Registration { device_id: id, device_secret: secret }),
        Err(error) => { eprintln!("journey-notifier device registration: {error}"); HttpResponse::InternalServerError().finish() }
    }
}
async fn refresh_token(req: HttpRequest, path: web::Path<String>, body: web::Json<TokenBody>, state: web::Data<State>) -> impl Responder {
    let device = match device_from_request(&req, &state.db).await { Ok(id) => id, Err(r) => return r };
    if path.as_str() != device || body.fcm_token.is_empty() || body.fcm_token.len() > 4096 {
        return HttpResponse::BadRequest().finish();
    }
    match sqlx::query("UPDATE journey_notifier_devices SET fcm_token=$1,updated_at=$2 WHERE id=$3")
        .bind(&body.fcm_token).bind(unix()).bind(&device).execute(&state.db).await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(error) => { eprintln!("journey-notifier token update: {error}"); HttpResponse::InternalServerError().finish() }
    }
}
async fn sync_journeys(req: HttpRequest, body: web::Json<SyncBody>, state: web::Data<State>) -> impl Responder {
    let device = match device_from_request(&req, &state.db).await { Ok(id) => id, Err(r) => return r };
    if body.journeys.len() > 250 { return HttpResponse::PayloadTooLarge().finish(); }
    let now = unix();
    let mut ids = Vec::with_capacity(body.journeys.len());
    for journey in &body.journeys {
        if journey.journey_id.is_empty() || journey.journey_id.len() > 256 || journey.legs.is_empty() || journey.legs.len() > 20
            || journey.scheduled_end < now - 30 * 86400 || journey.scheduled_end > now + 30 * 86400
            || ids.contains(&journey.journey_id)
            || journey.legs.iter().any(|leg| leg.chateau.is_empty() || leg.chateau.len() > 256
                || leg.trip_id.is_empty() || leg.trip_id.len() > 512
                || leg.scheduled_departure > leg.scheduled_arrival
                || leg.scheduled_arrival > journey.scheduled_end + 86400
                || leg.from_name.len() > 256 || leg.route_name.len() > 128)
        { return HttpResponse::BadRequest().finish(); }
        ids.push(journey.journey_id.clone());
    }
    let mut tx = match state.db.begin().await { Ok(t) => t, Err(_) => return HttpResponse::InternalServerError().finish() };
    for journey in &body.journeys {
        // A completed subscription must not be resurrected by periodic full-sync.
        let tombstone: Option<i64> = match sqlx::query_scalar(
            "SELECT expires_at FROM journey_notifier_expired WHERE device_id=$1 AND journey_id=$2 AND expires_at > $3"
        ).bind(&device).bind(&journey.journey_id).bind(now).fetch_optional(&mut *tx).await {
            Ok(row) => row, Err(_) => return HttpResponse::InternalServerError().finish()
        };
        if tombstone.is_some() { continue; }
        let new_legs = match serde_json::to_value(&journey.legs) { Ok(v) => v, Err(_) => return HttpResponse::BadRequest().finish() };
        let existing: Option<String> = match sqlx::query_scalar("SELECT legs::text FROM journey_notifier_journeys WHERE device_id=$1 AND journey_id=$2 FOR UPDATE")
            .bind(&device).bind(&journey.journey_id).fetch_optional(&mut *tx).await {
            Ok(row) => row, Err(_) => return HttpResponse::InternalServerError().finish()
        };
        let changed = existing.as_ref().is_some_and(|raw| serde_json::from_str::<serde_json::Value>(raw).ok().as_ref() != Some(&new_legs));
        let json = new_legs.to_string();
        // Without realtime coverage keep a conservative 72h grace for extreme delays.
        let conservative_expiry = (journey.scheduled_end + 72 * 3600).min(now + state.config.max_monitor_days.unwrap_or(30).max(1) * 86400);
        let insert = sqlx::query(
            "INSERT INTO journey_notifier_journeys(device_id,journey_id,legs,scheduled_end,expires_at,created_at,updated_at) VALUES($1,$2,$3::jsonb,$4,$5,$6,$6)
             ON CONFLICT(device_id,journey_id) DO UPDATE SET legs=EXCLUDED.legs,scheduled_end=EXCLUDED.scheduled_end,updated_at=EXCLUDED.updated_at")
            .bind(&device).bind(&journey.journey_id).bind(json).bind(journey.scheduled_end)
            .bind(conservative_expiry).bind(now).execute(&mut *tx).await;
        if insert.is_err() { return HttpResponse::InternalServerError().finish(); }
        if changed {
            if sqlx::query("DELETE FROM journey_notifier_leg_states WHERE device_id=$1 AND journey_id=$2")
                .bind(&device).bind(&journey.journey_id).execute(&mut *tx).await.is_err() { return HttpResponse::InternalServerError().finish(); }
        }
        if existing.is_none() || changed {
            for (i, leg) in journey.legs.iter().enumerate() {
                let baseline = serde_json::json!({
                    "departure": leg.expected_departure, "arrival": leg.expected_arrival,
                    "platform": leg.expected_platform, "cancelled": false
                }).to_string();
                if sqlx::query("INSERT INTO journey_notifier_leg_states(device_id,journey_id,leg_index,state) VALUES($1,$2,$3,$4::jsonb)")
                    .bind(&device).bind(&journey.journey_id).bind(i as i32).bind(baseline)
                    .execute(&mut *tx).await.is_err() { return HttpResponse::InternalServerError().finish(); }
            }
        }
    }
    if sqlx::query("DELETE FROM journey_notifier_journeys WHERE device_id=$1 AND NOT (journey_id=ANY($2))")
        .bind(&device).bind(&ids).execute(&mut *tx).await.is_err() { return HttpResponse::InternalServerError().finish(); }
    match tx.commit().await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(error) => { eprintln!("journey-notifier sync commit: {error}"); HttpResponse::InternalServerError().finish() }
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let config: CatenaryConfig = toml::from_str(&text).context("parsing catenaryconfig.toml")?;
    let notifier = config.journey_notifier;
    let project = notifier.firebase.project_id.as_deref().filter(|s| !s.is_empty())
        .context("configure [journey_notifier.firebase].project_id")?;
    let credentials = notifier.firebase.service_account_path.as_deref().filter(|s| !s.is_empty())
        .context("configure [journey_notifier.firebase].service_account_path")?;
    let firebase = Arc::new(firebase::Firebase::new(project.to_owned(), credentials)?);
    let db_url = args.database_url.or(config.postgres.database_url)
        .or_else(|| std::env::var("DATABASE_URL").ok()).context("DATABASE_URL not configured")?;
    let db = PgPoolOptions::new().max_connections(12).connect(&db_url).await?;
    sqlx::query("SELECT 1 FROM journey_notifier_devices LIMIT 1").execute(&db).await
        .context("journey-notifier migration missing; run diesel migration run")?;
    let state = web::Data::new(State { db, firebase, config: notifier, aspen: config.aspen });
    let worker_state = state.clone();
    actix_web::rt::spawn(async move { monitor::run(worker_state).await; });
    let listen = state.config.listen.as_deref().unwrap_or("127.0.0.1:40521").to_owned();
    eprintln!("journey-notifier listening on {listen}; ensure TLS reverse proxy and rate limiting");
    HttpServer::new(move || App::new().app_data(state.clone())
        .app_data(web::JsonConfig::default().limit(128 * 1024))
        .route("/v1/devices", web::post().to(register))
        .route("/v1/devices/{id}", web::put().to(refresh_token))
        .route("/v1/journeys/sync", web::post().to(sync_journeys)))
        .bind(&listen)?.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn leg_payload_round_trip() {
        let leg = Leg {
            chateau: "c".into(), trip_id: "t".into(), trip_start_date: "20261121".into(), trip_start_time: None,
            from_stop_sequence: Some(1), to_stop_sequence: Some(2), from_stop_id: None, to_stop_id: None,
            from_name: "A".into(), to_name: "B".into(), route_name: "IC".into(), trip_short_name: None,
            time_zone: Some("Europe/Zurich".into()), scheduled_departure: 1, scheduled_arrival: 2,
            expected_departure: 1, expected_arrival: 2, expected_platform: Some("3".into()), transfer_seconds: None
        };
        let encoded = serde_json::to_string(&leg).unwrap();
        assert_eq!(serde_json::from_str::<Leg>(&encoded).unwrap(), leg);
    }
}
