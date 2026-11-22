//! Poll only the Aspen chateaux/trip IDs referenced by active subscriptions.
//! A missing realtime update is *unknown*, never proof of an on-time run.
use super::{Leg, State, unix};
use catenary::{
    EtcdConnectionIps,
    aspen::lib::{ChateauMetadataEtcd, spawn_aspen_client_from_ip},
    aspen_dataset::{AspenisedTripScheduleRelationship, AspenisedStopTimeScheduleRelationship, AspenisedTripUpdate},
    etcd_cache::EtcdCache,
};
use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{Value, json};
use sqlx::{Row, PgPool};
use std::{collections::{BTreeMap, BTreeSet, HashMap}, sync::Arc, time::Duration};
use tarpc::context;
use actix_web::web;

#[derive(Clone)]
struct Subscription {
    device_id: String,
    journey_id: String,
    legs: Vec<Leg>,
    created_at: i64,
}
#[derive(Clone, Debug)]
struct StateLeg {
    departure: i64,
    arrival: i64,
    platform: Option<String>,
    cancelled: bool,
}
impl StateLeg {
    fn from_input(l: &Leg) -> Self {
        Self { departure: l.expected_departure, arrival: l.expected_arrival,
            platform: l.expected_platform.clone(), cancelled: false }
    }
    fn decode(value: &Value, fallback: &Leg) -> Self {
        let mut s = Self::from_input(fallback);
        s.departure = value.get("departure").and_then(Value::as_i64).unwrap_or(s.departure);
        s.arrival = value.get("arrival").and_then(Value::as_i64).unwrap_or(s.arrival);
        s.platform = value.get("platform").and_then(Value::as_str).map(ToOwned::to_owned);
        s.cancelled = value.get("cancelled").and_then(Value::as_bool).unwrap_or(false);
        s
    }
    fn json(&self) -> String {
        json!({"departure":self.departure,"arrival":self.arrival,"platform":self.platform,"cancelled":self.cancelled}).to_string()
    }
}

fn evaluate(leg: &Leg, update: &AspenisedTripUpdate) -> StateLeg {
    let mut result = StateLeg::from_input(leg);
    result.cancelled = matches!(
        update.trip.schedule_relationship.as_ref(),
        Some(AspenisedTripScheduleRelationship::Cancelled | AspenisedTripScheduleRelationship::Deleted)
    );
    let departure = update.stop_time_update.iter().find(|st| {
        leg.from_stop_sequence.is_some_and(|seq| st.stop_sequence == Some(seq))
            || (leg.from_stop_sequence.is_none() && leg.from_stop_id.as_deref() == st.stop_id.as_deref())
    });
    let arrival = update.stop_time_update.iter().find(|st| {
        leg.to_stop_sequence.is_some_and(|seq| st.stop_sequence == Some(seq))
            || (leg.to_stop_sequence.is_none() && leg.to_stop_id.as_deref() == st.stop_id.as_deref())
    });
    if let Some(st) = departure {
        if st.schedule_relationship == Some(AspenisedStopTimeScheduleRelationship::Skipped) {
            result.cancelled = true;
        }
        if let Some(platform) = st.platform_string.as_ref().map(|p| p.as_str()).filter(|p| !p.trim().is_empty()) {
            result.platform = Some(platform.to_owned());
        }
        if let Some(event) = &st.departure {
            result.departure = event.time
                .or_else(|| event.delay.map(|delay| leg.scheduled_departure.saturating_add(i64::from(delay))))
                .unwrap_or(result.departure);
        }
    } else if let Some(delay) = update.delay {
        result.departure = leg.scheduled_departure.saturating_add(i64::from(delay));
    }
    if let Some(st) = arrival {
        if st.schedule_relationship == Some(AspenisedStopTimeScheduleRelationship::Skipped) {
            result.cancelled = true;
        }
        if let Some(event) = &st.arrival {
            result.arrival = event.time
                .or_else(|| event.delay.map(|delay| leg.scheduled_arrival.saturating_add(i64::from(delay))))
                .unwrap_or(result.arrival);
        }
    } else if let Some(delay) = update.delay {
        result.arrival = leg.scheduled_arrival.saturating_add(i64::from(delay));
    }
    result
}
fn matches_instance(leg: &Leg, update: &AspenisedTripUpdate) -> bool {
    if update.trip.trip_id.as_deref() != Some(&leg.trip_id) { return false; }
    let expected_day = leg.trip_start_date.replace('-', "");
    let actual_day = update.trip.effective_start_date()
        .map(|d| d.format("%Y%m%d").to_string());
    // Never join two occurrences of the same trip_id on different days.
    if actual_day.as_deref() != Some(expected_day.as_str()) { return false; }
    if let (Some(expected), Some(actual)) = (leg.trip_start_time.as_deref(), update.trip.effective_start_time()) {
        if expected != actual { return false; }
    }
    true
}
fn local_time(unix_seconds: i64, timezone: Option<&str>) -> String {
    let utc = chrono::DateTime::<chrono::Utc>::from_timestamp(unix_seconds, 0)
        .unwrap_or_else(chrono::Utc::now);
    if let Some(tz) = timezone.and_then(|name| name.parse::<chrono_tz::Tz>().ok()) {
        utc.with_timezone(&tz).format("%H:%M").to_string()
    } else { utc.format("%H:%M UTC").to_string() }
}
fn route_name(leg: &Leg) -> String {
    match leg.trip_short_name.as_deref() {
        Some(short) if !short.is_empty() && short != leg.route_name =>
            format!("{} {}", leg.route_name, short),
        _ => leg.route_name.clone(),
    }
}
async fn queue(db: &PgPool, sub: &Subscription, fingerprint: &str, kind: &str, title: &str, body: &str) -> Result<()> {
    let now = unix();
    sqlx::query("INSERT INTO journey_notifier_events
        (id,device_id,journey_id,fingerprint,kind,title,body,next_attempt,created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
        ON CONFLICT (device_id,journey_id,fingerprint) DO NOTHING")
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&sub.device_id).bind(&sub.journey_id).bind(fingerprint).bind(kind)
        .bind(title).bind(body).bind(now).execute(db).await?;
    Ok(())
}
async fn process_subscription(db: &PgPool, sub: &Subscription, updates: &HashMap<(String,String), Vec<AspenisedTripUpdate>>, retention: i64, max_days: i64) -> Result<()> {
    let old_rows = sqlx::query(
        "SELECT leg_index,state::text AS state FROM journey_notifier_leg_states WHERE device_id=$1 AND journey_id=$2")
        .bind(&sub.device_id).bind(&sub.journey_id).fetch_all(db).await?;
    let mut prior = sub.legs.iter().map(StateLeg::from_input).collect::<Vec<_>>();
    for row in old_rows {
        let index: i32 = row.get("leg_index");
        let raw: String = row.get("state");
        if let Some((leg, state)) = sub.legs.get(index as usize).zip(prior.get_mut(index as usize)) {
            if let Ok(val) = serde_json::from_str::<Value>(&raw) { *state = StateLeg::decode(&val, leg); }
        }
    }
    let mut next = prior.clone();
    let mut confirmed = vec![false; sub.legs.len()];
    for (index, leg) in sub.legs.iter().enumerate() {
        let key = (leg.chateau.clone(), leg.trip_id.clone());
        let candidates = updates.get(&key).into_iter().flat_map(|v| v.iter())
            .filter(|rt| matches_instance(leg, rt)).collect::<Vec<_>>();
        // Ambiguous identities must never generate a false alert.
        if candidates.len() != 1 { continue; }
        let new_state = evaluate(leg, candidates[0]);
        confirmed[index] = true;
        let old = &prior[index];
        if !old.cancelled && new_state.cancelled {
            queue(db, sub, &format!("{index}:cancelled"), "cancellation", "Journey cancelled",
                &format!("{} during your journey has been cancelled. Tap for more information.", route_name(leg))).await?;
        } else if !new_state.cancelled {
            if let Some(platform) = new_state.platform.as_deref() {
                if old.platform.as_deref().is_some_and(|p| p != platform) {
                    queue(db, sub, &format!("{index}:platform:{platform}"), "platform_change", "Platform change",
                        &format!("Change of platform in {}. Departure at {} now from platform {}.",
                            leg.from_name, local_time(new_state.departure, leg.time_zone.as_deref()), platform)).await?;
                }
            }
            if new_state.departure != old.departure && (new_state.departure - old.departure).abs() >= 60 {
                queue(db, sub, &format!("{index}:departure:{}", new_state.departure), "departure_change", "Departure time changed",
                    &format!("{} at {} now departs at {}.", route_name(leg), leg.from_name,
                        local_time(new_state.departure, leg.time_zone.as_deref()))).await?;
            }
        }
        next[index] = new_state;
    }
    for index in 1..sub.legs.len() {
        if !confirmed[index - 1] && !confirmed[index] { continue; }
        let min_transfer = sub.legs[index].transfer_seconds.unwrap_or(120).clamp(0, 3600);
        let previously_possible = prior[index - 1].arrival.saturating_add(min_transfer) <= prior[index].departure
            && !prior[index - 1].cancelled && !prior[index].cancelled;
        let currently_possible = next[index - 1].arrival.saturating_add(min_transfer) <= next[index].departure
            && !next[index - 1].cancelled && !next[index].cancelled;
        // Cancellation has its own alert; do not describe it as a delay.
        if previously_possible && !currently_possible
            && !next[index - 1].cancelled && !next[index].cancelled {
            let leg = &sub.legs[index];
            queue(db, sub,
                &format!("transfer:{index}:{}:{}", next[index-1].arrival, next[index].departure),
                "connection_missed", "Connection no longer possible",
                &format!("Connection to {} no longer possible due to delay. Please find an alternate connection.", route_name(leg))
            ).await?;
        }
    }
    for (index, state) in next.iter().enumerate() {
        if !confirmed[index] || state.departure == prior[index].departure
            && state.arrival == prior[index].arrival
            && state.platform == prior[index].platform
            && state.cancelled == prior[index].cancelled { continue; }
        sqlx::query("UPDATE journey_notifier_leg_states SET state=$1::jsonb WHERE device_id=$2 AND journey_id=$3 AND leg_index=$4")
            .bind(state.json()).bind(&sub.device_id).bind(&sub.journey_id).bind(index as i32)
            .execute(db).await?;
    }
    // Once a last-leg prediction is available, retain until predicted arrival + grace.
    // Missing realtime data leaves the original conservative 72-hour fallback.
    if confirmed.last() == Some(&true) {
        let latest = next.last().map(|s| s.arrival).unwrap_or_default();
        let target = latest.max(unix()).saturating_add(retention * 3600);
        let cap = sub.created_at.saturating_add(max_days * 86400);
        sqlx::query("UPDATE journey_notifier_journeys SET expires_at=$1 WHERE device_id=$2 AND journey_id=$3")
            .bind(target.min(cap)).bind(&sub.device_id).bind(&sub.journey_id)
            .execute(db).await?;
    }
    Ok(())
}

async fn sweep(state: &State, assignments: &EtcdCache<ChateauMetadataEtcd>) -> Result<()> {
    let now = unix();
    // Keep a 30-day tombstone so WorkManager's full-device reconciliation
    // cannot reactivate journeys which the backend has already completed.
    let mut tx = state.db.begin().await?;
    sqlx::query("INSERT INTO journey_notifier_expired(device_id,journey_id,expires_at)
        SELECT device_id,journey_id,$2 FROM journey_notifier_journeys WHERE expires_at <= $1
        ON CONFLICT(device_id,journey_id) DO UPDATE SET expires_at=GREATEST(journey_notifier_expired.expires_at, EXCLUDED.expires_at)")
        .bind(now).bind(now + 30 * 86400).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM journey_notifier_journeys WHERE expires_at <= $1")
        .bind(now).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM journey_notifier_expired WHERE expires_at <= $1")
        .bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    sqlx::query("DELETE FROM journey_notifier_devices WHERE updated_at < $1 AND NOT EXISTS (SELECT 1 FROM journey_notifier_journeys j WHERE j.device_id=journey_notifier_devices.id)")
        .bind(now - 30 * 86400).execute(&state.db).await?;
    let rows = sqlx::query("SELECT device_id,journey_id,legs::text AS legs,created_at FROM journey_notifier_journeys WHERE expires_at > $1")
        .bind(now).fetch_all(&state.db).await?;
    let mut subscriptions = Vec::new();
    let mut lookup = BTreeMap::<String, BTreeSet<String>>::new();
    for row in rows {
        let device_id: String = row.get("device_id");
        let journey_id: String = row.get("journey_id");
        let raw: String = row.get("legs");
        let legs: Vec<Leg> = serde_json::from_str(&raw)?;
        for leg in &legs { lookup.entry(leg.chateau.clone()).or_default().insert(leg.trip_id.clone()); }
        subscriptions.push(Subscription { device_id, journey_id, legs, created_at: row.get("created_at") });
    }
    let maps = stream::iter(lookup.into_iter()).map(|(chateau, trips)| {
        let socket = assignments.get(&chateau).map(|info| info.socket);
        async move {
            let mut selected = HashMap::<(String,String),Vec<AspenisedTripUpdate>>::new();
            let Some(socket) = socket else { return selected; };
            let Ok(client) = spawn_aspen_client_from_ip(&socket).await else { return selected; };
            let ids = trips.into_iter().collect::<Vec<_>>();
            // Keep bounded RPC batches; never download all Aspen realtime runs.
            for batch in ids.chunks(128) {
                let result = tokio::time::timeout(Duration::from_secs(20),
                    client.get_all_trips_with_ids(context::current(), chateau.clone(), batch.to_vec())).await;
                if let Ok(Ok(Some(response))) = result {
                    for (_, update) in response.trip_updates {
                        if let Some(id) = update.trip.trip_id.clone() {
                            selected.entry((chateau.clone(), id)).or_default().push(update);
                        }
                    }
                }
            }
            selected
        }
    }).buffer_unordered(4).collect::<Vec<_>>().await;
    let mut updates = HashMap::<(String,String),Vec<AspenisedTripUpdate>>::new();
    for map in maps { for (key, mut value) in map { updates.entry(key).or_default().append(&mut value); } }
    let retention = state.config.retention_hours.unwrap_or(6).clamp(1, 72);
    let max_days = state.config.max_monitor_days.unwrap_or(30).clamp(1, 90);
    for sub in &subscriptions {
        if let Err(error) = process_subscription(&state.db, sub, &updates, retention, max_days).await {
            eprintln!("journey-notifier evaluation {}: {error}", sub.journey_id);
        }
    }
    Ok(())
}
async fn deliver(state: &State) -> Result<()> {
    let now = unix();
    let mut tx = state.db.begin().await?;
    // Claim using SKIP LOCKED and a recovery lease: another worker cannot dispatch
    // the same event concurrently; an interrupted attempt becomes eligible again.
    let rows = sqlx::query("SELECT e.id,e.device_id,e.journey_id,e.kind,e.title,e.body,d.fcm_token
        FROM journey_notifier_events e JOIN journey_notifier_devices d ON d.id=e.device_id
        WHERE e.sent_at IS NULL AND e.next_attempt <= $1 AND e.attempts < 20
        ORDER BY e.created_at LIMIT 32 FOR UPDATE OF e SKIP LOCKED")
        .bind(now).fetch_all(&mut *tx).await?;
    let mut pending = Vec::new();
    for row in rows {
        let id: String = row.get("id");
        let device: String = row.get("device_id");
        let journey: String = row.get("journey_id");
        let kind: String = row.get("kind");
        let title: String = row.get("title");
        let body: String = row.get("body");
        let token: String = row.get("fcm_token");
        sqlx::query("UPDATE journey_notifier_events SET attempts=attempts+1,next_attempt=$1 WHERE id=$2")
            .bind(now + 120).bind(&id).execute(&mut *tx).await?;
        pending.push((id,device,journey,kind,title,body,token));
    }
    tx.commit().await?;
    for (id, _device, journey, kind, title, body, token) in pending {
        match state.firebase.send(&token, &id, &journey, &kind, &title, &body).await {
            Ok(()) => { sqlx::query("UPDATE journey_notifier_events SET sent_at=$1 WHERE id=$2")
                .bind(unix()).bind(&id).execute(&state.db).await?; }
            Err(error) => {
                eprintln!("journey-notifier FCM delivery {id}: {error}");
                // Back off, capped at 1h. A 20-attempt ceiling prevents infinite poison retries.
                sqlx::query("UPDATE journey_notifier_events SET next_attempt=$1 WHERE id=$2")
                    .bind(unix() + 600).bind(&id).execute(&state.db).await?;
            }
        }
    }
    Ok(())
}
pub async fn run(state: web::Data<State>) {
    let urls = state.aspen.etcd_urls.clone().unwrap_or_else(|| vec!["127.0.0.1:2379".into()]);
    let options = match (state.aspen.etcd_username.clone(), state.aspen.etcd_password.clone()) {
        (Some(username), Some(password)) if !username.is_empty() =>
            Some(etcd_client::ConnectOptions::new().with_user(username,password)
                .with_keep_alive(Duration::from_secs(1), Duration::from_secs(5))),
        _ => None,
    };
    let assignments = match EtcdCache::<ChateauMetadataEtcd>::new(
        Arc::new(EtcdConnectionIps { ip_addresses: urls }),
        Arc::new(options), "/aspen_assigned_chateaux/").await {
        Ok(a) => a,
        Err(error) => { eprintln!("journey-notifier cannot initialize Aspen discovery: {error}"); return; }
    };
    let interval = state.config.poll_interval_seconds.unwrap_or(45).clamp(15, 3600);
    loop {
        if let Err(error) = sweep(&state, &assignments).await { eprintln!("journey-notifier Aspen sweep: {error:#}"); }
        if let Err(error) = deliver(&state).await { eprintln!("journey-notifier outbox: {error:#}"); }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timezone_and_fingerprint() {
        assert_eq!(local_time(0, Some("Europe/Zurich")), "01:00");
        assert_eq!(route_name(&Leg {
            chateau:"a".into(),trip_id:"b".into(),trip_start_date:"20261121".into(),
            trip_start_time:None,from_stop_sequence:None,to_stop_sequence:None,
            from_stop_id:None,to_stop_id:None,from_name:"X".into(),to_name:"Y".into(),
            route_name:"IC".into(),trip_short_name:Some("8".into()),time_zone:None,
            scheduled_departure:0,scheduled_arrival:1,expected_departure:0,expected_arrival:1,
            expected_platform:None,transfer_seconds:None
        }), "IC 8");
    }
}
