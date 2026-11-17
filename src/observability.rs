//! Opt-in, best-effort Datadog DogStatsD metrics for HTTP and websocket services.
//! DATADOG_ENABLED=1 enables UDP export to DD_AGENT_HOST:DD_DOGSTATSD_PORT.
//! Never include request IDs, coordinates, IPs, or user-controlled URLs in metric tags.
use std::net::UdpSocket;
use std::sync::OnceLock;
use std::time::Instant;
use actix_web::{body::{BoxBody, MessageBody}, dev::{ServiceRequest, ServiceResponse}, middleware::Next, Error};
static METRICS: OnceLock<Option<Exporter>> = OnceLock::new();
struct Exporter { socket: UdpSocket, address: String, service: &'static str, env: String }
pub fn init(service: &'static str) {
    METRICS.get_or_init(|| {
        if !matches!(std::env::var("DATADOG_ENABLED").as_deref(), Ok("1" | "true" | "yes")) {
            return None;
        }
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(error) => { eprintln!("DogStatsD disabled: {error}"); return None; }
        };
        if socket.set_nonblocking(true).is_err() { return None; }
        let host = std::env::var("DD_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("DD_DOGSTATSD_PORT").unwrap_or_else(|_| "8125".into());
        Some(Exporter {
            socket, address: format!("{host}:{port}"), service,
            env: std::env::var("DD_ENV").unwrap_or_else(|_| "production".into()),
        })
    });
}
pub fn enabled() -> bool { METRICS.get().is_some_and(Option::is_some) }
fn safe_tag(value: &str) -> String {
    value.chars().take(100).map(|c|
        if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/') { c } else { '_' }
    ).collect()
}
fn emit(name: &str, value: String, kind: &str, tags: &[(&str, &str)]) {
    let Some(Some(exporter)) = METRICS.get() else { return };
    let mut message = format!("catenary.{name}:{value}|{kind}|#service:{},env:{}",
        exporter.service, safe_tag(&exporter.env));
    for (key, value) in tags {
        message.push(',');
        message.push_str(key);
        message.push(':');
        message.push_str(&safe_tag(value));
    }
    let _ = exporter.socket.send_to(message.as_bytes(), &exporter.address);
}
pub fn count(name: &str, tags: &[(&str, &str)]) { emit(name, "1".into(), "c", tags); }
pub fn distribution(name: &str, ms: f64, tags: &[(&str, &str)]) {
    if ms.is_finite() && ms >= 0.0 { emit(name, ms.to_string(), "d", tags); }
}
pub fn country(header: Option<&str>) -> &str {
    match header {
        Some(s) if s.len() == 2 && s.as_bytes().iter().all(u8::is_ascii_uppercase) => s,
        _ => "unknown",
    }
}
pub async fn http_metrics(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    if !enabled() { return next.call(req).await.map(ServiceResponse::map_into_boxed_body); }
    let started = Instant::now();
    let country = country(req.headers().get("CF-IPCountry").and_then(|h| h.to_str().ok())).to_owned();
    let result = next.call(req).await;
    if let Ok(response) = &result {
        let endpoint = response.request().match_pattern().unwrap_or_else(|| "unmatched".into());
        let status = response.status().as_u16().to_string();
        let tags = [("endpoint", endpoint.as_str()), ("country", country.as_str()), ("status", status.as_str())];
        count("http.requests", &tags);
        distribution("http.duration_ms", started.elapsed().as_secs_f64()*1000.0, &tags);
        if endpoint.contains("nearbydepartures") { count("nearby.requests", &[("country", &country)]); }
        if endpoint.contains("directory") { count("directory.requests", &tags); }
        if endpoint.contains("stop") || endpoint.contains("station") { count("stop.requests", &tags); }
        if (endpoint.contains("tile") || endpoint.contains("tiles")) && response.status().is_success() {
            count("tiles.sent", &[("endpoint", &endpoint)]);
        }
        if METRICS.get().and_then(Option::as_ref).is_some_and(|e| e.service == "spruce")
            && (endpoint == "/getinitdata" || endpoint == "/ws/" || endpoint == "/ws/live" || endpoint == "/ws/trip") {
            count("spruce.initial_requests", &[("endpoint", &endpoint), ("country", &country)]);
        }
    }
    result.map(ServiceResponse::map_into_boxed_body)
}
/// Distinct GTFS chateaus in a single returned nearby response.
pub fn nearby_chateaus(data: &crate::nearby_departures::NearbyDeparturesV3Response) -> std::collections::HashSet<String> {
    let mut result = std::collections::HashSet::new();
    result.extend(data.routes.keys().cloned());
    result.extend(data.stops.keys().cloned());
    result.extend(data.local.iter().map(|route| route.chateau_id.clone()));
    result.extend(data.long_distance.iter().flat_map(|station| station.departures.iter().map(|d| d.chateau_id.clone())));
    result
}
pub fn record_chateaus(chateaus: impl IntoIterator<Item=String>) {
    for chateau in chateaus {
        count("nearby.chateau_accesses", &[("chateau", &chateau)]);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn cloudflare_country_validation() {
        assert_eq!(country(Some("US")), "US");
        assert_eq!(country(Some("us")), "unknown");
        assert_eq!(country(Some("USA")), "unknown");
        assert_eq!(country(None), "unknown");
    }
    #[test] fn tags_cannot_inject_dogstatsd_fields() {
        assert_eq!(safe_tag("x|#evil:yes"), "x__evil_yes");
    }
}
