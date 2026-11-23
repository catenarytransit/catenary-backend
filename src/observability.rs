//! Opt-in, best-effort Datadog DogStatsD metrics for HTTP and websocket services.
//! DATADOG_ENABLED=1 enables UDP export to DD_AGENT_HOST:DD_DOGSTATSD_PORT.
//! Never include request IDs, coordinates, IPs, or user-controlled URLs in metric tags.
use actix_web::{
    Error,
    body::{BoxBody, MessageBody},
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
};
use std::net::UdpSocket;
use std::sync::OnceLock;
use std::time::Instant;
static METRICS: OnceLock<Option<Exporter>> = OnceLock::new();
struct Exporter {
    socket: UdpSocket,
    address: String,
    service: &'static str,
    env: String,
}
pub fn init(service: &'static str) {
    METRICS.get_or_init(|| {
        if !matches!(
            std::env::var("DATADOG_ENABLED").as_deref(),
            Ok("1" | "true" | "yes")
        ) {
            return None;
        }
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(error) => {
                eprintln!("DogStatsD disabled: {error}");
                return None;
            }
        };
        if socket.set_nonblocking(true).is_err() {
            return None;
        }
        let host = std::env::var("DD_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("DD_DOGSTATSD_PORT").unwrap_or_else(|_| "8125".into());
        Some(Exporter {
            socket,
            address: format!("{host}:{port}"),
            service,
            env: std::env::var("DD_ENV").unwrap_or_else(|_| "production".into()),
        })
    });
}
pub fn enabled() -> bool {
    METRICS.get().is_some_and(Option::is_some)
}
fn safe_tag(value: &str) -> String {
    value
        .chars()
        .take(100)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
fn emit(name: &str, value: String, kind: &str, tags: &[(&str, &str)]) {
    let Some(Some(exporter)) = METRICS.get() else {
        return;
    };
    let mut message = format!(
        "catenary.{name}:{value}|{kind}|#service:{},env:{}",
        exporter.service,
        safe_tag(&exporter.env)
    );
    for (key, value) in tags {
        message.push(',');
        message.push_str(key);
        message.push(':');
        message.push_str(&safe_tag(value));
    }
    if matches!(
        std::env::var("DATADOG_DEBUG_METRICS").as_deref(),
        Ok("1" | "true")
    ) && name.starts_with("spruce.")
    {
        eprintln!("DogStatsD -> {}: {}", exporter.address, message);
    }
    if let Err(error) = exporter
        .socket
        .send_to(message.as_bytes(), &exporter.address)
    {
        if matches!(
            std::env::var("DATADOG_DEBUG_METRICS").as_deref(),
            Ok("1" | "true")
        ) {
            eprintln!("DogStatsD send failed: {error}");
        }
    }
}
pub fn count(name: &str, tags: &[(&str, &str)]) {
    emit(name, "1".into(), "c", tags);
}
pub fn distribution(name: &str, ms: f64, tags: &[(&str, &str)]) {
    if ms.is_finite() && ms >= 0.0 {
        emit(name, ms.to_string(), "d", tags);
    }
}
pub fn country(header: Option<&str>) -> &str {
    match header {
        Some(s) if s.len() == 2 && s.as_bytes().iter().all(u8::is_ascii_uppercase) => s,
        _ => "unknown",
    }
}

/// Client country from a trusted Cloudflare/gateway header. Unknown stays a
/// single bounded tag value rather than creating arbitrary time-series.
pub fn country_from_headers(headers: &actix_web::http::header::HeaderMap) -> &str {
    country(
        headers
            .get("CF-IPCountry")
            .and_then(|value| value.to_str().ok()),
    )
}
fn is_datadog_synthetics_user_agent(user_agent: Option<&str>) -> bool {
    user_agent.is_some_and(|agent| agent.to_ascii_lowercase().contains("datadog/synthetics"))
}

/// Identify Datadog Synthetics traffic before emitting request-scoped metrics.
/// Datadog API tests may send proprietary headers even when their user agent varies.
pub fn is_synthetics_request(headers: &actix_web::http::header::HeaderMap) -> bool {
    let contains_synthetics = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().contains("synthetics"))
    };
    contains_synthetics("user-agent")
        || contains_synthetics("sec-datadog")
        || contains_synthetics("x-datadog-origin")
}

pub async fn http_metrics(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    if !enabled()
        || is_datadog_synthetics_user_agent(
            req.headers()
                .get(actix_web::http::header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
        )
    {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_boxed_body);
    }
    let started = Instant::now();
    let country = country_from_headers(req.headers()).to_owned();
    let result = next.call(req).await;
    if let Ok(response) = &result {
        let endpoint = response
            .request()
            .match_pattern()
            .unwrap_or_else(|| "unmatched".into());
        let status = response.status().as_u16().to_string();
        let tags = [
            ("endpoint", endpoint.as_str()),
            ("country", country.as_str()),
            ("status", status.as_str()),
        ];
        count("http.requests", &tags);
        distribution(
            "http.duration_ms",
            started.elapsed().as_secs_f64() * 1000.0,
            &tags,
        );
        if endpoint.contains("nearbydepartures") {
            count("nearby.requests", &[("country", &country)]);
        }
        if endpoint.contains("directory") {
            count("directory.requests", &tags);
        }
        if endpoint.contains("stop") || endpoint.contains("station") {
            count("stop.requests", &tags);
        }
        if (endpoint.contains("tile") || endpoint.contains("tiles"))
            && response.status().is_success()
        {
            count(
                "tiles.sent",
                &[("endpoint", &endpoint), ("country", &country)],
            );
        }
        if METRICS
            .get()
            .and_then(Option::as_ref)
            .is_some_and(|e| e.service == "spruce")
            && (endpoint == "/getinitdata"
                || endpoint == "/ws/"
                || endpoint == "/ws/live"
                || endpoint == "/ws/trip")
        {
            count(
                "spruce.initial_requests",
                &[("endpoint", &endpoint), ("country", &country)],
            );
        }
    }
    result.map(ServiceResponse::map_into_boxed_body)
}
/// Distinct GTFS chateaus in a single returned nearby response.
pub fn nearby_chateaus(
    data: &crate::nearby_departures::NearbyDeparturesV3Response,
) -> std::collections::HashSet<String> {
    let mut result = std::collections::HashSet::new();
    result.extend(data.routes.keys().cloned());
    result.extend(data.stops.keys().cloned());
    result.extend(data.local.iter().map(|route| route.chateau_id.clone()));
    result.extend(
        data.long_distance
            .iter()
            .flat_map(|station| station.departures.iter().map(|d| d.chateau_id.clone())),
    );
    result
}
/// Emit one Spruce usage count per distinct chateau in a logical request.
pub fn record_spruce_chateaus(country: &str, chateaus: impl IntoIterator<Item = String>) {
    for chateau in chateaus {
        count(
            "spruce.chateau_requests",
            &[("chateau", &chateau), ("country", country)],
        );
    }
}
/// Record one count per chateau actually present on a returned stop/station board.
/// No metrics are emitted for empty boards or purely looked-up candidate feeds.
pub fn record_stop_board_chateaus(
    endpoint: &'static str,
    country: &str,
    chateaus: impl IntoIterator<Item = String>,
) {
    let distinct: std::collections::HashSet<String> = chateaus.into_iter().collect();
    for chateau in distinct {
        count(
            "stop_board.chateau_accesses",
            &[
                ("endpoint", endpoint),
                ("chateau", &chateau),
                ("country", country),
            ],
        );
    }
}

/// Extract distinct chateaus from legs in the returned MOTIS routing list,
/// excluding empty/unknown values and lookup-only candidates.
pub fn routing_chateaus(response: &serde_json::Value) -> std::collections::HashSet<String> {
    ["itineraries", "direct"]
        .into_iter()
        .filter_map(|field| response.get(field).and_then(serde_json::Value::as_array))
        .flat_map(|journeys| journeys.iter())
        .filter_map(|journey| journey.get("legs").and_then(serde_json::Value::as_array))
        .flat_map(|legs| legs.iter())
        .filter_map(|leg| leg.get("chateau").and_then(serde_json::Value::as_str))
        .filter(|chateau| !chateau.is_empty())
        .map(str::to_owned)
        .collect()
}

pub fn record_routing_chateaus(response: &serde_json::Value, country: &str) {
    for chateau in routing_chateaus(response) {
        count(
            "routing.chateau_accesses",
            &[("chateau", &chateau), ("country", country)],
        );
    }
}

pub fn record_chateaus(country: &str, chateaus: impl IntoIterator<Item = String>) {
    for chateau in chateaus {
        count(
            "nearby.chateau_accesses",
            &[("chateau", &chateau), ("country", country)],
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn datadog_synthetics_user_agents_are_excluded() {
        assert!(is_datadog_synthetics_user_agent(Some("Datadog/Synthetics")));
        assert!(is_datadog_synthetics_user_agent(Some(
            "Datadog/Synthetics/1.0"
        )));
        assert!(is_datadog_synthetics_user_agent(Some(
            "Mozilla/5.0 Datadog/Synthetics/1.0"
        )));
        assert!(is_datadog_synthetics_user_agent(Some("datadog/synthetics")));
        assert!(!is_datadog_synthetics_user_agent(Some("Datadog")));
        assert!(!is_datadog_synthetics_user_agent(Some("Mozilla/5.0")));
        assert!(!is_datadog_synthetics_user_agent(None));
    }
    #[test]
    fn synthetics_request_detection() {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        assert!(!is_synthetics_request(&headers));
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("Datadog/Synthetics"),
        );
        assert!(is_synthetics_request(&headers));
        headers.clear();
        headers.insert(
            HeaderName::from_static("sec-datadog"),
            HeaderValue::from_static("Request sent by a Datadog Synthetics API Test"),
        );
        assert!(is_synthetics_request(&headers));
        headers.clear();
        headers.insert(
            HeaderName::from_static("x-datadog-origin"),
            HeaderValue::from_static("synthetics"),
        );
        assert!(is_synthetics_request(&headers));
        headers.clear();
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_static("Mozilla/5.0"),
        );
        assert!(!is_synthetics_request(&headers));
    }
    #[test]
    fn routing_chateaus_only_counts_returned_leg_sources_once() {
        let response = serde_json::json!({
            "itineraries": [
                {"legs": [{"chateau":"amtrak"}, {"chateau":"amtrak"}, {"chateau":null}]},
                {"legs": [{"chateau":"schweiz"}, {"chateau":""}]}
            ],
            "direct": [{"legs": [{"chateau":"amtrak"}]}]
        });
        let result = routing_chateaus(&response);
        assert_eq!(result.len(), 2);
        assert!(result.contains("amtrak"));
        assert!(result.contains("schweiz"));
        assert!(routing_chateaus(&serde_json::json!({"itineraries":[]})).is_empty());
    }
    #[test]
    fn cloudflare_country_validation() {
        assert_eq!(country(Some("US")), "US");
        assert_eq!(country(Some("us")), "unknown");
        assert_eq!(country(Some("USA")), "unknown");
        assert_eq!(country(None), "unknown");
    }
    #[test]
    fn country_from_headers_validates_proxy_country() {
        use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        assert_eq!(country_from_headers(&headers), "unknown");
        headers.insert(
            HeaderName::from_static("cf-ipcountry"),
            HeaderValue::from_static("CH"),
        );
        assert_eq!(country_from_headers(&headers), "CH");
        headers.insert(
            HeaderName::from_static("cf-ipcountry"),
            HeaderValue::from_static("usa"),
        );
        assert_eq!(country_from_headers(&headers), "unknown");
    }

    #[test]
    fn tags_cannot_inject_dogstatsd_fields() {
        assert_eq!(safe_tag("x|#evil:yes"), "x__evil_yes");
    }
}
