use catenary::routing_common::api::{EdelweissServiceClient, RoutingRequest, TravelMode};
use chrono::{Datelike, TimeZone, Utc};
use chrono_tz::America::Los_Angeles;
use clap::Parser;
use rand::Rng;
use tarpc::{client, context};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, allow_hyphen_values = true)]
    start_lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    start_lon: f64,
    #[arg(long, allow_hyphen_values = true)]
    end_lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    end_lon: f64,
    #[arg(long)]
    time: Option<u64>,
    #[arg(long, default_value = "walk")]
    mode: String,
    #[arg(long, default_value = "127.0.0.1:9090")]
    server: String,
    #[arg(long)]
    geojson: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let transport = tarpc::serde_transport::Transport::from((
        TcpStream::connect(&args.server).await?,
        tarpc::tokio_serde::formats::Json::default(),
    ));

    let client = EdelweissServiceClient::new(client::Config::default(), transport).spawn();

    let mode = match args.mode.to_lowercase().as_str() {
        "bike" => TravelMode::Bike,
        _ => TravelMode::Walk,
    };

    let time = if let Some(t) = args.time {
        t
    } else {
        let now = Utc::now().with_timezone(&Los_Angeles);
        let days_until_monday = (7 - now.weekday().num_days_from_monday()) % 7;
        let days_until_monday = if days_until_monday == 0 {
            7
        } else {
            days_until_monday
        };

        let target_date = now.date_naive() + chrono::Duration::days(days_until_monday as i64);

        let mut rng = rand::rng();
        let random_hour = rng.random_range(8..17);
        let random_minute = rng.random_range(0..60);
        let random_second = rng.random_range(0..60);

        let target_time = target_date
            .and_hms_opt(random_hour, random_minute, random_second)
            .unwrap();

        let la_time = Los_Angeles.from_local_datetime(&target_time).unwrap();
        la_time.timestamp() as u64
    };

    let req = RoutingRequest {
        start_lat: args.start_lat,
        start_lon: args.start_lon,
        end_lat: args.end_lat,
        end_lon: args.end_lon,
        time,
        is_departure_time: true,
        mode,
        speed_mps: 1.4,
        wheelchair_accessible: false,
    };

    let mut ctx = context::current();
    ctx.deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let response = client.route(ctx, req).await?;

    if args.geojson {
        // Convert to GeoJSON FeatureCollection
        let mut features = Vec::new();
        for itinerary in &response.itineraries {
            for leg in &itinerary.legs {
                if !leg.geometry().is_empty() {
                    let coords: Vec<Vec<f64>> = leg
                        .geometry()
                        .iter()
                        .map(|(lat, lon)| vec![*lon, *lat])
                        .collect();
                    let geometry = serde_json::json!({
                        "type": "LineString",
                        "coordinates": coords
                    });
                    let properties = serde_json::json!({
                        "mode": format!("{:?}", leg.mode()),
                        "route": leg.route_name(),
                        "trip": leg.trip_name(),
                        "from": leg.start_stop_name(),
                        "to": leg.end_stop_name(),
                        "duration": leg.duration_seconds()
                    });
                    features.push(serde_json::json!({
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": properties
                    }));
                }
            }
        }
        let feature_collection = serde_json::json!({
            "type": "FeatureCollection",
            "features": features
        });
        println!("{}", serde_json::to_string_pretty(&feature_collection)?);
    } else {
        println!("{:#?}", response);
    }

    Ok(())
}
