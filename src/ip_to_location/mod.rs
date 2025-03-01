use crate::models::IpToGeoAddr;
use crate::postgres_tools::CatenaryPostgresPool;
use ahash::AHashMap;
use csv::ReaderBuilder;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

pub static IPV4_SOURCE: &str =
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-city/geolite2-city-ipv4.csv.gz";
pub static IPV6_SOURCE: &str =
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-city/geolite2-city-ipv6.csv.gz";

pub static CSV_HEADER: &str = "ip_range_start,ip_range_end,country_code,state,state2,city,postcode,latitude,longitude,timezone";

#[derive(Debug, Deserialize, Serialize)]
pub struct CityGeoEntryRaw {
    pub ip_range_start: String,
    pub ip_range_end: String,
    pub country_code: Option<String>,
    pub state: Option<String>,
    pub state2: Option<String>,
    pub city: Option<String>,
    pub postcode: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub timezone: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CityGeoEntry {
    pub range: GeoIpRange,
    pub country_code: Option<String>,
    pub state: Option<String>,
    pub state2: Option<String>,
    pub city: Option<String>,
    pub postcode: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub timezone: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GeoIpRange {
    V4(Ipv4Addr, Ipv4Addr),
    V6(Ipv6Addr, Ipv6Addr),
}

pub fn range_maker(a: &str, b: &str) -> Option<GeoIpRange> {
    let parse_a = IpAddr::from_str(a);
    let parse_b = IpAddr::from_str(b);

    match (parse_a, parse_b) {
        (Ok(a), Ok(b)) => match (a, b) {
            (IpAddr::V4(a), IpAddr::V4(b)) => Some(GeoIpRange::V4(a, b)),
            (IpAddr::V6(a), IpAddr::V6(b)) => Some(GeoIpRange::V6(a, b)),
            _ => None,
        },
        _ => None,
    }
}

pub async fn download_and_process_url(
    is_v6: bool,
) -> Result<Vec<CityGeoEntry>, Box<dyn Error + Send + Sync>> {
    let client = reqwest::Client::new();

    let url_to_use = match is_v6 {
        true => IPV6_SOURCE,
        false => IPV4_SOURCE,
    };

    let get_gz_raw = client.get(url_to_use).send().await?;

    let gz_raw_bytes = get_gz_raw.bytes().await?;

    let mut gz = GzDecoder::new(gz_raw_bytes.as_ref());

    let mut s = String::new();
    gz.read_to_string(&mut s)?;

    let s = s;

    let corrected_file_contents = format!("{}\n{}", CSV_HEADER, s);

    raw_with_header_csv_to_strings(&corrected_file_contents)
}

pub fn raw_with_header_csv_to_strings(
    raw_input: &str,
) -> Result<Vec<CityGeoEntry>, Box<dyn Error + Send + Sync>> {
    let mut rdr = ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(raw_input.as_bytes());

    let mut result: Vec<CityGeoEntry> = vec![];

    for csv_row in rdr.deserialize() {
        let row: CityGeoEntryRaw = csv_row?;

        if let Some(valid_range) = range_maker(&row.ip_range_start, &row.ip_range_end) {
            let good_row = CityGeoEntry {
                range: valid_range,
                country_code: row.country_code,
                state: row.state,
                state2: row.state2,
                city: row.city,
                postcode: row.postcode,
                latitude: row.latitude,
                longitude: row.longitude,
                timezone: row.timezone,
            };

            result.push(good_row);
        }
    }

    Ok(result)
}

pub async fn insert_ip_db_into_postgres(
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut ipv6_download = download_and_process_url(true).await?;
    let mut ipv4_download = download_and_process_url(true).await?;

    let mut joined_downloads = vec![];
    joined_downloads.append(&mut ipv4_download);
    joined_downloads.append(&mut ipv6_download);

    let joined_ip_to_geo_addr_vec = joined_downloads
        .into_iter()
        .map(|geo_entry| {
            let is_ipv6 = match geo_entry.range {
                GeoIpRange::V6(_, _) => true,
                GeoIpRange::V4(_, _) => false,
            };

            let range_start: ipnet::IpNet = match geo_entry.range {
                GeoIpRange::V6(a, _) => ipnet::IpNet::V6(ipnet::Ipv6Net::new(a, 128).unwrap()),
                GeoIpRange::V4(a, _) => ipnet::IpNet::V4(ipnet::Ipv4Net::new(a, 32).unwrap()),
            };

            let range_end: ipnet::IpNet = match geo_entry.range {
                GeoIpRange::V6(_, b) => ipnet::IpNet::V6(ipnet::Ipv6Net::new(b, 128).unwrap()),
                GeoIpRange::V4(_, b) => ipnet::IpNet::V4(ipnet::Ipv4Net::new(b, 32).unwrap()),
            };

            IpToGeoAddr {
                is_ipv6,
                range_start,
                range_end,
                country_code: geo_entry.country_code,
                geo_state: geo_entry.state,
                geo_state2: geo_entry.state2,
                city: geo_entry.city,
                postcode: geo_entry.postcode,
                latitude: geo_entry.latitude,
                longitude: geo_entry.longitude,
                timezone: geo_entry.timezone,
            }
        })
        .collect::<Vec<IpToGeoAddr>>();

    let joined_ip_to_geo_addr_vec_dedup: Vec<IpToGeoAddr> = {
        let mut x: AHashMap<(ipnet::IpNet, ipnet::IpNet), IpToGeoAddr> = AHashMap::new();

        for item in joined_ip_to_geo_addr_vec {
            x.insert((item.range_start, item.range_end), item);
        }

        x.into_values().collect::<Vec<IpToGeoAddr>>()
    };

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        async move {
            let _ = diesel::delete(crate::schema::gtfs::ip_addr_to_geo::dsl::ip_addr_to_geo)
                .execute(conn)
                .await?;

            for chunk in joined_ip_to_geo_addr_vec_dedup.as_slice().chunks(100) {
                let _ =
                    diesel::insert_into(crate::schema::gtfs::ip_addr_to_geo::dsl::ip_addr_to_geo)
                        .values(chunk)
                        .execute(conn)
                        .await?;
            }

            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    Ok(())
}

pub async fn lookup_geo_from_ip_addr(
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    ip_addr: ipnet::IpNet,
) -> Result<Vec<crate::models::IpToGeoAddr>, Box<dyn std::error::Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let lookup_results: Vec<crate::models::IpToGeoAddr> =
        crate::schema::gtfs::ip_addr_to_geo::dsl::ip_addr_to_geo
            .filter(crate::schema::gtfs::ip_addr_to_geo::dsl::range_start.le(ip_addr))
            .filter(crate::schema::gtfs::ip_addr_to_geo::dsl::range_end.ge(ip_addr))
            .select(crate::models::IpToGeoAddr::as_select())
            .load(conn)
            .await?;

    Ok(lookup_results)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_raw_csv() {
        let input_test_ipv6: &str = r#"ip_range_start,ip_range_end,country_code,state,state2,city,postcode,latitude,longitude,timezone
        2001:418:0:5001::,2001:418:1001:4:ffff:ffff:ffff:ffff,US,,,,,37.7510,-97.8220,America/Chicago
2001:418:1001:5::,2001:418:1001:5:ffff:ffff:ffff:ffff,US,Georgia,,Atlanta,30083,33.7920,-84.2049,America/New_York
2001:418:1001:6::,2001:418:1401:1e:ffff:ffff:ffff:ffff,US,,,,,37.7510,-97.8220,America/Chicago
2001:418:1401:1f::,2001:418:1401:1f:ffff:ffff:ffff:ffff,US,California,,Los Angeles,90001,33.9720,-118.2420,America/Los_Angeles
2001:418:1401:20::,2001:418:1401:2f:ffff:ffff:ffff:ffff,US,,,,,37.7510,-97.8220,America/Chicago"#;

        let csv_output = raw_with_header_csv_to_strings(input_test_ipv6);

        if let Err(csv_err) = &csv_output {
            eprintln!("{:#?}", csv_err);
        }

        assert!(csv_output.is_ok());

        // println!("ip address conversion {:#?}", csv_output.unwrap());
    }

    #[tokio::test]
    async fn test_from_internet() {
        let ipv6_download = download_and_process_url(true).await;

        if let Err(ipv6_download_err) = &ipv6_download {
            eprintln!("{:#?}", ipv6_download_err);
        }

        assert!(ipv6_download.is_ok());

        let ipv4_download = download_and_process_url(false).await;

        assert!(ipv4_download.is_ok());
    }
}
