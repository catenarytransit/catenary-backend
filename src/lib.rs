// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref,
    clippy::useless_vec,
    clippy::module_inception
)]
#![recursion_limit = "4096"]

#[macro_use]
extern crate diesel_derive_newtype;
#[macro_use]
extern crate serde;

pub mod agency_secret;
pub mod aspen;
pub mod cholla;
pub mod custom_pg_types;
pub mod enum_to_int;
pub mod gtfs_rt_handlers;
pub mod gtfs_rt_rough_hash;
pub mod id_cleanup;
pub mod ip_to_location;
pub mod maple_syrup;
pub mod models;
pub mod postgis_to_diesel;
pub mod postgres_tools;
pub mod santa_cruz;
pub mod schema;
pub mod validate_gtfs_rt;
use crate::aspen::lib::RealtimeFeedMetadataEtcd;
pub mod custom_alerts;
pub mod elasticutils;
pub mod hashfolder;
pub mod hull;
pub mod routing_common;
pub mod shape_fetcher;
pub mod travic_types;
use ahash::AHasher;
use chrono::Datelike;
use chrono::NaiveDate;
use fasthash::MetroHasher;
use gtfs_realtime::{FeedEntity, FeedMessage};
use gtfs_structures::RouteType;
use schema::gtfs::trip_frequencies::start_time;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
pub mod metrolink_ptc_to_stop_id;
pub mod rt_recent_history;
use crate::rt_recent_history::*;
pub mod schedule_filtering;
pub mod tile_save_and_get;
pub mod timestamp_extraction;
use csv::ReaderBuilder;
use csv::StringRecord;
use csv::WriterBuilder;
use flate2::Compression;
use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::io::Cursor;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::{fs::File, io::BufReader, io::BufWriter};
pub mod aspen_dataset;
pub mod compact_formats;
pub mod connections_lookup;
pub mod genentech_auth;
pub mod graph_formats;
pub mod stop_matching;
pub mod trip_logic;

lazy_static! {
    static ref CLOCK_AM_PM_REGEX: Regex =
        Regex::new(r"(?i)\b(?P<hour>1[0-2]|[1-9])(?::(?P<minute>[0-5][0-9]))?\s*(?P<meridian>[AP]\.?[Mm]\.?)\b(?P<ending>\.)?").unwrap();
}

pub static THROW_AWAY_START_DATES: &[&str] = &["san-francisco-bay-area"];

pub fn fix_stop_times_headsigns(
    input_path: &str,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_file = File::open(input_path)?;
    let output_file = File::create(output_path)?;
    let input_reader = BufReader::new(input_file);
    let mut output_writer = WriterBuilder::new().from_writer(BufWriter::new(output_file));

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(input_reader);

    let headers = csv_reader.headers()?.clone();
    let mut output_headers = Vec::new();
    let mut stop_headsign_index: Option<usize> = None;
    for (i, header) in headers.into_iter().enumerate() {
        if header == "stop_headsign" {
            stop_headsign_index = Some(i);
        } else {
            output_headers.push(header);
        }
    }
    output_writer.write_record(&output_headers)?;

    let mut current_trip_id: Option<String> = None;
    let mut current_trip_rows: Vec<StringRecord> = Vec::new();
    let mut current_trip_headsigns: Vec<String> = Vec::new();

    for result in csv_reader.records() {
        let record = result?;
        if record.len() < 8 {
            continue; // Skip malformed records
        }
        let trip_id = &record[0];
        let stop_headsign = &record[5];

        if current_trip_id.is_none() || current_trip_id.as_ref().unwrap() != trip_id {
            // Process the previous trip
            if let Some(prev_trip_id) = current_trip_id {
                let all_headsigns_same = current_trip_headsigns.windows(2).all(|w| w[0] == w[1]);
                if all_headsigns_same && !current_trip_headsigns.is_empty() {
                    for row in &current_trip_rows {
                        let mut output_record = Vec::new();
                        for (i, field) in row.iter().enumerate() {
                            if Some(i) != stop_headsign_index {
                                output_record.push(field);
                            }
                        }
                        output_writer.write_record(&output_record)?;
                    }
                } else {
                    for row in &current_trip_rows {
                        output_writer.write_record(row)?;
                    }
                }
            }

            // Start a new trip
            current_trip_id = Some(trip_id.to_string());
            current_trip_rows.clear();
            current_trip_headsigns.clear();
        }

        current_trip_rows.push(record.clone()); // Added .clone() here
        current_trip_headsigns.push(stop_headsign.to_string());
    }

    // Process the last trip
    if let Some(prev_trip_id) = current_trip_id {
        let all_headsigns_same = current_trip_headsigns.windows(2).all(|w| w[0] == w[1]);
        if all_headsigns_same && !current_trip_headsigns.is_empty() {
            for row in &current_trip_rows {
                let mut output_record = Vec::new();
                for (i, field) in row.iter().enumerate() {
                    if Some(i) != stop_headsign_index {
                        output_record.push(field);
                    }
                }
                output_writer.write_record(&output_record)?;
            }
        } else {
            for row in &current_trip_rows {
                output_writer.write_record(row)?;
            }
        }
    }

    output_writer.flush()?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChateauDataNoGeometry {
    pub chateau_id: String,
    pub static_feeds: Vec<String>,
    pub realtime_feeds: Vec<String>,
}

pub const WGS_84_SRID: u32 = 4326;

pub fn compress_zlib(input: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input).unwrap();
    encoder.finish().unwrap()
}

pub fn decompress_zlib(input: &[u8]) -> Vec<u8> {
    let mut decoder = flate2::read::ZlibDecoder::new(Cursor::new(input));
    let mut decompressed_bytes = Vec::new();
    decoder.read_to_end(&mut decompressed_bytes).unwrap();

    decompressed_bytes
}

pub mod gtfs_schedule_protobuf {
    use gtfs_structures::ExactTimes;

    include!(concat!(env!("OUT_DIR"), "/gtfs_schedule_protobuf.rs"));

    fn frequency_to_protobuf(frequency: &gtfs_structures::Frequency) -> GtfsFrequencyProto {
        GtfsFrequencyProto {
            start_time: frequency.start_time,
            end_time: frequency.end_time,
            headway_secs: frequency.headway_secs,
            exact_times: match frequency.exact_times {
                Some(ExactTimes::FrequencyBased) => Some(ExactTimesProto::FrequencyBased.into()),
                Some(ExactTimes::ScheduleBased) => Some(ExactTimesProto::ScheduleBased.into()),
                None => None,
            },
        }
    }

    fn protobuf_to_frequency(frequency: &GtfsFrequencyProto) -> gtfs_structures::Frequency {
        gtfs_structures::Frequency {
            start_time: frequency.start_time,
            end_time: frequency.end_time,
            headway_secs: frequency.headway_secs,
            exact_times: match frequency.exact_times {
                Some(0) => Some(ExactTimes::FrequencyBased),
                Some(1) => Some(ExactTimes::ScheduleBased),
                _ => None,
                None => None,
            },
        }
    }

    pub fn frequencies_to_protobuf(
        frequencies: &Vec<gtfs_structures::Frequency>,
    ) -> GtfsFrequenciesProto {
        let frequencies: Vec<GtfsFrequencyProto> =
            frequencies.iter().map(frequency_to_protobuf).collect();

        GtfsFrequenciesProto { frequencies }
    }

    pub fn protobuf_to_frequencies(
        frequencies: &GtfsFrequenciesProto,
    ) -> Vec<gtfs_structures::Frequency> {
        frequencies
            .frequencies
            .iter()
            .map(protobuf_to_frequency)
            .collect()
    }
}

pub fn fast_hash<T: Hash>(t: &T) -> u64 {
    let mut s: MetroHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

pub fn ahash_fast_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = AHasher::default();
    t.hash(&mut hasher);
    hasher.finish()
}

/*
pub fn gx_fast_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = gxhasher::GxBuildHasher::default();
    t.hash(&mut hasher);
    hasher.finish()
}*/

pub fn duration_since_unix_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn parse_gtfs_rt_message(
    bytes: &[u8],
) -> Result<gtfs_realtime::FeedMessage, Box<dyn std::error::Error>> {
    let x = prost::Message::decode(bytes);

    match x {
        Ok(x) => Ok(x),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn route_id_transform(feed_id: &str, route_id: String) -> String {
    match feed_id {
        "f-dp3-pace~rt" => {
            if !route_id.contains("-367") {
                format!("{}-367", route_id)
            } else {
                route_id.to_owned()
            }
        }
        _ => route_id,
    }
}

pub async fn get_node_for_realtime_feed_id_kvclient(
    etcd: &mut etcd_client::KvClient,
    realtime_feed_id: &str,
) -> Option<RealtimeFeedMetadataEtcd> {
    let node = etcd
        .get(
            format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id).as_str(),
            None,
        )
        .await;

    match node {
        Ok(resp) => {
            let kvs = resp.kvs();

            match kvs.len() {
                0 => None,
                _ => {
                    let data = bincode::serde::decode_from_slice::<
                        RealtimeFeedMetadataEtcd,
                        bincode::config::Configuration<
                            bincode::config::LittleEndian,
                            bincode::config::Fixint,
                            bincode::config::NoLimit,
                        >,
                    >(kvs[0].value(), bincode::config::legacy());

                    match data {
                        Ok(data) => Some(data.0),
                        Err(e) => {
                            println!("Error deserializing RealtimeFeedMetadataEtcd: {:?}", e);
                            None
                        }
                    }
                }
            }
        }
        _ => None,
    }
}

pub async fn get_node_for_realtime_feed_id(
    etcd: &mut etcd_client::Client,
    realtime_feed_id: &str,
) -> Option<RealtimeFeedMetadataEtcd> {
    let node = etcd
        .get(
            format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id).as_str(),
            None,
        )
        .await;

    match node {
        Ok(resp) => {
            let kvs = resp.kvs();

            match kvs.len() {
                0 => None,
                _ => {
                    let data = bincode::serde::decode_from_slice::<
                        RealtimeFeedMetadataEtcd,
                        bincode::config::Configuration<
                            bincode::config::LittleEndian,
                            bincode::config::Fixint,
                            bincode::config::NoLimit,
                        >,
                    >(kvs[0].value(), bincode::config::legacy());

                    match data {
                        Ok(data) => Some(data.0),
                        Err(e) => {
                            println!("Error deserializing RealtimeFeedMetadataEtcd: {:?}", e);
                            None
                        }
                    }
                }
            }
        }
        _ => None,
    }
}

pub fn make_feed_from_entity_vec(entities: Vec<FeedEntity>) -> FeedMessage {
    FeedMessage {
        header: gtfs_realtime::FeedHeader {
            gtfs_realtime_version: "2.0".to_string(),
            incrementality: Some(gtfs_realtime::feed_header::Incrementality::FullDataset as i32),
            timestamp: Some(duration_since_unix_epoch().as_secs() as u64),
            feed_version: None,
        },
        entity: entities,
    }
}

pub mod unzip_uk {
    use std::io::Read;
    pub async fn get_raw_gtfs_rt(
        client: &reqwest::Client,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Sync + Send>> {
        let url = "https://data.bus-data.dft.gov.uk/avl/download/gtfsrt";
        let response = client.get(url).send().await?;
        let bytes = response.bytes().await?;

        //unzip and return file gtfsrt.bin
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
        let mut file = zip.by_name("gtfsrt.bin")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod unzip_uk_test {
    use super::*;
    use reqwest::Client;
    #[tokio::test]
    async fn test_get_raw_gtfs_rt() {
        let client = Client::new();
        let x = unzip_uk::get_raw_gtfs_rt(&client).await.unwrap();
        assert!(!x.is_empty());

        //attempt to decode into gtfs-rt

        let x = parse_gtfs_rt_message(&x);

        assert!(x.is_ok());

        let x = x.unwrap();

        // println!("{:#?}", x);
    }
}

pub struct EtcdConnectionIps {
    pub ip_addresses: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerializableStop {
    pub id: String,
    pub code: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub location_type: i16,
    pub parent_station: Option<String>,
    pub zone_id: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub timezone: Option<String>,
    pub platform_code: Option<String>,
    pub level_id: Option<String>,
    pub routes: Vec<String>,
}

pub fn is_null_island(x: f64, y: f64) -> bool {
    x.abs() < 0.1 && y.abs() < 0.1
}

pub fn contains_rail_or_metro_lines(gtfs: &gtfs_structures::Gtfs) -> bool {
    let mut answer = false;

    for (_, route) in gtfs.routes.iter() {
        let is_rail_line = matches!(
            route.route_type,
            RouteType::Tramway
                | RouteType::Subway
                | RouteType::Rail
                | RouteType::CableCar
                | RouteType::Funicular
        );

        if is_rail_line {
            answer = true;
            break;
        }
    }

    answer
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneralCalendar {
    pub days: Vec<chrono::Weekday>,
    pub start_date: chrono::NaiveDate,
    pub end_date: chrono::NaiveDate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalendarUnified {
    pub id: String,
    pub general_calendar: Option<GeneralCalendar>,
    pub exceptions:
        Option<std::collections::BTreeMap<chrono::NaiveDate, gtfs_structures::Exception>>,
}

// Kyler Chin
// Iterator Optimisation by https://github.com/Priyansh4444 Priyash Sash
pub fn make_weekdays(calendar: &crate::models::Calendar) -> Vec<chrono::Weekday> {
    use chrono::Weekday::*;

    let day_list = [
        (calendar.monday, Mon),
        (calendar.tuesday, Tue),
        (calendar.wednesday, Wed),
        (calendar.thursday, Thu),
        (calendar.friday, Fri),
        (calendar.saturday, Sat),
        (calendar.sunday, Sun),
    ];

    day_list
        .into_iter()
        .filter(|(a, _)| *a)
        .map(|(_, b)| b)
        .collect()
}

pub fn make_calendar_structure_from_pg_single_chateau(
    services_calendar_lookup_queries_to_perform: Vec<crate::models::Calendar>,
    services_calendar_dates_lookup_queries_to_perform: Vec<crate::models::CalendarDate>,
) -> BTreeMap<String, CalendarUnified> {
    let mut calendar_structures: BTreeMap<String, CalendarUnified> = BTreeMap::new();

    for calendar in services_calendar_lookup_queries_to_perform {
        calendar_structures.insert(
            calendar.service_id.clone(),
            CalendarUnified {
                id: calendar.service_id.clone(),
                general_calendar: Some(GeneralCalendar {
                    days: make_weekdays(&calendar),
                    start_date: calendar.gtfs_start_date,
                    end_date: calendar.gtfs_end_date,
                }),
                exceptions: None,
            },
        );
    }

    for calendar_date in services_calendar_dates_lookup_queries_to_perform {
        let exception_number = match calendar_date.exception_type {
            1 => gtfs_structures::Exception::Added,
            2 => gtfs_structures::Exception::Deleted,
            _ => panic!("WHAT IS THIS!!!!!!"),
        };

        match calendar_structures.entry(calendar_date.service_id.clone()) {
            btree_map::Entry::Occupied(mut oe) => {
                let mut calendar_unified = oe.get_mut();

                if let Some(entry) = &mut calendar_unified.exceptions {
                    entry.insert(calendar_date.gtfs_date, exception_number);
                } else {
                    calendar_unified.exceptions = Some(BTreeMap::from_iter([(
                        calendar_date.gtfs_date,
                        exception_number,
                    )]));
                }
            }
            btree_map::Entry::Vacant(mut ve) => {
                ve.insert(CalendarUnified::empty_exception_from_calendar_date(
                    &calendar_date,
                ));
            }
        }
    }

    calendar_structures
}

impl CalendarUnified {
    pub fn empty_exception_from_calendar_date(x: &crate::models::CalendarDate) -> Self {
        CalendarUnified {
            id: x.service_id.clone(),
            general_calendar: None,
            exceptions: Some(std::collections::BTreeMap::from_iter([(
                x.gtfs_date,
                match x.exception_type {
                    1 => gtfs_structures::Exception::Added,
                    2 => gtfs_structures::Exception::Deleted,
                    _ => panic!("WHAT IS THIS!!!!!!"),
                },
            )])),
        }
    }
}

pub struct TripToFindScheduleFor {
    pub trip_id: String,
    pub chateau: String,
    pub timezone: chrono_tz::Tz,
    pub time_since_start_of_service_date: chrono::Duration,
    pub frequency: Option<Vec<gtfs_structures::Frequency>>,
    pub itinerary_id: String,
    pub direction_id: String,
}

pub fn find_service_ranges(
    service: &CalendarUnified,
    trip_instance: &TripToFindScheduleFor,
    input_time: chrono::DateTime<chrono::Utc>,
    back_duration: chrono::Duration,
    forward_duration: chrono::Duration,
) -> Vec<(chrono::NaiveDate, chrono::DateTime<chrono_tz::Tz>)> {
    let start_chrono = input_time - back_duration;

    let additional_lookback = match &trip_instance.frequency {
        Some(freq) => {
            freq.iter()
                .max_by(|a, b| a.end_time.cmp(&b.end_time))
                .unwrap()
                .end_time
        }
        None => 0,
    };

    let start_service_datetime_falls_here = start_chrono
        - trip_instance.time_since_start_of_service_date
        - chrono::TimeDelta::new(additional_lookback.into(), 0).unwrap();

    let end_chrono = input_time + forward_duration - trip_instance.time_since_start_of_service_date;

    let look_at_this_service_start =
        start_service_datetime_falls_here.with_timezone(&trip_instance.timezone);

    let look_at_this_service_end = end_chrono.with_timezone(&trip_instance.timezone);

    let start_service_date_check = look_at_this_service_start.date_naive();
    let end_date_service_check = look_at_this_service_end.date_naive();

    let mut i = start_service_date_check;
    let mut valid_service_days_to_look_at: Vec<NaiveDate> = vec![];

    while i <= end_date_service_check {
        if datetime_in_service(service, i) {
            valid_service_days_to_look_at.push(i);
        }

        i = i.succ_opt().unwrap();
    }

    valid_service_days_to_look_at.sort();
    valid_service_days_to_look_at.dedup();

    //println!("checked {:?} from {:?} to {:?}, found {} valid", service, start_service_date_check, end_date_service_check, valid_service_days_to_look_at.len());

    let results = valid_service_days_to_look_at
        .iter()
        .map(|nd| {
            let noon = nd
                .and_hms_opt(12, 0, 0)
                .unwrap()
                .and_local_timezone(trip_instance.timezone)
                .unwrap();

            let starting_time = noon - chrono::TimeDelta::new(43200, 0).unwrap();

            (*nd, starting_time)
        })
        .collect::<Vec<(chrono::NaiveDate, chrono::DateTime<chrono_tz::Tz>)>>();

    results
}

pub fn datetime_in_service(service: &CalendarUnified, input_date: chrono::NaiveDate) -> bool {
    let mut answer = false;

    if let Some(calendar_general) = &service.general_calendar {
        let weekday = input_date.weekday();

        if calendar_general.days.contains(&weekday)
            && calendar_general.start_date <= input_date
            && calendar_general.end_date >= input_date
        {
            answer = true;
        }
    }

    if let Some(exceptions) = &service.exceptions {
        if let Some(exception) = exceptions.get(&input_date) {
            match exception {
                gtfs_structures::Exception::Added => {
                    answer = true;
                }
                gtfs_structures::Exception::Deleted => {
                    answer = false;
                }
            }
        }
    }

    answer
}

#[cfg(test)]
mod test_calendar {
    use super::*;

    #[test]
    fn test_date() {
        let calendar = CalendarUnified {
            id: "a".to_string(),
            general_calendar: Some(GeneralCalendar {
                days: vec![chrono::Weekday::Mon],
                start_date: NaiveDate::from_ymd(2024, 8, 1),
                end_date: NaiveDate::from_ymd(2024, 8, 31),
            }),
            exceptions: None,
        };

        let date = NaiveDate::from_ymd(2024, 8, 26);

        assert!(datetime_in_service(&calendar, date));

        let trip_instance = TripToFindScheduleFor {
            trip_id: "11499201".to_string(),
            chateau: "orangecountytransportationauthority".to_string(),
            timezone: chrono_tz::Tz::UTC,
            time_since_start_of_service_date: chrono::Duration::zero(),
            frequency: None,
            itinerary_id: "9936372064990961207".to_string(),
            direction_id: "0".to_string(),
        };
    }
}

// Metrolink date fix
pub fn metrolink_unix_fix(date: &str) -> u64 {
    ///Date(1729199040000)/
    //extract all the numbers
    let mut numbers = date.chars().filter(|x| x.is_numeric()).collect::<String>();

    //remove the last 3 digits

    numbers.pop();
    numbers.pop();
    numbers.pop();

    //convert to number

    numbers.parse::<u64>().unwrap()
}

pub fn bincode_serialize<T>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError>
where
    T: serde::Serialize,
{
    use bincode::config;
    use bincode::config::*;

    let config: Configuration<LittleEndian, Fixint> = config::legacy();

    bincode::serde::encode_to_vec(value, config)
}

pub fn bincode_deserialize<T>(value: &[u8]) -> Result<T, bincode::error::DecodeError>
where
    T: serde::de::DeserializeOwned,
{
    use bincode::config;
    use bincode::config::*;

    let config: Configuration<LittleEndian, Fixint> = config::legacy();

    match bincode::serde::decode_from_slice(value, config) {
        Ok(x) => Ok(x.0),
        Err(e) => Err(e),
    }
}

pub fn convert_text_12h_to_24h(input: &str) -> String {
    // This regex matches:
    // - hour: 1-9 or 10,11,12
    // - optional minute part, e.g. :30
    // - optional whitespace between time and meridian indicator
    // - meridian indicator: am, a.m., pm, p.m. (case insensitive)
    // - optional trailing period to preserve punctuation.

    CLOCK_AM_PM_REGEX
        .replace_all(input, |caps: &regex::Captures| {
            // Parse the hour and optional minute.
            let hour_str = &caps["hour"];
            let hour: u32 = hour_str.parse().unwrap();
            let minute_str = caps.name("minute").map_or("00", |m| m.as_str());
            let minute: u32 = minute_str.parse().unwrap();

            // Normalize the meridian indicator to lowercase for easier checking.
            let meridian = caps["meridian"].to_ascii_lowercase();
            // Preserve the optional ending period, if present.
            let ending = caps.name("ending").map_or("", |m| m.as_str());

            // Convert 12-hour time to 24-hour time.
            let mut hour_24 = hour;
            if meridian.starts_with('p') {
                if hour != 12 {
                    hour_24 += 12;
                }
            } else if meridian.starts_with('a') {
                if hour == 12 {
                    hour_24 = 0;
                }
            }
            // Format as HH:MM and append any ending punctuation.
            format!("{:02}:{:02}{}", hour_24, minute, ending)
        })
        .to_string()
}

pub fn convert_hhmmss_to_seconds(input: &str) -> Option<u32> {
    let parts: Vec<&str> = input.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let hours: Result<u32, _> = parts[0].parse();
    let minutes: Result<u32, _> = parts[1].parse();
    let seconds: Result<u32, _> = parts[2].parse();

    match (hours, minutes, seconds) {
        (Ok(hours), Ok(minutes), Ok(seconds)) => Some(hours * 3600 + minutes * 60 + seconds),
        _ => None,
    }
}

pub fn number_of_seconds_to_hhmmss(input: u32) -> String {
    let hours = input / 3600;
    let minutes = (input % 3600) / 60;
    let seconds = input % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_size_of_stop_update() {
        let x = std::mem::size_of::<gtfs_realtime::trip_update::StopTimeUpdate>();
        println!("Size of StopTimeUpdate: {}", x);

        let x = std::mem::size_of::<crate::aspen_dataset::AspenisedStopTimeUpdate>();

        println!("Size of AspenisedStopTimeUpdate: {}", x);
    }
}

use geo::HaversineDestination;
pub fn make_degree_length_as_distance_from_point(point: &geo::Point, distance_metres: f64) -> f64 {
    let direction = match point.x() > 0. {
        true => 90.,
        false => -90.,
    };

    let distance_calc_point = point.haversine_destination(direction, distance_metres);

    f64::abs(distance_calc_point.x() - point.x())
}

pub fn make_calendar_structure_from_pg(
    services_calendar_lookup_queries_to_perform: Vec<Vec<crate::models::Calendar>>,
    services_calendar_dates_lookup_queries_to_perform: Vec<Vec<crate::models::CalendarDate>>,
) -> Result<
    BTreeMap<String, BTreeMap<String, crate::CalendarUnified>>,
    Box<dyn std::error::Error + Sync + Send>,
> {
    let mut calendar_structures: BTreeMap<String, BTreeMap<String, crate::CalendarUnified>> =
        BTreeMap::new();

    for calendar_group in services_calendar_lookup_queries_to_perform {
        let chateau = match calendar_group.get(0) {
            Some(calendar) => calendar.chateau.clone(),
            None => continue,
        };

        let mut new_calendar_table: BTreeMap<String, crate::CalendarUnified> = BTreeMap::new();

        for calendar in calendar_group {
            new_calendar_table.insert(
                calendar.service_id.clone(),
                crate::CalendarUnified {
                    id: calendar.service_id.clone(),
                    general_calendar: Some(crate::GeneralCalendar {
                        days: make_weekdays(&calendar),
                        start_date: calendar.gtfs_start_date,
                        end_date: calendar.gtfs_end_date,
                    }),
                    exceptions: None,
                },
            );
        }

        calendar_structures.insert(chateau, new_calendar_table);
    }

    for calendar_date_group in services_calendar_dates_lookup_queries_to_perform {
        if !calendar_date_group.is_empty() {
            let chateau = match calendar_date_group.get(0) {
                Some(calendar_date) => calendar_date.chateau.clone(),
                None => continue,
            };

            let pile_of_calendars_exists = calendar_structures.contains_key(&chateau);

            if !pile_of_calendars_exists {
                calendar_structures.insert(chateau.clone(), BTreeMap::new());
            }

            let pile_of_calendars = calendar_structures.get_mut(&chateau).unwrap();

            for calendar_date in calendar_date_group {
                let exception_number = match calendar_date.exception_type {
                    1 => gtfs_structures::Exception::Added,
                    2 => gtfs_structures::Exception::Deleted,
                    _ => panic!("WHAT IS THIS!!!!!!"),
                };

                match pile_of_calendars.entry(calendar_date.service_id.clone()) {
                    btree_map::Entry::Occupied(mut oe) => {
                        let mut calendar_unified = oe.get_mut();

                        if let Some(entry) = &mut calendar_unified.exceptions {
                            entry.insert(calendar_date.gtfs_date, exception_number);
                        } else {
                            calendar_unified.exceptions = Some(BTreeMap::from_iter([(
                                calendar_date.gtfs_date,
                                exception_number,
                            )]));
                        }
                    }
                    btree_map::Entry::Vacant(mut ve) => {
                        ve.insert(CalendarUnified::empty_exception_from_calendar_date(
                            &calendar_date,
                        ));
                    }
                }
            }
        }
    }

    Ok(calendar_structures)
}

pub fn serde_value_to_translated_hashmap(
    input: &Option<serde_json::value::Value>,
) -> Option<std::collections::HashMap<String, String>> {
    match input {
        Some(input) => match input.as_object() {
            Some(obj) => Some(
                obj.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            None => None,
        },
        None => None,
    }
}

pub fn name_shortening_hash_insert_elastic(
    h: &mut std::collections::HashMap<String, String>,
    lang: &language_tags::LanguageTag,
    translated_result: &str,
) {
    if lang.primary_language() != "zh" || lang.primary_language() != "ja" {
        h.insert(
            lang.primary_language().to_lowercase().to_string(),
            translated_result.to_string(),
        );
    } else {
        let lang_tag = lang
            .to_string()
            .replace("-", "_")
            .to_lowercase()
            .to_string();

        h.insert(lang_tag, translated_result.to_string());
    }
}

pub fn yyyymmdd_to_naive_date(date_str: &str) -> Result<NaiveDate, chrono::ParseError> {
    NaiveDate::parse_from_str(date_str, "%Y%m%d")
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct GirolleFeedDownloadResult {
    pub feed_id: String,
    pub seahash: Option<u64>,
    pub download_timestamp_ms: u64,
    pub valid_gtfs: bool,
    pub time_to_download_ms: Option<u64>,
    pub byte_size: Option<usize>,
    pub url: String,
}

pub async fn get_etcd_client(
    etcd_connection_ips: &EtcdConnectionIps,
    etcd_connection_options: &Option<etcd_client::ConnectOptions>,
    etcd_reuser: &tokio::sync::RwLock<Option<etcd_client::Client>>,
) -> Result<etcd_client::Client, String> {
    {
        let etcd_reuser_contents = etcd_reuser.read().await;
        if let Some(client) = etcd_reuser_contents.as_ref() {
            let mut client = client.clone();
            if client.status().await.is_ok() {
                return Ok(client);
            }
        }
    }

    let new_client = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().cloned(),
    )
    .await;

    match new_client {
        Ok(client) => {
            let mut etcd_reuser_write_lock = etcd_reuser.write().await;
            *etcd_reuser_write_lock = Some(client.clone());
            Ok(client)
        }
        Err(e) => Err(format!("{}", e)),
    }
}

pub async fn invalidate_etcd_client(
    etcd_reuser: &tokio::sync::RwLock<Option<etcd_client::Client>>,
) {
    let mut lock = etcd_reuser.write().await;
    *lock = None;
}
