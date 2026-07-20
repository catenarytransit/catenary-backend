use ahash::AHashMap;
use catenary::agency_specific_types::mta_rail::MtaTrain;
use chrono::{NaiveDate, TimeZone};
use ecow::EcoString;
use gtfs_structures::{Gtfs, Trip};
use prost::Message;
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::OnceCell;

const LIRR_TRIPS_FEED: &str =
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr";
const MNR_TRIPS_FEED: &str =
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr";
const LIRR_STATIC_GTFS: &str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip";
const MNR_STATIC_GTFS: &str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip";
const MAX_MTA_GTFS_RT_AGE_SECONDS: u64 = 5 * 60;

static LIRR_GTFS: OnceCell<Arc<Gtfs>> = OnceCell::const_new();
static MNR_GTFS: OnceCell<Arc<Gtfs>> = OnceCell::const_new();

pub async fn get_and_convert_lirr(
    client: &reqwest::Client,
) -> Result<
    (Vec<gtfs_realtime::FeedEntity>, gtfs_realtime::FeedMessage),
    Box<dyn std::error::Error + Sync + Send>,
> {
    let import_data = get_mta_locations(client, MtaRailroad::LIRR).await?;
    let gtfs_rt_trips =
        get_mta_trips_with_fallback(client, MtaRailroad::LIRR, &import_data).await?;
    let converted = convert(&import_data, MtaRailroad::LIRR, &gtfs_rt_trips);

    Ok((converted, gtfs_rt_trips))
}

pub async fn fetch_mta_lirr_data(
    realtime_feed_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::RealtimeFeedMetadataEtcd>,
    >,
    feed_id: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    match get_and_convert_lirr(client).await {
        Ok((converted, gtfs_rt_trips)) => {
            let lirr_vehicle_position = catenary::make_feed_from_entity_vec(converted);

            send_mta_rail_to_aspen(
                realtime_feed_cache,
                MtaRailroad::LIRR,
                lirr_vehicle_position.encode_to_vec(),
                gtfs_rt_trips.encode_to_vec(),
                feed_id,
            )
            .await;
        }
        Err(error) => eprintln!("Failed to build LIRR realtime data: {error}"),
    }

    Ok(())
}

pub async fn fetch_mta_metronorth_data(
    realtime_feed_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::RealtimeFeedMetadataEtcd>,
    >,
    feed_id: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let import_data = get_mta_locations(client, MtaRailroad::MNR).await?;
    let gtfs_rt_trips = get_mta_trips_with_fallback(client, MtaRailroad::MNR, &import_data).await?;
    let converted = convert(&import_data, MtaRailroad::MNR, &gtfs_rt_trips);
    let mnr_vehicle_position = catenary::make_feed_from_entity_vec(converted);

    send_mta_rail_to_aspen(
        realtime_feed_cache,
        MtaRailroad::MNR,
        mnr_vehicle_position.encode_to_vec(),
        gtfs_rt_trips.encode_to_vec(),
        feed_id,
    )
    .await;

    Ok(())
}

async fn get_mta_locations(
    client: &reqwest::Client,
    railroad: MtaRailroad,
) -> Result<Vec<MtaTrain>, Box<dyn std::error::Error + Sync + Send>> {
    let request = client
        .get(format!(
            "https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad={}",
            railroad.as_str()
        ))
        .header("Accept-Version", "3.0")
        .send()
        .await?
        .error_for_status()?;

    Ok(serde_json::from_slice(&request.bytes().await?)?)
}

fn get_lirr_train_id(entity: &gtfs_realtime::FeedEntity) -> Option<&str> {
    entity
        .trip_update
        .as_ref()
        .and_then(|trip_update| trip_update.trip.trip_id.as_deref())
        .and_then(|trip_id| trip_id.split('_').nth(2))
}

async fn get_mta_trips(
    client: &reqwest::Client,
    url: &str,
) -> Result<gtfs_realtime::FeedMessage, Box<dyn std::error::Error + Sync + Send>> {
    let resp = client
        .get(url)
        .header("x-api-key", "hvThsOlHmP2XzvYWlKKC17YPcq07meIg2V2RPLbC")
        .send()
        .await?
        .error_for_status()?;

    Ok(gtfs_realtime::FeedMessage::decode(resp.bytes().await?)?)
}

async fn get_mta_trips_with_fallback(
    client: &reqwest::Client,
    railroad: MtaRailroad,
    trains: &[MtaTrain],
) -> Result<gtfs_realtime::FeedMessage, Box<dyn std::error::Error + Sync + Send>> {
    let now = catenary::duration_since_unix_epoch().as_secs();
    let realtime_result = get_mta_trips(client, railroad.realtime_url()).await;

    let mut trip_feed = match realtime_result {
        Ok(feed) if feed_is_fresh(&feed, now) => feed,
        Ok(feed) => {
            eprintln!(
                "{} GTFS-RT is stale (latest timestamp {:?}); synthesizing it from MTA JSON",
                railroad.as_str(),
                latest_feed_timestamp(&feed)
            );
            empty_trip_feed(now)
        }
        Err(error) => {
            eprintln!(
                "Failed to fetch {} GTFS-RT ({error}); synthesizing it from MTA JSON",
                railroad.as_str()
            );
            empty_trip_feed(now)
        }
    };

    if railroad == MtaRailroad::MNR && !trip_feed.entity.is_empty() {
        match load_static_gtfs(railroad).await {
            Ok(gtfs) => trip_feed = mnr_trip_id_fixer(&gtfs, trip_feed),
            Err(error) => eprintln!("Failed to load MNR static GTFS for trip ID repair: {error}"),
        }
    }

    let missing_trains = trains
        .iter()
        .filter(|train| train.railroad == railroad.as_str())
        .filter(|train| {
            !trip_feed
                .entity
                .iter()
                .any(|entity| entity_matches_train(entity, train, railroad))
        })
        .collect::<Vec<_>>();

    if missing_trains.is_empty() {
        return Ok(trip_feed);
    }

    let static_gtfs = match load_static_gtfs(railroad).await {
        Ok(gtfs) => gtfs,
        Err(error) if !trip_feed.entity.is_empty() => {
            eprintln!(
                "Failed to load {} static GTFS for {} unmatched trains: {error}",
                railroad.as_str(),
                missing_trains.len()
            );
            return Ok(trip_feed);
        }
        Err(error) => return Err(error),
    };

    let mut synthesized = 0;
    for train in missing_trains {
        match synthesize_trip_entity(&static_gtfs, train, railroad) {
            Some(entity) => {
                trip_feed.entity.push(entity);
                synthesized += 1;
            }
            None => eprintln!(
                "Could not match {} train {} on {} to static GTFS",
                railroad.as_str(),
                train.train_num,
                train.run_date
            ),
        }
    }

    if synthesized > 0 {
        trip_feed.header.gtfs_realtime_version = "2.0".to_string();
        trip_feed.header.timestamp = Some(now);
        println!(
            "Synthesized {synthesized} {} GTFS-RT trip updates",
            railroad.as_str()
        );
    }

    Ok(trip_feed)
}

fn empty_trip_feed(timestamp: u64) -> gtfs_realtime::FeedMessage {
    let mut feed = gtfs_realtime::FeedMessage::default();
    feed.header.gtfs_realtime_version = "2.0".to_string();
    feed.header.timestamp = Some(timestamp);
    feed
}

fn latest_feed_timestamp(feed: &gtfs_realtime::FeedMessage) -> Option<u64> {
    let mut latest: Option<u64> = None;

    for entity in &feed.entity {
        if let Some(timestamp) = entity
            .trip_update
            .as_ref()
            .and_then(|trip_update| trip_update.timestamp)
        {
            latest = Some(latest.map_or(timestamp, |current| current.max(timestamp)));
        }
        if let Some(timestamp) = entity
            .vehicle
            .as_ref()
            .and_then(|vehicle| vehicle.timestamp)
        {
            latest = Some(latest.map_or(timestamp, |current| current.max(timestamp)));
        }
    }

    latest.or(feed.header.timestamp)
}

fn feed_is_fresh(feed: &gtfs_realtime::FeedMessage, now: u64) -> bool {
    latest_feed_timestamp(feed).is_some_and(|timestamp| {
        timestamp <= now.saturating_add(60)
            && now.saturating_sub(timestamp) <= MAX_MTA_GTFS_RT_AGE_SECONDS
    })
}

async fn load_static_gtfs(
    railroad: MtaRailroad,
) -> Result<Arc<Gtfs>, Box<dyn std::error::Error + Sync + Send>> {
    let (cell, url) = match railroad {
        MtaRailroad::LIRR => (&LIRR_GTFS, LIRR_STATIC_GTFS),
        MtaRailroad::MNR => (&MNR_GTFS, MNR_STATIC_GTFS),
    };

    let gtfs = cell
        .get_or_try_init(|| async move {
            println!("Lazy-loading {} static GTFS from {url}", railroad.as_str());
            gtfs_structures::GtfsReader::default()
                .read_shapes(false)
                .read_from_url_async(url)
                .await
                .map(Arc::new)
        })
        .await?;

    Ok(Arc::clone(gtfs))
}

fn entity_matches_train(
    entity: &gtfs_realtime::FeedEntity,
    train: &MtaTrain,
    railroad: MtaRailroad,
) -> bool {
    let vehicle_id_matches = entity
        .vehicle
        .as_ref()
        .and_then(|vehicle| vehicle.vehicle.as_ref())
        .and_then(|vehicle| vehicle.id.as_deref())
        .or_else(|| {
            entity
                .trip_update
                .as_ref()
                .and_then(|trip_update| trip_update.vehicle.as_ref())
                .and_then(|vehicle| vehicle.id.as_deref())
        })
        == Some(train.train_num.as_str());

    vehicle_id_matches
        || match railroad {
            MtaRailroad::MNR => entity.id == train.train_num,
            MtaRailroad::LIRR => get_lirr_train_id(entity) == Some(train.train_num.as_str()),
        }
}

fn find_static_trip<'a>(gtfs: &'a Gtfs, train: &MtaTrain) -> Option<&'a Trip> {
    let service_date = NaiveDate::parse_from_str(&train.run_date, "%Y-%m-%d").ok()?;
    let mut candidates = gtfs
        .trips
        .values()
        .filter(|trip| trip.trip_short_name.as_deref() == Some(train.train_num.as_str()))
        .filter(|trip| gtfs.trip_days(&trip.service_id, service_date).contains(&0))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));

    if candidates.len() == 1 {
        return candidates.first().copied();
    }

    let midnight = chrono_tz::America::New_York
        .from_local_datetime(&service_date.and_hms_opt(0, 0, 0)?)
        .earliest()?
        .timestamp();

    let mut best: Option<(&Trip, usize, bool, i64)> = None;
    for trip in candidates {
        let stop_pairs = matched_stop_pairs(train, trip);
        let matched_stops = stop_pairs.len();
        if matched_stops == 0 {
            continue;
        }

        let headsign_matches = trip
            .trip_headsign
            .as_deref()
            .is_some_and(|headsign| headsign.eq_ignore_ascii_case(&train.details.headsign));
        let schedule_error = stop_pairs
            .iter()
            .filter_map(|(mta_index, gtfs_index)| {
                let static_time = trip.stop_times[*gtfs_index]
                    .departure_time
                    .or(trip.stop_times[*gtfs_index].arrival_time)?;
                let mta_time = i64::from(train.details.stops[*mta_index].sched_time) - midnight;
                Some((mta_time - i64::from(static_time)).abs())
            })
            .sum::<i64>();

        let should_replace = match best.as_ref() {
            None => true,
            Some((best_trip, best_stops, best_headsign, best_error)) => {
                matched_stops > *best_stops
                    || (matched_stops == *best_stops && headsign_matches && !*best_headsign)
                    || (matched_stops == *best_stops
                        && headsign_matches == *best_headsign
                        && schedule_error < *best_error)
                    || (matched_stops == *best_stops
                        && headsign_matches == *best_headsign
                        && schedule_error == *best_error
                        && trip.id < best_trip.id)
            }
        };

        if should_replace {
            best = Some((trip, matched_stops, headsign_matches, schedule_error));
        }
    }

    best.map(|(trip, _, _, _)| trip)
}

fn matched_stop_pairs(train: &MtaTrain, trip: &Trip) -> Vec<(usize, usize)> {
    let mut next_gtfs_index = 0;
    let mut pairs = Vec::new();

    for (mta_index, mta_stop) in train.details.stops.iter().enumerate() {
        let Some(relative_index) =
            trip.stop_times[next_gtfs_index..]
                .iter()
                .position(|stop_time| {
                    stop_time.stop.code.as_deref() == Some(mta_stop.code.as_str())
                        || stop_time.stop.id == mta_stop.code
                })
        else {
            continue;
        };

        let gtfs_index = next_gtfs_index + relative_index;
        pairs.push((mta_index, gtfs_index));
        next_gtfs_index = gtfs_index + 1;
    }

    pairs
}

fn synthesize_trip_entity(
    gtfs: &Gtfs,
    train: &MtaTrain,
    railroad: MtaRailroad,
) -> Option<gtfs_realtime::FeedEntity> {
    let trip = find_static_trip(gtfs, train)?;
    let stop_pairs = matched_stop_pairs(train, trip);

    let mut trip_descriptor = gtfs_realtime::TripDescriptor::default();
    trip_descriptor.trip_id = Some(trip.id.clone());
    trip_descriptor.route_id = Some(trip.route_id.clone());
    trip_descriptor.start_date = Some(train.run_date.replace('-', ""));
    trip_descriptor.schedule_relationship = Some(if train.status.canceled {
        gtfs_realtime::trip_descriptor::ScheduleRelationship::Canceled
    } else {
        gtfs_realtime::trip_descriptor::ScheduleRelationship::Scheduled
    } as i32);

    let mut vehicle_descriptor = gtfs_realtime::VehicleDescriptor::default();
    vehicle_descriptor.id = Some(train.train_num.clone());
    vehicle_descriptor.label = Some(train.train_id.clone());

    let stop_time_update = if train.status.canceled {
        Vec::new()
    } else {
        stop_pairs
            .iter()
            .map(|(mta_index, gtfs_index)| {
                let mta_stop = &train.details.stops[*mta_index];
                let gtfs_stop = &trip.stop_times[*gtfs_index];
                let skipped = mta_stop.local_cancel.unwrap_or(false)
                    && mta_stop.act_arrive_time.is_none()
                    && mta_stop.act_depart_time.is_none()
                    && mta_stop.act_time.is_none()
                    && mta_stop.stop_status.as_deref() != Some("DEPARTED");
                let mut update = gtfs_realtime::trip_update::StopTimeUpdate::default();
                update.stop_sequence = Some(gtfs_stop.stop_sequence);
                update.stop_id = Some(gtfs_stop.stop.id.clone());
                update.departure_occupancy_status = occupancy_status(&mta_stop.occupancy);

                if skipped {
                    update.schedule_relationship = Some(
                        gtfs_realtime::trip_update::stop_time_update::ScheduleRelationship::Skipped
                            as i32,
                    );
                } else {
                    let arrival_time = mta_stop
                        .act_arrive_time
                        .or(mta_stop.act_time)
                        .or(mta_stop.act_depart_time)
                        .unwrap_or(i64::from(mta_stop.sched_time));
                    let departure_time = mta_stop
                        .act_depart_time
                        .or(mta_stop.act_time)
                        .or(mta_stop.act_arrive_time)
                        .unwrap_or(i64::from(mta_stop.sched_time));
                    update.arrival = Some(stop_time_event(arrival_time));
                    update.departure = Some(stop_time_event(departure_time));
                }

                update
            })
            .collect()
    };

    let mut trip_update = gtfs_realtime::TripUpdate::default();
    trip_update.trip = trip_descriptor.clone();
    trip_update.vehicle = Some(vehicle_descriptor.clone());
    trip_update.stop_time_update = stop_time_update;
    trip_update.timestamp = u64::try_from(train.location.timestamp).ok();

    let (current_stop_sequence, stop_id, current_status) = current_stop(train, trip, &stop_pairs);
    let mut vehicle_position = gtfs_realtime::VehiclePosition::default();
    vehicle_position.trip = Some(trip_descriptor);
    vehicle_position.vehicle = Some(vehicle_descriptor);
    vehicle_position.position = Some(gtfs_realtime::Position {
        latitude: train.location.latitude,
        longitude: train.location.longitude,
        bearing: train.location.heading,
        odometer: None,
        speed: Some(train.location.speed.unwrap_or(0.0) * 0.44704),
    });
    vehicle_position.current_stop_sequence = current_stop_sequence;
    vehicle_position.stop_id = stop_id;
    vehicle_position.current_status = current_status;
    vehicle_position.timestamp = u64::try_from(train.location.timestamp).ok();

    let mut entity = gtfs_realtime::FeedEntity::default();
    entity.id = format!("fallback-{}-{}", railroad.as_str(), train.train_id);
    entity.trip_update = Some(trip_update);
    entity.vehicle = Some(vehicle_position);
    Some(entity)
}

fn stop_time_event(time: i64) -> gtfs_realtime::trip_update::StopTimeEvent {
    gtfs_realtime::trip_update::StopTimeEvent {
        delay: None,
        time: Some(time),
        uncertainty: Some(0),
        scheduled_time: None,
    }
}

fn current_stop(
    train: &MtaTrain,
    trip: &Trip,
    stop_pairs: &[(usize, usize)],
) -> (Option<u32>, Option<String>, Option<i32>) {
    stop_pairs
        .iter()
        .find_map(|(mta_index, gtfs_index)| {
            let mta_stop = &train.details.stops[*mta_index];
            if mta_stop.stop_status.as_deref() == Some("DEPARTED")
                || mta_stop.local_cancel.unwrap_or(false)
            {
                return None;
            }

            let gtfs_stop = &trip.stop_times[*gtfs_index];
            Some((
                Some(gtfs_stop.stop_sequence),
                Some(gtfs_stop.stop.id.clone()),
                vehicle_stop_status(mta_stop.stop_status.as_deref()),
            ))
        })
        .unwrap_or((None, None, None))
}

fn vehicle_stop_status(status: Option<&str>) -> Option<i32> {
    match status? {
        "STOPPED" | "STOPPED_AT" => {
            Some(gtfs_realtime::vehicle_position::VehicleStopStatus::StoppedAt as i32)
        }
        "INCOMING" | "INCOMING_AT" => {
            Some(gtfs_realtime::vehicle_position::VehicleStopStatus::IncomingAt as i32)
        }
        "EN_ROUTE" => Some(gtfs_realtime::vehicle_position::VehicleStopStatus::InTransitTo as i32),
        _ => None,
    }
}

fn occupancy_status(value: &str) -> Option<i32> {
    use gtfs_realtime::vehicle_position::OccupancyStatus;

    Some(match value {
        "EMPTY" => OccupancyStatus::Empty,
        "MANY_SEATS_AVAILABLE" => OccupancyStatus::ManySeatsAvailable,
        "FEW_SEATS_AVAILABLE" => OccupancyStatus::FewSeatsAvailable,
        "STANDING_ROOM_ONLY" => OccupancyStatus::StandingRoomOnly,
        "CRUSHED_STANDING_ROOM_ONLY" => OccupancyStatus::CrushedStandingRoomOnly,
        "FULL" => OccupancyStatus::Full,
        "NOT_ACCEPTING_PASSENGERS" => OccupancyStatus::NotAcceptingPassengers,
        "NOT_BOARDABLE" => OccupancyStatus::NotBoardable,
        "NO_DATA" | "NO_DATA_AVAILABLE" => OccupancyStatus::NoDataAvailable,
        _ => return None,
    } as i32)
}

fn lookup_tables(gtfs: &Gtfs) -> AHashMap<EcoString, BTreeSet<EcoString>> {
    let mut trip_name_to_ids: AHashMap<EcoString, BTreeSet<EcoString>> = AHashMap::new();

    for (trip_id, trip) in gtfs.trips.iter() {
        if let Some(short_name) = &trip.trip_short_name {
            trip_name_to_ids
                .entry(short_name.clone().into())
                .and_modify(|x| {
                    x.insert(trip_id.clone().into());
                })
                .or_insert(BTreeSet::from_iter([trip_id.clone().into()]));
        }
    }

    trip_name_to_ids
}

fn mnr_trip_id_fixer(gtfs: &Gtfs, input: gtfs_realtime::FeedMessage) -> gtfs_realtime::FeedMessage {
    let lookup_tables_made = lookup_tables(gtfs);

    let mut input = input;

    for entity in input.entity.iter_mut() {
        if let Some(trip_update) = &mut entity.trip_update {
            //start times are broken, let Catenary find the correct start time instead.
            trip_update.trip.start_time = None;

            if let Some(original_trip_id) = &mut trip_update.trip.trip_id {
                //checks if the trip id given by Metro North is actually invalid
                if !gtfs.trips.contains_key(original_trip_id) {
                    //it's invalid.

                    //get the operating trip short name, which is contained in the vehicle id of the gtfs field
                    if let Some(vehicle_data) = &mut entity.vehicle {
                        if let Some(vehicle) = &vehicle_data.vehicle {
                            if let Some(vehicle_id) = &vehicle.id {
                                if let Some(schedule_trip_ids_with_same_name) =
                                    lookup_tables_made.get(vehicle_id.as_str())
                                {
                                    //println!("MTA MNR metro north lookup {} found {} trips", vehicle_id, schedule_trip_ids_with_same_name.len());
                                    match schedule_trip_ids_with_same_name.len() {
                                        0 => {}
                                        1 => {
                                            *original_trip_id = schedule_trip_ids_with_same_name
                                                .iter()
                                                .nth(0)
                                                .unwrap()
                                                .to_string();

                                            println!(
                                                "Changed to {}",
                                                schedule_trip_ids_with_same_name
                                                    .iter()
                                                    .nth(0)
                                                    .unwrap()
                                                    .to_string()
                                            );

                                            if let Some(trip_vehicle_desc) = &mut vehicle_data.trip
                                            {
                                                trip_vehicle_desc.trip_id = Some(
                                                    schedule_trip_ids_with_same_name
                                                        .iter()
                                                        .nth(0)
                                                        .unwrap()
                                                        .to_string(),
                                                );
                                            }
                                        }
                                        _ => {
                                            let naive_date = trip_update
                                                .trip
                                                .start_date
                                                .as_ref()
                                                .map(|x| {
                                                    catenary::yyyymmdd_to_naive_date(x.as_str())
                                                        .ok()
                                                })
                                                .flatten();

                                            if let Some(naive_date) = naive_date {
                                                let trip_id_found =
                                                    schedule_trip_ids_with_same_name.iter().find(
                                                        |proposed_trip_id| {
                                                            if let Some(trip) = gtfs
                                                                .trips
                                                                .get(proposed_trip_id.as_str())
                                                            {
                                                                if let Some(calendar_dates) = gtfs
                                                                    .calendar_dates
                                                                    .get(trip.service_id.as_str())
                                                                {
                                                                    let naive_date_list =
                                                                        calendar_dates
                                                                            .iter()
                                                                            .map(|each_cal| {
                                                                                each_cal.date
                                                                            })
                                                                            .collect::<Vec<_>>();

                                                                    if naive_date_list
                                                                        .contains(&naive_date)
                                                                    {
                                                                        return true;
                                                                    }
                                                                }
                                                            }

                                                            false
                                                        },
                                                    );

                                                if let Some(trip_id_found) = trip_id_found {
                                                    *original_trip_id = trip_id_found.to_string();

                                                    if let Some(trip_vehicle_desc) =
                                                        &mut vehicle_data.trip
                                                    {
                                                        trip_vehicle_desc.trip_id =
                                                            Some(trip_id_found.to_string());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    input
}

fn convert(
    mta: &[MtaTrain],
    railroad: MtaRailroad,
    input_gtfs_trips: &gtfs_realtime::FeedMessage,
) -> Vec<gtfs_realtime::FeedEntity> {
    mta.iter()
        .filter(|train| train.railroad == railroad.as_str())
        .map(|train| {
            let supporting_gtfs = input_gtfs_trips
                .entity
                .iter()
                .find(|entity| entity_matches_train(entity, train, railroad));
            let supporting_vehicle = supporting_gtfs.and_then(|entity| entity.vehicle.as_ref());
            let trip = supporting_gtfs
                .and_then(|entity| entity.trip_update.as_ref())
                .map(|trip_update| trip_update.trip.clone())
                .or_else(|| supporting_vehicle.and_then(|vehicle| vehicle.trip.clone()));

            let mut vehicle_position = gtfs_realtime::VehiclePosition::default();
            vehicle_position.vehicle = supporting_vehicle
                .and_then(|vehicle| vehicle.vehicle.clone())
                .or_else(|| {
                    supporting_gtfs
                        .and_then(|entity| entity.trip_update.as_ref())
                        .and_then(|trip_update| trip_update.vehicle.clone())
                });
            vehicle_position.trip = trip;
            vehicle_position.position = Some(gtfs_realtime::Position {
                latitude: train.location.latitude,
                longitude: train.location.longitude,
                bearing: train.location.heading,
                odometer: None,
                speed: Some(train.location.speed.unwrap_or(0.0) * 0.44704),
            });
            vehicle_position.current_stop_sequence =
                supporting_vehicle.and_then(|vehicle| vehicle.current_stop_sequence);
            vehicle_position.stop_id =
                supporting_vehicle.and_then(|vehicle| vehicle.stop_id.clone());
            vehicle_position.current_status =
                supporting_vehicle.and_then(|vehicle| vehicle.current_status);
            vehicle_position.timestamp = u64::try_from(train.location.timestamp).ok();

            let mut entity = gtfs_realtime::FeedEntity::default();
            entity.id = train.train_id.clone();
            entity.vehicle = Some(vehicle_position);
            entity
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MtaRailroad {
    LIRR,
    MNR,
}

impl MtaRailroad {
    fn as_str(self) -> &'static str {
        match self {
            Self::LIRR => "LIRR",
            Self::MNR => "MNR",
        }
    }

    fn realtime_url(self) -> &'static str {
        match self {
            Self::LIRR => LIRR_TRIPS_FEED,
            Self::MNR => MNR_TRIPS_FEED,
        }
    }
}

pub async fn send_mta_rail_to_aspen(
    realtime_feed_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::RealtimeFeedMetadataEtcd>,
    >,
    railroad: MtaRailroad,
    vehicle_position: Vec<u8>,
    trip_updates: Vec<u8>,
    feed_id: &str,
) {
    let fetch_assigned_node_meta = realtime_feed_cache.get(feed_id);

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await;

        if let Err(e) = aspen_client {
            eprintln!("Failed to connect to Aspen at {}: {}", data.socket, e);
            return;
        }
        let aspen_client = aspen_client.unwrap();

        let tarpc_send_to_aspen = aspen_client
            .from_alpenrose(
                tarpc::context::current(),
                data.chateau_id.clone(),
                String::from(feed_id),
                Some(vehicle_position),
                Some(trip_updates),
                None,
                true,
                true,
                false,
                Some(200),
                Some(200),
                None,
                catenary::duration_since_unix_epoch().as_millis() as u64,
            )
            .await;

        match tarpc_send_to_aspen {
            Ok(_) => {
                println!(
                    "Successfully sent MTA commuter rail data sent to {}",
                    feed_id
                );
            }
            Err(e) => {
                eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custom_rt_feeds::mta::MNR_TRIPS_FEED;

    #[tokio::test]
    async fn test_mnr() {
        let gtfs = gtfs_structures::Gtfs::from_url_async(
            "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip",
        )
        .await
        .unwrap();

        let client = reqwest::Client::new();

        let download_gtfs_rt = get_mta_trips(&client, MNR_TRIPS_FEED).await.unwrap();

        let a = mnr_trip_id_fixer(&gtfs, download_gtfs_rt);

        for entity in a.entity {
            if let Some(trip) = entity.trip_update {
                println!("{:#?}", trip.trip.trip_id);
            }
        }
    }

    #[tokio::test]
    async fn test_lirr() {
        let client = reqwest::Client::new();
        let (converted, _) = get_and_convert_lirr(&client).await.unwrap();
        //println!("{:#?}", converted);

        for e in converted {
            if let Some(vehicle) = &e.vehicle {
                println!("vehicle's trip {:?}", vehicle.trip);
            }
        }
    }
}
