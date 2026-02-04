// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0

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
    clippy::op_ref
)]

// https://en.wikipedia.org/wiki/Rhododendron_ferrugineum
use catenary::agency_secret::*;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use dashmap::DashMap;
use futures::prelude::*;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::RwLock;
use uuid::Uuid;
mod custom_rt_feeds;
pub mod get_feed_metadata;
mod leader_job;
use std::io;
use zip::ZipArchive;
mod single_fetch_time;
use crate::single_fetch_time::UrlType;
use bytes::Buf;
use catenary::bincode_deserialize;
use catenary::bincode_serialize;
use flixbus_gtfs_realtime::aggregator::Aggregator;
use flixbus_gtfs_realtime::client::FlixbusClient;
use get_feed_metadata::RealtimeFeedFetch;
use scc::HashMap as SccHashMap;
use std::io::prelude::*;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let request_limit = match std::env::var("REQUEST_LIMIT") {
        Ok(request_limit) => request_limit.parse::<usize>().unwrap(),
        Err(_) => 40,
    };

    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    let start = Instant::now();

    let amtrak_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let rtc_quebec_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let chicago_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let bridgeport_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let mnr_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let via_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let cta_bus_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>> = Arc::new(RwLock::new(None));
    let flixbus_us_aggregator: Arc<RwLock<Option<Aggregator>>> = Arc::new(RwLock::new(None));
    let flixbus_eu_aggregator: Arc<RwLock<Option<Aggregator>>> = Arc::new(RwLock::new(None));
    let chicago_trips_str: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));

    let mut downloads_started: HashSet<String> = HashSet::new();

    println!("Worker id {}", this_worker_id);

    let hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>> = Arc::new(SccHashMap::new());

    let too_many_requests_log: Arc<SccHashMap<String, std::time::Instant>> =
        Arc::new(SccHashMap::new());

    // last check time
    let last_check_time_ms: Option<u64> = None;
    let mut last_set_of_active_nodes_hash: Option<u64> = None;
    let mut last_updated_feeds_hash: Option<u64> = None;

    //connect to postgres
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    //let conn_pool = arc_conn_pool.as_ref();
    //let conn_pre = conn_pool.get().await;
    //let conn = &mut conn_pre?;

    let assignments_for_this_worker: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let mut previously_known_updated_ms_for_this_worker: Option<u64> = None;

    let last_fetch_per_feed: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

    //make client for reqwest
    //allow various compression algorithms to be used during the download process, as enabled in Cargo.toml
    let client = reqwest::ClientBuilder::new()
        //timeout queries
        .timeout(Duration::from_secs(8))
        .connect_timeout(Duration::from_secs(5))
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .cookie_store(true)
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // Spawn Genentech authentication task
    let auth_client = client.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = catenary::genentech_auth::authenticate_genentech(&auth_client).await {
                eprintln!("Failed to authenticate with Genentech: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
    });

    let etcd_urls_original =
        std::env::var("ETCD_URLS").unwrap_or_else(|_| "localhost:2379".to_string());
    let etcd_urls = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    let etcd_urls = Arc::new(etcd_urls);

    let etcd_username = std::env::var("ETCD_USERNAME");

    let etcd_password = std::env::var("ETCD_PASSWORD");

    let etcd_connection_options: Option<etcd_client::ConnectOptions> =
        match (etcd_username, etcd_password) {
            (Ok(username), Ok(password)) => {
                Some(etcd_client::ConnectOptions::new().with_user(username, password))
            }
            _ => None,
        };

    let arc_etcd_connection_options = Arc::new(etcd_connection_options.clone());

    let mut etcd =
        etcd_client::Client::connect(etcd_urls.as_ref(), etcd_connection_options.clone()).await?;

    println!("Connected to etcd");

    let etcd_lease_id: i64 = rand::rng().random_range(0..i64::MAX);

    let make_lease = etcd
        .lease_grant(
            //20 seconds
            20,
            Some(etcd_client::LeaseGrantOptions::new().with_id(etcd_lease_id)),
        )
        .await?;

    println!("etcd registered lease {}", etcd_lease_id);

    //create parent node for workers

    loop {
        //let is_online = online::tokio::check(Some(10)).await.is_ok();

        let ping_firefox = client
            .get("https://detectportal.firefox.com")
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        let is_online = match ping_firefox {
            Err(e) => false,
            Ok(resp) => match resp.bytes().await {
                Ok(data) => {
                    let string = String::from_utf8(data.as_ref().to_vec()).unwrap();

                    println!("Firefox check internet {}", string);

                    string.contains("success")
                }
                Err(e) => false,
            },
        };

        let mut etcd = etcd.clone();

        if is_online {
            //renew the etcd lease

            let _ = etcd.lease_keep_alive(etcd_lease_id).await?;

            // create this worker as an ephemeral node

            let etcd_this_worker_assignment = etcd
                .put(
                    format!("/alpenrose_workers/{}", this_worker_id).as_str(),
                    bincode_serialize(&etcd_lease_id).unwrap(),
                    Some(etcd_client::PutOptions::new().with_lease(etcd_lease_id)),
                )
                .await?;

            //each feed id ephemeral id contains the last time updated, with none meaning the data has not been assigned to the node yet

            let mut election_client = etcd.election_client();

            let current_leader_election = election_client.leader("/alpenrose_leader").await;

            match current_leader_election {
                Ok(current_leader_election) => {
                    let leader_kv = current_leader_election.kv();

                    match leader_kv {
                        None => {
                            let attempt_to_become_leader = election_client
                                .campaign(
                                    "/alpenrose_leader",
                                    bincode_serialize(this_worker_id.as_ref()).unwrap(),
                                    etcd_lease_id,
                                )
                                .await;

                            println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);
                        }
                        Some(leader_kv) => {
                            let leader_id: String = bincode_deserialize(leader_kv.value()).unwrap();

                            if &leader_id == this_worker_id.as_ref() {
                                // I AM THE LEADER!!!

                                println!("I AM THE LEADER!!!");

                                leader_job::perform_leader_job(
                                    &mut etcd,
                                    Arc::clone(&arc_conn_pool),
                                    &mut last_set_of_active_nodes_hash,
                                    &mut last_updated_feeds_hash,
                                )
                                .await?;
                            }
                        }
                    }
                }
                Err(leader_election_err) => {
                    let attempt_to_become_leader = election_client
                        .campaign(
                            "/alpenrose_leader",
                            bincode_serialize(this_worker_id.as_ref()).unwrap(),
                            etcd_lease_id,
                        )
                        .await;

                    println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);

                    eprintln!("{:#?}", leader_election_err);

                    //fetch again and see if leader

                    let current_leader_election = election_client.leader("/alpenrose_leader").await;

                    if let Ok(current_leader_resp) = current_leader_election {
                        if let Some(leader_kv) = current_leader_resp.kv() {
                            let leader_id: String = bincode_deserialize(leader_kv.value()).unwrap();

                            if &leader_id == this_worker_id.as_ref() {
                                // I AM THE LEADER!!!

                                println!("I AM THE LEADER on first try!!!");

                                leader_job::perform_leader_job(
                                    &mut etcd,
                                    Arc::clone(&arc_conn_pool),
                                    &mut last_set_of_active_nodes_hash,
                                    &mut last_updated_feeds_hash,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }

            //read from etcd to get the current assignments for this node

            let fetch_last_updated_assignments_for_this_worker_resp = etcd
                .get(
                    format!("/alpenrose_assignments_last_updated/{}", this_worker_id),
                    None,
                )
                .await?;

            let last_updated_worker_time_kv =
                fetch_last_updated_assignments_for_this_worker_resp.kvs();

            if let Some(last_updated_worker_time) = last_updated_worker_time_kv.first() {
                let last_updated_worker_time_value =
                    bincode_deserialize::<u64>(last_updated_worker_time.value()).unwrap();

                if Some(last_updated_worker_time_value)
                    != previously_known_updated_ms_for_this_worker
                {
                    previously_known_updated_ms_for_this_worker =
                        Some(last_updated_worker_time_value);

                    let mut assignments_for_this_worker_lock =
                        assignments_for_this_worker.write().await;

                    //fetch all the assignments

                    let prefix_search = format!("/alpenrose_assignments/{}/", this_worker_id);

                    let assignments = etcd
                        .get(
                            prefix_search.clone(),
                            Some(etcd_client::GetOptions::new().with_prefix()),
                        )
                        .await?
                        .take_kvs()
                        .into_iter()
                        .map(|each_kv| {
                            (
                                each_kv
                                    .key_str()
                                    .unwrap()
                                    .to_string()
                                    .replace(&prefix_search, ""),
                                bincode_deserialize::<RealtimeFeedFetch>(each_kv.value()).unwrap(),
                            )
                        })
                        .collect::<HashMap<String, RealtimeFeedFetch>>();

                    *assignments_for_this_worker_lock = assignments;
                }
            }

            // Spawn a background task to renew the etcd lease

            let mut etcd = etcd.clone();
            let lease_id = etcd_lease_id;

            let etcd_keep_alive_tokio = tokio::spawn(async move {
                match etcd.lease_keep_alive(lease_id).await {
                    Ok(_) => (),
                    Err(e) => eprintln!("Failed to renew lease: {}", e),
                }
            });

            //get the feed data from the feeds assigned to this worker

            {
                let assignments_guard = assignments_for_this_worker.read().await;
                let assigned_feeds: HashSet<String> = assignments_guard.keys().cloned().collect();
                drop(assignments_guard);

                // Amtrak
                if assigned_feeds.contains("f-amtrak~rt") && !downloads_started.contains("amtrak") {
                    println!("Spawning Amtrak download task...");
                    downloads_started.insert("amtrak".to_string());
                    let amtrak_gtfs = amtrak_gtfs.clone();
                    tokio::spawn(async move {
                        let gtfs = gtfs_structures::GtfsReader::default()
                            .read_shapes(false)
                            .read_from_url_async("https://github.com/catenarytransit/amtrak-schedule-fixer/releases/download/latest/amtrak-gtfs-noshapes-fixed.zip")
                            .await;

                        match gtfs {
                            Ok(gtfs) => {
                                println!("Amtrak GTFS downloaded.");
                                *amtrak_gtfs.write().await = Some(gtfs);
                            }
                            Err(e) => eprintln!("Failed to download Amtrak GTFS: {}", e),
                        }
                    });
                }

                // RTC Quebec
                if assigned_feeds.contains("f-rtcquebec~rt")
                    && !downloads_started.contains("rtc_quebec")
                {
                    println!("Spawning RTC Quebec read task...");
                    downloads_started.insert("rtc_quebec".to_string());
                    let rtc_quebec_gtfs = rtc_quebec_gtfs.clone();
                    tokio::spawn(async move {
                        let res = tokio::task::spawn_blocking(|| {
                            gtfs_structures::GtfsReader::default()
                                .read_shapes(false)
                                .read("rtcquebec.zip")
                        })
                        .await;

                        match res {
                            Ok(Ok(gtfs)) => {
                                println!("RTC Quebec GTFS loaded.");
                                *rtc_quebec_gtfs.write().await = Some(gtfs);
                            }
                            Ok(Err(e)) => eprintln!("Failed to read RTC Quebec GTFS: {}", e),
                            Err(e) => eprintln!("Join error RTC Quebec: {}", e),
                        }
                    });
                }

                // Bridgeport Transit
                if assigned_feeds.contains("f-dr7f-greaterbridgeporttransit~rt")
                    && !downloads_started.contains("bridgeport")
                {
                    println!("Spawning Bridgeport Transit download task...");
                    downloads_started.insert("bridgeport".to_string());
                    let bridgeport_gtfs = bridgeport_gtfs.clone();
                    tokio::spawn(async move {
                        let gtfs = gtfs_structures::GtfsReader::default()
                            .read_shapes(false)
                            .read_from_url_async(
                                "https://data.trilliumtransit.com/gtfs/gbt-ct-us/gbt-ct-us.zip",
                            )
                            .await;

                        match gtfs {
                            Ok(gtfs) => {
                                println!("Bridgeport GTFS downloaded.");
                                *bridgeport_gtfs.write().await = Some(gtfs);
                            }
                            Err(e) => {
                                eprintln!("Failed to download Bridgeport GTFS: {}", e)
                            }
                        }
                    });
                }

                // Chicago & MNR & Flixbus need the client
                let client_dl = client.clone();

                if assigned_feeds.contains("f-dp3-cta~rt") && !downloads_started.contains("chicago")
                {
                    println!("Spawning Chicago download task...");
                    downloads_started.insert("chicago".to_string());
                    let chicago_gtfs = chicago_gtfs.clone();
                    let chicago_trips_str = chicago_trips_str.clone();
                    let client_dl = client_dl.clone();
                    tokio::spawn(async move {
                        let url = "https://github.com/catenarytransit/pfaedled-gtfs-actions/releases/download/latest/cta_gtfs_railonly.zip";
                        println!("Downloading Chicago GTFS...");
                        match client_dl
                            .get(url)
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                match resp.bytes().await {
                                    Ok(bytes) => {
                                        println!("Chicago GTFS downloaded, parsing...");
                                        // Parse GTFS
                                        let gtfs = gtfs_structures::Gtfs::from_reader(
                                            io::Cursor::new(bytes.clone()),
                                        );
                                        if let Ok(gtfs) = gtfs {
                                            *chicago_gtfs.write().await = Some(gtfs);
                                        } else {
                                            eprintln!("Failed to parse Chicago GTFS");
                                        }

                                        // Parse trips.txt
                                        let mut archive = ZipArchive::new(io::Cursor::new(bytes));
                                        if let Ok(mut archive) = archive {
                                            if let Ok(mut trips_file) = archive.by_name("trips.txt")
                                            {
                                                let mut buffer = Vec::new();
                                                if io::copy(&mut trips_file, &mut buffer).is_ok() {
                                                    if let Ok(s) = String::from_utf8(buffer) {
                                                        *chicago_trips_str.write().await = Some(s);
                                                        println!("Chicago trips.txt loaded.");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Failed to get bytes for Chicago: {}", e),
                                }
                            }
                            Err(e) => eprintln!("Failed to download Chicago: {}", e),
                        }
                    });
                }

                // VIA Rail
                if assigned_feeds.contains("f-viarail~rt") && !downloads_started.contains("viarail")
                {
                    println!("Spawning VIA Rail download task...");
                    downloads_started.insert("viarail".to_string());
                    let via_gtfs = via_gtfs.clone();
                    tokio::spawn(async move {
                        let gtfs = gtfs_structures::GtfsReader::default()
                            .read_shapes(false)
                            .read_from_url_async(
                                "https://www.viarail.ca/sites/all/files/gtfs/viarail.zip",
                            )
                            .await;

                        match gtfs {
                            Ok(gtfs) => {
                                println!("VIA Rail GTFS downloaded.");
                                *via_gtfs.write().await = Some(gtfs);
                            }
                            Err(e) => eprintln!("Failed to download VIA Rail GTFS: {}", e),
                        }
                    });
                }

                // CTA Bus
                if assigned_feeds.contains("f-dp3-cta~bus~rt")
                    && !downloads_started.contains("cta_bus")
                {
                    println!("Spawning CTA Bus download task...");
                    downloads_started.insert("cta_bus".to_string());
                    let cta_bus_gtfs = cta_bus_gtfs.clone();
                    tokio::spawn(async move {
                        let gtfs = gtfs_structures::GtfsReader::default()
                            .read_shapes(false)
                            .read_from_url_async(
                                "https://www.transitchicago.com/downloads/sch_data/google_transit.zip",
                            )
                            .await;

                        match gtfs {
                            Ok(gtfs) => {
                                println!("CTA Bus GTFS downloaded.");
                                *cta_bus_gtfs.write().await = Some(gtfs);
                            }
                            Err(e) => eprintln!("Failed to download CTA Bus GTFS: {}", e),
                        }
                    });
                }

                // MNR
                if assigned_feeds.contains("f-mta~nyc~rt~mnr") && !downloads_started.contains("mnr")
                {
                    println!("Spawning MNR download task...");
                    downloads_started.insert("mnr".to_string());
                    let mnr_gtfs = mnr_gtfs.clone();
                    let client_dl = client_dl.clone();
                    tokio::spawn(async move {
                        let url = "https://github.com/catenarytransit/agency-gtfs-mirror/raw/refs/heads/main/gtfsmnr.zip";
                        println!("Downloading MNR GTFS...");
                        match client_dl
                            .get(url)
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                        {
                            Ok(resp) => match resp.bytes().await {
                                Ok(bytes) => {
                                    let gtfs =
                                        gtfs_structures::Gtfs::from_reader(io::Cursor::new(bytes));
                                    match gtfs {
                                        Ok(gtfs) => {
                                            println!("MNR GTFS loaded.");
                                            *mnr_gtfs.write().await = Some(gtfs);
                                        }
                                        Err(e) => eprintln!("Failed to parse MNR GTFS: {}", e),
                                    }
                                }
                                Err(e) => eprintln!("Failed to get bytes for MNR: {}", e),
                            },
                            Err(e) => eprintln!("Failed to download MNR: {}", e),
                        }
                    });
                }

                // Flixbus US
                if assigned_feeds.contains("f-flixbus~us~rt")
                    && !downloads_started.contains("flixbus_us")
                {
                    println!("Spawning Flixbus US download task...");
                    downloads_started.insert("flixbus_us".to_string());
                    let flixbus_us_aggregator = flixbus_us_aggregator.clone();
                    let client_dl = client_dl.clone();
                    tokio::spawn(async move {
                        let url = "http://gtfs.gis.flix.tech/gtfs_generic_us.zip";
                        println!("Downloading Flixbus US GTFS...");
                        match client_dl
                            .get(url)
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                        {
                            Ok(resp) => match resp.bytes().await {
                                Ok(bytes) => {
                                    let gtfs =
                                        gtfs_structures::Gtfs::from_reader(io::Cursor::new(bytes));
                                    match gtfs {
                                        Ok(gtfs) => {
                                            println!("Flixbus US GTFS loaded.");
                                            let client = FlixbusClient::new();
                                            let agg = Aggregator::new(client, gtfs);
                                            *flixbus_us_aggregator.write().await = Some(agg);
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to parse Flixbus US GTFS: {}", e)
                                        }
                                    }
                                }
                                Err(e) => eprintln!("Failed to get bytes for Flixbus US: {}", e),
                            },
                            Err(e) => eprintln!("Failed to download Flixbus US: {}", e),
                        }
                    });
                }

                // Flixbus EU
                if assigned_feeds.contains("f-flixbus~eu~rt")
                    && !downloads_started.contains("flixbus_eu")
                {
                    println!("Spawning Flixbus EU download task...");
                    downloads_started.insert("flixbus_eu".to_string());
                    let flixbus_eu_aggregator = flixbus_eu_aggregator.clone();
                    let client_dl = client_dl.clone();
                    tokio::spawn(async move {
                        let url = "http://gtfs.gis.flix.tech/gtfs_generic_eu.zip";
                        println!("Downloading Flixbus EU GTFS...");
                        match client_dl
                            .get(url)
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                        {
                            Ok(resp) => match resp.bytes().await {
                                Ok(bytes) => {
                                    let gtfs =
                                        gtfs_structures::Gtfs::from_reader(io::Cursor::new(bytes));
                                    match gtfs {
                                        Ok(gtfs) => {
                                            println!("Flixbus EU GTFS loaded.");
                                            let client = FlixbusClient::new();
                                            let agg = Aggregator::new(client, gtfs);
                                            *flixbus_eu_aggregator.write().await = Some(agg);
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to parse Flixbus EU GTFS: {}", e)
                                        }
                                    }
                                }
                                Err(e) => eprintln!("Failed to get bytes for Flixbus EU: {}", e),
                            },
                            Err(e) => eprintln!("Failed to download Flixbus EU: {}", e),
                        }
                    });
                }
            }

            single_fetch_time::single_fetch_time(
                request_limit,
                client.clone(),
                Arc::clone(&assignments_for_this_worker),
                Arc::clone(&last_fetch_per_feed),
                Arc::clone(&amtrak_gtfs),
                Arc::clone(&chicago_trips_str),
                Arc::clone(&chicago_gtfs),
                Arc::clone(&rtc_quebec_gtfs),
                Arc::clone(&bridgeport_gtfs),
                Arc::clone(&mnr_gtfs),
                Arc::clone(&via_gtfs),
                Arc::clone(&cta_bus_gtfs),
                Arc::clone(&flixbus_us_aggregator),
                Arc::clone(&flixbus_eu_aggregator),
                etcd_urls.clone(),
                etcd_connection_options.clone(),
                etcd_lease_id,
                hashes_of_data.clone(),
                too_many_requests_log.clone(),
            )
            .await?;

            //check if the worker is still alive

            let tokio_checker = tokio::join!(etcd_keep_alive_tokio);

            if let Err(e) = tokio_checker.0 {
                eprintln!("Error in tokio checker: {}", e);
                Err(e)?;
            }
        } else {
            println!("Not connected to the internet!");

            //revoke the lease

            let _ = etcd.lease_revoke(etcd_lease_id).await?;

            //end the program

            Err("No internet connection")?;
        }
    }
}
