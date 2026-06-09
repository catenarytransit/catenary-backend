use std::sync::Arc;

use rand::Rng;

mod leader_thread;
use leader_thread::aspen_leader_thread;
use uuid::Uuid;

use catenary::catenaryconfig;
use catenary::postgres_tools::*;

use catenary::aspen::lib::ChateauxLeaderHashMap;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let aspenleader_config = &catenaryconfig::config().aspenleader;

    // Worker Id for this instance of Aspen
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());
    let etcd_username = std::env::var("ETCD_USERNAME")
        .ok()
        .or_else(|| aspenleader_config.etcd_username.clone());

    let etcd_password = std::env::var("ETCD_PASSWORD")
        .ok()
        .or_else(|| aspenleader_config.etcd_password.clone());
    let etcd_lease_id_for_this_worker: i64 = rand::rng().random_range(0..i64::MAX);
    let etcd_connect_options: Option<etcd_client::ConnectOptions> =
        match (etcd_username, etcd_password) {
            (Some(username), Some(password)) => {
                Some(etcd_client::ConnectOptions::new().with_user(username, password))
            }
            _ => None,
        };

    let etcd_urls_original = std::env::var("ETCD_URLS")
        .ok()
        .or_else(|| {
            aspenleader_config
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
        .unwrap_or_else(|| "localhost:2379".to_string());
    let etcd_urls = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    let etcd_addresses = Arc::new(etcd_urls);

    let arc_etcd_connect_options = Arc::new(etcd_connect_options.clone());
    let chateau_list: Arc<Mutex<Option<ChateauxLeaderHashMap>>> = Arc::new(Mutex::new(None));

    let chateau_list_for_leader_thread = Arc::clone(&chateau_list);

    //connect to postgres
    println!("Connecting to postgres");
    let conn_pool: CatenaryPostgresPool = make_async_pool().await.unwrap();
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);
    println!("Connected to postgres");

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    aspen_leader_thread(
        workers_nodes,
        chateau_list_for_leader_thread,
        this_worker_id,
        arc_conn_pool,
        Arc::clone(&etcd_addresses),
        arc_etcd_connect_options,
        etcd_lease_id_for_this_worker,
    )
    .await
    .unwrap();
}
