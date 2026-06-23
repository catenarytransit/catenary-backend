use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use std::sync::Arc;

pub use catenary::pasque::lib::TrajectorySubscriptionParams;
pub use catenary::pasque::lib::TrajectoryWrapper;

pub async fn get_trajectories(
    _pool: Arc<CatenaryPostgresPool>,
    _etcd_connection_ips: Arc<EtcdConnectionIps>,
    _etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    _aspen_client_manager: Arc<AspenClientManager>,
    _etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    params: TrajectorySubscriptionParams,
) -> Result<Vec<TrajectoryWrapper>, String> {
    let addr: std::net::SocketAddr = "127.0.0.1:52775"
        .parse()
        .map_err(|e| format!("Invalid Pasque address: {:?}", e))?;
    
    let client = catenary::pasque::lib::spawn_pasque_client_from_ip(&addr)
        .await
        .map_err(|e| format!("Failed to connect to Pasque: {:?}", e))?;

    let result = client
        .get_trajectories(tarpc::context::current(), params)
        .await
        .map_err(|e| format!("Pasque RPC error: {:?}", e))?;

    result
}
