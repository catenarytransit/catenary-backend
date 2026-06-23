use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TrajectorySubscriptionParams {
    pub bbox: Vec<f64>,
    pub zoom: u8,
    pub modes: Vec<String>,
    #[serde(default)]
    pub client_reference: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TrajectoryWrapper {
    pub source: String,
    pub timestamp: u64,
    pub client_reference: String,
    pub content: serde_json::Value,
}

#[tarpc::service]
pub trait PasqueRpc {
    async fn get_trajectories(params: TrajectorySubscriptionParams) -> Result<Vec<TrajectoryWrapper>, String>;
}

pub async fn spawn_pasque_client_from_ip(
    addr: &std::net::SocketAddr,
) -> Result<PasqueRpcClient, Box<dyn std::error::Error + Sync + Send>> {
    let mut transport_builder = tarpc::serde_transport::tcp::connect(addr, tarpc::tokio_serde::formats::Bincode::default);
    transport_builder
        .config_mut()
        .max_frame_length(1024 * 1024 * 128);
    let transport = transport_builder.await?;
    Ok(PasqueRpcClient::new(tarpc::client::Config::default(), transport).spawn())
}
