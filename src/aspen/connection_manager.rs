use crate::aspen::lib::AspenRpcClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AspenClientManager {
    pub clients: Arc<RwLock<HashMap<SocketAddr, AspenRpcClient>>>,
}

impl AspenClientManager {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_client(&self, addr: SocketAddr) -> Option<AspenRpcClient> {
        let clients = self.clients.read().await;
        clients.get(&addr).cloned()
    }

    pub async fn insert_client(&self, addr: SocketAddr, client: AspenRpcClient) {
        let mut clients = self.clients.write().await;
        clients.insert(addr, client);
    }
}
