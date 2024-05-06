use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::str::FromStr;
use tarpc::{
    context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Bincode,
};

#[tokio::main]
async fn main() {
    //fd7a:115c:a1e0::4

    let socket_addr = SocketAddr::new(
        IpAddr::V6(Ipv6Addr::from_str("fd7a:115c:a1e0::4").unwrap()),
        40427,
    );

    println!("Will connect to {:?}", socket_addr);

    let transport = tarpc::serde_transport::tcp::connect(socket_addr, Bincode::default)
        .await
        .expect("Failed to connect to Aspen");

    let aspen_client =
        catenary::aspen::lib::AspenRpcClient::new(tarpc::client::Config::default(), transport)
            .spawn();

    //send hello world
    let tarpc_send_to_aspen = aspen_client
        .hello(tarpc::context::current(), "Kyler".to_string())
        .await;

    match tarpc_send_to_aspen {
        Ok(response) => {
            println!("Response from Aspen: {}", response);
        }
        Err(e) => {
            println!("Error from Aspen: {}", e);
        }
    }
}
