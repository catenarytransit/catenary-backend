use std::env;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

/// This is the service definition. It looks a lot like a trait definition.
/// It defines one RPC, hello, which takes one arg, name, and returns a String.
#[tarpc::service]
pub trait AspenRpc {
    /// Returns a greeting for name.
    async fn hello(name: String) -> String;

    //maybesend gtfs rt? 
    async fn new_rt_kactus(realtime_feed_id: String, data: Vec<u8>) -> bool;
}
