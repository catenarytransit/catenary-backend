use actix::prelude::*;
use actix_web_actors::ws;
use catenary::trip_logic::{
    fetch_trip_information, fetch_trip_rt_update, GtfsRtRefreshData, QueryTripInformationParams,
    ResponseForGtfsRtRefresh, TripIntroductionInformation,
};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_INTERVAL: Duration = Duration::from_millis(300);

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "subscribe_trip")]
    SubscribeTrip {
        chateau: String,
        #[serde(flatten)]
        params: QueryTripInformationParams,
    },
    #[serde(rename = "unsubscribe_trip")]
    UnsubscribeTrip {
        chateau: String,
        #[serde(flatten)]
        params: QueryTripInformationParams,
    },
    #[serde(rename = "unsubscribe_all_trips")]
    UnsubscribeAllTrips,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "initial_trip")]
    InitialTrip { data: TripIntroductionInformation },
    #[serde(rename = "update_trip")]
    UpdateTrip { data: GtfsRtRefreshData },
    #[serde(rename = "error")]
    Error { message: String },
}

pub struct TripWebSocket {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,
}

impl TripWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
    ) -> Self {
        Self {
            pool,
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            subscriptions: HashMap::new(),
            hb: Instant::now(),
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn start_periodic_updates(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(UPDATE_INTERVAL, |act, ctx| {
            for ((chateau, params), _) in act.subscriptions.iter_mut() {
                let fs = fetch_trip_rt_update(
                    chateau.clone(),
                    params.clone(),
                    act.etcd_connection_ips.clone(),
                    act.etcd_connection_options.clone(),
                    act.aspen_client_manager.clone(),
                );
                
                let params_clone = params.clone();
                let chateau_clone = chateau.clone();

                let fut = async move {
                    fs.await
                };

                let fut = actix::fut::wrap_future(fut)
                    .map(move |result, act: &mut TripWebSocket, ctx: &mut ws::WebsocketContext<Self>| {
                        match result {
                            Ok(response) => {
                                if response.found_data {
                                    if let Some(data) = response.data {
                                         if let Some(ts) = data.timestamp {
                                             let key = (chateau_clone.clone(), params_clone.clone());
                                             if let Some(current_last_update) = act.subscriptions.get_mut(&key) {
                                                  if let Some(last) = current_last_update {
                                                      if *last == ts {
                                                          return;
                                                      }
                                                  }
                                                  *current_last_update = Some(ts);
                                             }
                                         }
                                         
                                         let msg = ServerMessage::UpdateTrip { data };
                                         let text = serde_json::to_string(&msg).unwrap();
                                         ctx.text(text);
                                    }
                                }
                            }
                            Err(e) => {
                                // Optionally send error or log it. 
                                // Sending repeated errors might be noisy.
                                // eprintln!("Error fetching update: {}", e);
                            }
                        }
                    });
                
                ctx.spawn(fut);
            }
        });
    }
}

impl Actor for TripWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.start_periodic_updates(ctx);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for TripWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                let msg: Result<ClientMessage, _> = serde_json::from_str(&text);
                match msg {
                    Ok(ClientMessage::UnsubscribeTrip { chateau, params }) => {
                        self.subscriptions.remove(&(chateau, params));
                    }
                    Ok(ClientMessage::UnsubscribeAllTrips) => {
                        self.subscriptions.clear();
                    }
                    Ok(ClientMessage::SubscribeTrip { chateau, params }) => {
                        self.subscriptions.insert((chateau.clone(), params.clone()), None);
                        
                        // Fetch initial data immediately
                        let fs = fetch_trip_information(
                            chateau,
                            params,
                            self.pool.clone(),
                            self.etcd_connection_ips.clone(),
                            self.etcd_connection_options.clone(),
                            self.aspen_client_manager.clone(),
                            None,
                        );

                        let fut = async move {
                            fs.await
                        };
                        
                        let fut = actix::fut::wrap_future(fut)
                            .map(|result, _, ctx: &mut ws::WebsocketContext<Self>| {
                                 match result {
                                     Ok(data) => {
                                         let msg = ServerMessage::InitialTrip { data };
                                         let text = serde_json::to_string(&msg).unwrap();
                                         ctx.text(text);
                                     }
                                     Err(e) => {
                                         let msg = ServerMessage::Error { message: e };
                                         let text = serde_json::to_string(&msg).unwrap();
                                         ctx.text(text);
                                     }
                                 }
                            });
                        ctx.spawn(fut);
                    }
                    Err(e) => {
                         let msg = ServerMessage::Error { message: format!("Invalid message: {}", e) };
                         ctx.text(serde_json::to_string(&msg).unwrap());
                    }
                }
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(_reason)) => {
                ctx.stop();
            }
            _ => (),
        }
    }
}
