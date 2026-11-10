use super::*;
use catenary::nearby_departures::{NearbyDeparturesV3Response, NearbyFromCoordsV3, RouteSelection, prepare_nearby};

#[derive(Message)]
#[rtype(result = "()")]
pub struct NearbyFrame {
    generation: u64,
    request_id: String,
    phase: &'static str,
    complete: bool,
    result: Result<NearbyDeparturesV3Response, String>,
}

impl Handler<NearbyFrame> for RamondaWebSocket {
    type Result = ();
    fn handle(&mut self, frame: NearbyFrame, ctx: &mut Self::Context) {
        if frame.generation != self.nearby_generation { return; }
        if frame.complete {
            self.nearby_fetch = None;
            self.nearby_loaded |= frame.result.is_ok();
        }
        let message = match frame.result {
            Ok(data) => serde_json::json!({
                "type": "nearby_departures", "request_id": frame.request_id,
                "phase": frame.phase, "complete": frame.complete, "data": data,
            }),
            Err(error) => serde_json::json!({
                "type": "nearby_departures_error", "request_id": frame.request_id,
                "message": error,
            }),
        };
        ctx.text(message.to_string());
    }
}

impl RamondaWebSocket {
    pub fn cancel_nearby(&mut self, ctx: &mut ws::WebsocketContext<Self>) {
        self.nearby_generation = self.nearby_generation.wrapping_add(1);
        if let Some(handle) = self.nearby_fetch.take() { ctx.cancel_future(handle); }
    }

    pub fn fetch_nearby(&mut self, request_id: String, params: NearbyFromCoordsV3,
        progressive: Option<bool>, ctx: &mut ws::WebsocketContext<Self>) {
        self.cancel_nearby(ctx);
        let generation = self.nearby_generation;
        let pool = self.pool.clone();
        let cache = self.aspen_chateau_cache.clone();
        let address = ctx.address();
        let progressive = progressive.unwrap_or(!self.nearby_loaded);
        let future = async move {
            let result = async {
                if request_id.is_empty() || request_id.len() > 128 {
                    return Err("request_id must contain 1 to 128 bytes".into());
                }
                let prepared = prepare_nearby(params, pool.clone()).await?;
                if progressive {
                    let routes = prepared.priority_selection(pool.clone()).await?;
                    let data = prepared.load(pool.clone(), cache.clone(), RouteSelection::Only(routes.clone())).await?;
                    address.do_send(NearbyFrame { generation, request_id: request_id.clone(),
                        phase: "priority", complete: false, result: Ok(data) });
                    prepared.load(pool, cache, RouteSelection::Except(routes)).await
                } else {
                    prepared.load(pool, cache, RouteSelection::All).await
                }
            }.await;
            address.do_send(NearbyFrame { generation, request_id,
                phase: if progressive { "remaining" } else { "full" }, complete: true, result });
        };
        self.nearby_fetch = Some(ctx.spawn(actix::fut::wrap_future(future).map(|_, _, _| ())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearby_request_parameters_do_not_interfere_with_envelope_fields() {
        let message: ClientMessage = serde_json::from_str(r#"{
            "type":"nearby_departures", "request_id":"r1", "progressive":false,
            "lat":34.05, "lon":-118.25, "departure_time":1794355200
        }"#).unwrap();
        match message {
            ClientMessage::NearbyDepartures { request_id, params, progressive } => {
                assert_eq!(request_id, "r1");
                assert_eq!(progressive, Some(false));
                assert_eq!(params.departure_time, Some(1794355200));
                assert!(params.validate().is_ok());
            }
            _ => panic!("wrong request type"),
        }
        let message: ClientMessage = serde_json::from_str(r#"{
            "type":"nearby_departures", "request_id":"r2", "lat":34.05, "lon":-118.25
        }"#).unwrap();
        assert!(matches!(message, ClientMessage::NearbyDepartures { progressive: None, .. }));
        assert!(matches!(serde_json::from_str::<ClientMessage>(r#"{"type":"cancel_nearby_departures"}"#).unwrap(),
            ClientMessage::CancelNearbyDepartures));
    }
}
