//! Transport only: the supplied runner is also used by the HTTP endpoint.
use actix::{Actor, ActorContext, AsyncContext, StreamHandler};
use actix_web::{Error, HttpRequest, HttpResponse, web};
use actix_web_actors::ws;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub type Runner = dyn Fn(
        Value,
        String,
        Arc<AtomicBool>,
        &mut dyn FnMut(Value) -> anyhow::Result<()>,
    ) -> anyhow::Result<Value>
    + Send
    + Sync;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Route {
        #[serde(rename = "requestId")]
        request_id: String,
        query: Value,
    },
    Cancel {
        #[serde(rename = "requestId")]
        request_id: String,
    },
}
struct Active {
    id: String,
    cancelled: Arc<AtomicBool>,
}
struct Output {
    id: String,
    value: Value,
    finished: bool,
}
struct Socket {
    runner: Arc<Runner>,
    active: Option<Active>,
    pending: Option<(String, Value)>,
    heartbeat: Instant,
    identities: HashMap<String, String>,
    sender: mpsc::Sender<Output>,
    receiver: Option<mpsc::Receiver<Output>>,
}

pub fn start(
    request: HttpRequest,
    payload: web::Payload,
    runner: Arc<Runner>,
) -> Result<HttpResponse, Error> {
    let (sender, receiver) = mpsc::channel(2);
    ws::WsResponseBuilder::new(
        Socket {
            runner,
            active: None,
            pending: None,
            heartbeat: Instant::now(),
            sender,
            receiver: Some(receiver),
            identities: HashMap::new(),
        },
        &request,
        payload,
    )
    .frame_size(64 * 1024)
    .start()
}

impl Socket {
    fn error(ctx: &mut ws::WebsocketContext<Self>, id: Option<&str>, message: &str) {
        ctx.text(json!({"type":"error", "requestId":id, "message":message}).to_string());
    }
    fn command(&mut self, command: Command, ctx: &mut ws::WebsocketContext<Self>) {
        match command {
            Command::Cancel { request_id } => {
                if let Some(active) = self.active.as_ref().filter(|a| a.id == request_id) {
                    active.cancelled.store(true, Ordering::Relaxed);
                    ctx.text(json!({"type":"cancelling","requestId":request_id}).to_string());
                } else if self
                    .pending
                    .as_ref()
                    .is_some_and(|(id, _)| id == &request_id)
                {
                    self.pending = None;
                    ctx.text(json!({"type":"cancelled","requestId":request_id}).to_string());
                } else {
                    Self::error(ctx, Some(&request_id), "request is not active");
                }
            }
            Command::Route { request_id, query } => {
                if request_id.is_empty() || request_id.len() > 128 {
                    Self::error(
                        ctx,
                        Some(&request_id),
                        "requestId must contain 1..128 bytes",
                    );
                    return;
                }
                let fingerprint = match super::pagination::digest(&query) {
                    Ok(value) => value,
                    Err(error) => {
                        Self::error(ctx, Some(&request_id), &error.to_string());
                        return;
                    }
                };
                if let Some(known) = self.identities.get(&request_id) {
                    if known != &fingerprint {
                        Self::error(
                            ctx,
                            Some(&request_id),
                            "requestId already identifies a different query",
                        );
                        return;
                    }
                } else if self.identities.len() >= 128 {
                    Self::error(
                        ctx,
                        Some(&request_id),
                        "request ID history is full; reconnect to start a new session",
                    );
                    return;
                }
                if let Some(active) = &self.active {
                    if active.id == request_id {
                        ctx.text(
                            json!({"type":"accepted","requestId":request_id,"duplicate":true})
                                .to_string(),
                        );
                    } else if let Some((id, queued_query)) = &self.pending {
                        if id == &request_id && queued_query == &query {
                            ctx.text(
                                json!({"type":"queued","requestId":request_id,"duplicate":true})
                                    .to_string(),
                            );
                        } else {
                            Self::error(
                                ctx,
                                Some(&request_id),
                                "one replacement request is already queued",
                            );
                        }
                    } else {
                        active.cancelled.store(true, Ordering::Relaxed);
                        ctx.text(json!({"type":"queued","requestId":request_id,"replacesRequestId":active.id}).to_string());
                        self.pending = Some((request_id, query));
                    }
                    return;
                }
                self.identities
                    .insert(request_id.clone(), fingerprint.clone());
                let cancelled = Arc::new(AtomicBool::new(false));
                self.active = Some(Active {
                    id: request_id.clone(),
                    cancelled: cancelled.clone(),
                });
                ctx.text(
                    json!({"type":"accepted","requestId":request_id,"duplicate":false}).to_string(),
                );
                let runner = self.runner.clone();
                let sender = self.sender.clone();
                actix_web::rt::spawn(async move {
                    let worker_id = request_id.clone();
                    let worker_sender = sender.clone();
                    let worker_cancel = cancelled.clone();
                    let result = web::block(move || {
                        let mut sequence = 0_u64;
                        runner(query, worker_id.clone(), worker_cancel, &mut |response| {
                            sequence += 1;
                            worker_sender.blocking_send(Output { id: worker_id.clone(), finished: false,
                                value: json!({"type":"snapshot","requestId":worker_id,"sequence":sequence,"response":response}) })
                                .map_err(|_| anyhow::anyhow!("websocket disconnected"))
                        })
                    }).await;
                    let value = if cancelled.load(Ordering::Relaxed) {
                        json!({"type":"cancelled","requestId":request_id})
                    } else {
                        match result {
                            Ok(Ok(response)) => {
                                json!({"type":"complete","requestId":request_id,"response":response})
                            }
                            Ok(Err(error)) => {
                                json!({"type":"error","requestId":request_id,"message":error.to_string()})
                            }
                            Err(error) => {
                                json!({"type":"error","requestId":request_id,"message":error.to_string()})
                            }
                        }
                    };
                    let _ = sender
                        .send(Output {
                            id: request_id,
                            value,
                            finished: true,
                        })
                        .await;
                });
            }
        }
    }
}
impl Actor for Socket {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.add_stream(ReceiverStream::new(self.receiver.take().unwrap()));
        ctx.run_interval(Duration::from_secs(15), |actor, ctx| {
            if actor.heartbeat.elapsed() > Duration::from_secs(45) {
                ctx.stop();
            } else {
                ctx.ping(b"edelweiss");
            }
        });
    }
    fn stopped(&mut self, _: &mut Self::Context) {
        if let Some(active) = &self.active {
            active.cancelled.store(true, Ordering::Relaxed);
        }
    }
}
impl StreamHandler<Output> for Socket {
    fn handle(&mut self, output: Output, ctx: &mut Self::Context) {
        if let Some(active) = self.active.as_ref().filter(|a| a.id == output.id) {
            if output.finished || !active.cancelled.load(Ordering::Relaxed) {
                ctx.text(output.value.to_string());
            }
            if output.finished {
                self.active = None;
                if let Some((request_id, query)) = self.pending.take() {
                    self.command(Command::Route { request_id, query }, ctx);
                }
            }
        }
    }
}
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Socket {
    fn handle(&mut self, message: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match message {
            Ok(ws::Message::Ping(bytes)) => {
                self.heartbeat = Instant::now();
                ctx.pong(&bytes);
            }
            Ok(ws::Message::Pong(_)) => {
                self.heartbeat = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                self.heartbeat = Instant::now();
                match serde_json::from_str(&text) {
                    Ok(command) => self.command(command, ctx),
                    Err(error) => Self::error(ctx, None, &error.to_string()),
                }
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => {
                ctx.close(None);
                ctx.stop();
            }
        }
    }
    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop();
    }
}
