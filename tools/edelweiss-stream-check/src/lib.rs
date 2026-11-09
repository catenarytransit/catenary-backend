//! Compile and exercise production pagination and WebSocket transport without
//! OSR graphs, PostgreSQL, or the monolithic GTFS-import dependencies.
#[path = "../../../src/edelweiss/pagination.rs"]
pub mod pagination;
#[path = "../../../src/edelweiss/websocket.rs"]
pub mod websocket;

#[cfg(test)]
mod transport_tests {
    use super::*;
    use actix_web::{App, HttpServer, web};
    use actix_web_actors::ws;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    macro_rules! receive {
        ($socket:expr) => {{
            let frame = actix_web::rt::time::timeout(Duration::from_secs(3), $socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match frame {
                ws::Frame::Text(bytes) => serde_json::from_slice::<Value>(&bytes).unwrap(),
                other => panic!("expected JSON text, got {other:?}"),
            }
        }};
    }

    #[actix_web::test]
    async fn live_socket_streams_before_completion_correlates_ids_and_cancels() {
        let released = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_release = released.clone();
        let worker_calls = calls.clone();
        let runner: Arc<websocket::Runner> = Arc::new(move |_, _, cancelled, observer| {
            worker_calls.fetch_add(1, Ordering::Relaxed);
            observer(json!({"itineraries":[{"id":"first"}]}))?;
            let end = Instant::now() + Duration::from_secs(3);
            while !worker_release.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed) {
                anyhow::ensure!(Instant::now() < end, "test worker timed out");
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(json!({"itineraries":[{"id":"first"},{"id":"second"}]}))
        });
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = HttpServer::new(move || {
            App::new().app_data(web::Data::new(runner.clone())).route(
                "/ws",
                web::get().to(
                    |request, payload, runner: web::Data<Arc<websocket::Runner>>| async move {
                        websocket::start(request, payload, runner.get_ref().clone())
                    },
                ),
            )
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);
        let (_, mut socket) = awc::Client::new()
            .ws(format!("http://{address}/ws"))
            .connect()
            .await
            .unwrap();
        let route = json!({"type":"route","requestId":"q1","query":{"from":"a"}});
        socket
            .send(ws::Message::Text(route.to_string().into()))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "accepted");
        let snapshot = receive!(socket);
        assert_eq!(snapshot["type"], "snapshot");
        assert_eq!(snapshot["requestId"], "q1");
        assert_eq!(snapshot["sequence"], 1);
        assert!(!released.load(Ordering::Relaxed)); // arrived while worker is still searching
        socket
            .send(ws::Message::Text(route.to_string().into()))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["duplicate"], true);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q1","query":{"from":"b"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "error");
        socket
            .send(ws::Message::Text(
                json!({"type":"cancel","requestId":"q1"}).to_string().into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "cancelling");
        assert_eq!(receive!(socket)["type"], "cancelled");
        released.store(true, Ordering::Relaxed);
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q2","query":{"from":"a"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["requestId"], "q2");
        assert_eq!(receive!(socket)["type"], "snapshot");
        let complete = receive!(socket);
        assert_eq!(complete["type"], "complete");
        assert_eq!(complete["requestId"], "q2");
        assert_eq!(
            complete["response"]["itineraries"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        // A scroll/new query can replace a still-running request. It starts
        // only after the old worker exits, keeping one RAPTOR state per socket.
        released.store(false, Ordering::Relaxed);
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q3","query":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "accepted");
        assert_eq!(receive!(socket)["type"], "snapshot");
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q4","query":{"page":2}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "queued");
        let cancelled = receive!(socket);
        assert_eq!(cancelled["type"], "cancelled");
        assert_eq!(cancelled["requestId"], "q3");
        assert_eq!(receive!(socket)["requestId"], "q4");
        assert_eq!(receive!(socket)["type"], "snapshot");
        released.store(true, Ordering::Relaxed);
        assert_eq!(receive!(socket)["type"], "complete");
        // Completed identities cannot be reused for a different page/query.
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q1","query":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "error");
        drop(socket);
        handle.stop(true).await;
    }

    #[actix_web::test]
    async fn disconnect_releases_active_worker() {
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = stopped.clone();
        let runner: Arc<websocket::Runner> = Arc::new(move |_, _, cancelled, observer| {
            observer(json!({"itineraries":[]}))?;
            let end = Instant::now() + Duration::from_secs(3);
            while !cancelled.load(Ordering::Relaxed) {
                anyhow::ensure!(Instant::now() < end, "disconnect did not cancel search");
                std::thread::sleep(Duration::from_millis(1));
            }
            worker_stopped.store(true, Ordering::Relaxed);
            Ok(json!({}))
        });
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = HttpServer::new(move || {
            App::new().app_data(web::Data::new(runner.clone())).route(
                "/ws",
                web::get().to(
                    |request, payload, runner: web::Data<Arc<websocket::Runner>>| async move {
                        websocket::start(request, payload, runner.get_ref().clone())
                    },
                ),
            )
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);
        let (_, mut socket) = awc::Client::new()
            .ws(format!("http://{address}/ws"))
            .connect()
            .await
            .unwrap();
        socket
            .send(ws::Message::Text(
                json!({"type":"route","requestId":"q","query":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive!(socket)["type"], "accepted");
        assert_eq!(receive!(socket)["type"], "snapshot");
        drop(socket);
        actix_web::rt::time::timeout(Duration::from_secs(3), async {
            while !stopped.load(Ordering::Relaxed) {
                actix_web::rt::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        handle.stop(true).await;
    }
}
