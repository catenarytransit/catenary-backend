//! Response body byte counts shared by Actix HTTP services.

use actix_web::{
    Error,
    body::{BodySize, MessageBody},
    dev::{ServiceRequest, ServiceResponse},
    http::{StatusCode, header::HeaderValue},
    middleware::Next,
};

/// Serialized payload byte count before dynamically applied HTTP compression.
pub const RESPONSE_SIZE_HEADER: &str = "x-response-size-bytes";

/// Add the serialized response body's byte count without reading or copying it.
///
/// Install with `middleware::from_fn(response_size)` before registering `Compress`
/// so the count describes the body before Actix's HTTP compression. Bodies that
/// are already encoded (e.g. gzip tiles) are measured in that encoded form.
/// Unknown-length streams are left streaming and do not receive a size header.
/// This is payload size, excluding HTTP headers and framing, not wire size.
pub async fn response_size<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let mut response = next.call(req).await?;

    // Never propagate a stale size supplied by a handler or upstream proxy.
    response.headers_mut().remove(RESPONSE_SIZE_HEADER);
    let status = response.status();
    if status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::RESET_CONTENT
        || status == StatusCode::NOT_MODIFIED
    {
        return Ok(response);
    }

    let size = match response.response().body().size() {
        BodySize::Sized(size) => size,
        BodySize::None => 0,
        BodySize::Stream => return Ok(response),
    };
    response.headers_mut().insert(
        actix_web::http::header::HeaderName::from_static(RESPONSE_SIZE_HEADER),
        HeaderValue::from_str(&size.to_string()).expect("a byte count is a valid header value"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, HttpResponse, http::header, middleware, test, web};

    #[actix_web::test]
    async fn counts_utf8_and_binary_without_changing_bodies() {
        let app = test::init_service(
            App::new()
                .wrap(middleware::from_fn(response_size))
                .route(
                    "/ron",
                    web::get().to(|| async { HttpResponse::Ok().body("é🚆") }),
                )
                .route("/pb", web::get().to(|| async {
                    HttpResponse::Ok()
                        .insert_header((RESPONSE_SIZE_HEADER, "999"))
                        .body(vec![0u8, 255, 128])
                })),
        )
        .await;
        for (path, expected) in [("/ron", "é🚆".as_bytes()), ("/pb", &[0u8, 255, 128][..])] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(path).to_request()).await;
            assert_eq!(response.headers()[RESPONSE_SIZE_HEADER], expected.len().to_string());
            assert_eq!(test::read_body(response).await.as_ref(), expected);
        }
    }

    #[actix_web::test]
    async fn survives_compression_and_is_exposed_to_browsers() {
        let payload = "nearby departures ".repeat(1024);
        let expected_size = payload.len();
        let app = test::init_service(
            App::new()
                .wrap(middleware::from_fn(response_size))
                .wrap(
                    actix_cors::Cors::default()
                        .allow_any_origin()
                        .expose_headers([RESPONSE_SIZE_HEADER]),
                )
                .wrap(middleware::Compress::default())
                .route("/", web::get().to(move || {
                    let payload = payload.clone();
                    async move { HttpResponse::Ok().body(payload) }
                })),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .insert_header((header::ORIGIN, "https://maps.catenarymaps.org"))
                .insert_header((header::ACCEPT_ENCODING, "gzip"))
                .to_request(),
        )
        .await;
        assert_eq!(response.headers()[RESPONSE_SIZE_HEADER], expected_size.to_string());
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        assert!(response.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS]
            .to_str().unwrap().contains(RESPONSE_SIZE_HEADER));
        assert!(test::read_body(response).await.len() < expected_size);
    }

    #[actix_web::test]
    async fn omits_unknown_stream_size_and_bodyless_statuses() {
        let app = test::init_service(
            App::new()
                .wrap(middleware::from_fn(response_size))
                .route("/stream", web::get().to(|| async {
                    HttpResponse::Ok()
                        .insert_header((RESPONSE_SIZE_HEADER, "999"))
                        .streaming(futures_util::stream::once(async {
                            Ok::<_, Error>(web::Bytes::from_static(b"streamed"))
                        }))
                }))
                .route("/empty", web::get().to(|| async { HttpResponse::Ok().finish() }))
                .route("/304", web::get().to(|| async {
                    HttpResponse::NotModified()
                        .insert_header((RESPONSE_SIZE_HEADER, "999"))
                        .finish()
                }))
                .route("/204", web::get().to(|| async { HttpResponse::NoContent().finish() })),
        )
        .await;
        for path in ["/stream", "/304", "/204"] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(path).to_request()).await;
            assert!(!response.headers().contains_key(RESPONSE_SIZE_HEADER));
            if path == "/stream" {
                assert_eq!(test::read_body(response).await.as_ref(), b"streamed");
            }
        }
        let response =
            test::call_service(&app, test::TestRequest::get().uri("/empty").to_request()).await;
        assert_eq!(response.headers()[RESPONSE_SIZE_HEADER], "0");
    }
}
