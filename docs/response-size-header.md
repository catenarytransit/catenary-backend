# Response size header

Birch adds `X-Response-Size-Bytes` to responses with known body lengths across
all routes, including GTFS-RT protobuf/JSON/RON, nearby departures, stop
departures, realtime vehicles, alerts, shapes, and tiles.

The value is the serialized body length in bytes **before Actix's HTTP
compression**, excluding HTTP headers and framing. UTF-8 is counted in bytes.
Already encoded bodies (such as precompressed tiles) are counted as provided
by the handler; the middleware does not decompress them. This is neither the
in-memory dataset size nor the final compressed network transfer size.

Unknown-length streams omit the header to avoid buffering the response.
Informational responses and 204/205/304 responses omit it as well. Ordinary
empty responses report zero. HEAD responses use the body size supplied by the
handler, although Actix does not transmit that body.

The header is exposed through CORS, allowing browser clients to read it with
`response.headers.get('X-Response-Size-Bytes')`. Standard `Content-Length`
remains managed by Actix and may be absent when compression streams the body.

Other Actix services can reuse the same function:

```rust,ignore
App::new()
    .wrap(middleware::from_fn(catenary::response_size::response_size))
    .wrap(middleware::Compress::default())
```

Register the size middleware before `Compress`: Actix processes response
wrappers in registration order. Add `RESPONSE_SIZE_HEADER` to that service's
CORS exposed headers when browser clients need access.
