FROM rust:1.83-slim-bookworm as builder
WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y pkg-config libssl-dev protobuf-compiler cmake build-essential
RUN cargo build --release --bin edelweiss

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/edelweiss /app/edelweiss
CMD ["/app/edelweiss"]
