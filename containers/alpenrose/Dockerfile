FROM debian:latest

# Install Rust using rustup

RUN apt-get update && apt-get install -y curl
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup default stable

# Install the necessary dependencies

RUN apt install -y protobuf-compiler build-essential gcc pkg-config libssl-dev unzip wget libpq-dev
RUN cargo install diesel_cli --no-default-features --features postgres

WORKDIR /catenary

# Copy the source code into the container

COPY ./../.. .

RUN cargo build --release --bin alpenrose

RUN mkdir output-binaries/

RUN cp ./target/release/alpenrose ./output-binaries/

RUN rm -rf ./target/

CMD ["./output-binaries/alpenrose"]