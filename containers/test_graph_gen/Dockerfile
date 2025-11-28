FROM debian:bookworm

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    cmake \
    git \
    pkg-config \
    libssl-dev \
    libpq-dev \
    protobuf-compiler \
    zlib1g-dev \
    libbz2-dev \
    libexpat1-dev \
    libboost-all-dev \
    unzip \
    wget \
    jq \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Diesel CLI
RUN cargo install diesel_cli --no-default-features --features postgres

# Build pfaedle
WORKDIR /tmp
RUN git clone https://github.com/ad-freiburg/pfaedle.git
WORKDIR /tmp/pfaedle
RUN mkdir build && cd build && cmake .. && make -j$(nproc) && make install

# Clone transitland-atlas
WORKDIR /
RUN git clone https://github.com/catenaryTransit/transitland-atlas.git /dmfr_source

# Setup workspace
WORKDIR /catenary

# Copy source code
COPY . .

# Build binaries
RUN cargo build --release --bin maple --bin avens --bin gentian

# Create output directory for binaries
RUN mkdir -p /catenary/output-binaries
RUN cp target/release/maple /catenary/output-binaries/
RUN cp target/release/avens /catenary/output-binaries/
RUN cp target/release/gentian /catenary/output-binaries/

# Clean up build artifacts to save space (optional, but good practice)
# RUN cargo clean

# Copy scripts
COPY containers/test_graph_gen/run_test.sh /catenary/run_test.sh
RUN chmod +x /catenary/run_test.sh

CMD ["/catenary/run_test.sh"]
