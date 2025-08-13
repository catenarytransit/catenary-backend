sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install libprotoc-dev protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql postgresql-17 postgresql-17-postgis postgresql-contrib unzip wget cmake openssl libpq-dev

## update and install some things we should probably have
apt-get update
apt-get install -y \
  curl \
  git \
  gnupg2 \
  jq \
  sudo \
  vim \
  build-essential \
  openssl

## Install rustup and common components
curl https://sh.rustup.rs -sSf | sh -s -- -y 
rustup component add rustfmt
rustup component add clippy 

cargo install cargo-expand
cargo install cargo-edit
