## Install Dependencies

```bash
sudo apt install protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql unzip
```

### Running gtfs static ingest

```bash
cargo run --bin schedule_ingest -- --postgres "host=localhost user=postgres password=correcthorsebatterystaple"
```