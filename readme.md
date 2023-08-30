## Install Dependencies

```bash
sudo apt install protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql unzip
```

### Install Systemd Service
```bash
sudo mv transitbackend.service /etc/systemd/system/transitbackend.service
sudo systemctl enable --now transitbackend.service
```

### Running gtfs static ingest

```bash
cargo run --bin schedule_ingest -- --postgres "host=localhost user=postgres password=correcthorsebatterystaple"
```

Example endpoints

`http://localhost:5401/getroutesperagency?feed_id=f-9mu-orangecountytransportationauthority`

`http://localhost:5401/gettrip?feed_id=f-9mu-orangecountytransportationauthority&trip_id=10995882`
