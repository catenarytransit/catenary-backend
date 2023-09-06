## Install Dependencies

```bash
sudo apt install protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql unzip
```

## Loading in Data
Loading in data into the Postgres database is a multistep process. Ensure your postgres database is working and your password is set correctly.

### Download the Transitland repo
Transitland acts as an initial source of knowledge for Catenary-Backend, and associates static feeds and realtime feeds together.
```bash
./clone-transitland.sh
```

### Download GTFS static data
This downloads the world's GTFS Static Data. This step may take a while, so go play some Minecraft / touch grass and come back when it's all finished!
```bash
cargo run --bin transitlanddownload
```

### Unzip and format the zip files
```bash
./src/ingest_gtfs_schedule/unzip-statics.sh
```

### Import data into the postgres database

```bash
cargo run --bin import -- --postgres "host=localhost user=postgres password=correcthorsebatterystaple" --threads 25 --startfresh false
```

You're all done! Data is fully ready for serving to users!

## Running the Application

### Install Systemd Service
```bash
sudo mv transitbackend.service /etc/systemd/system/transitbackend.service
sudo systemctl enable --now transitbackend.service
```

Example endpoints

`http://localhost:5401/getroutesperagency?feed_id=f-9mu-orangecountytransportationauthority`

`http://localhost:5401/gettrip?feed_id=f-9mu-orangecountytransportationauthority&trip_id=10995882`