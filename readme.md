## Install Dependencies

```bash
sudo apt install protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql unzip wget
```

## Loading in Data
Loading in data into the Postgres database is a multistep process. Ensure your postgres database is working and your password is set correctly.

### Download the Transitland repo
Transitland acts as an initial source of knowledge for Catenary-Backend, and associates static feeds and realtime feeds together.
Download and Update it via:
```bash
git submodule update
```

If you already have it, remember to git pull / merge changes
To do this, cd into the folder `transitland-atlas` and run `git pull`

### Download GTFS static data
This downloads the world's GTFS Static Data. This step may take a while, so go play some Minecraft / touch grass and come back when it's all finished!
```bash
cargo run --release --bin transitlanddownload
```

### Unzip and format the zip files
```bash
./src/ingest_gtfs_schedule/unzip-statics.sh
```

### Import data into the postgres database

```bash
cargo run --release --bin import -- --postgres "host=localhost user=postgres password=correcthorsebatterystaple" --threads 25 --startfresh true --isprod false
```

This command writes to `gtfs_stage`. 
Omit startfresh if you would want to wipe the staging directory.

For safety reasons, you are unable to wipe the `gtfs` schema, which is the production database, from this version.

You can also write to production, especially loading in a single agency, like this.

```bash
cargo run --release --bin import -- --postgres "host=localhost user=postgres password=correcthorsebatterystaple" --threads 25 --startfresh false --limittostaticfeed f-9q9-caltrain --isprod true
```

### Moving staging to be the new production database.

Moving the `gtfs_stage` set of tables to `gtfs` is really simple

```bash
cargo --bin move_to_prod --postgres "host=localhost user=postgres password=correcthorsebatterystaple"
```

You're all done! Data is fully ready for serving to users!

## Running the Application

### Install Systemd Service
```bash
sudo cp transitbackend.service /etc/systemd/system/transitbackend.service
sudo systemctl daemon-reload
sudo systemctl enable --now transitbackend.service
```

Example endpoints

`http://localhost:5401/getroutesperagency?feed_id=f-9mu-orangecountytransportationauthority`

`http://localhost:5401/gettrip?feed_id=f-9mu-orangecountytransportationauthority&trip_id=10995882`
