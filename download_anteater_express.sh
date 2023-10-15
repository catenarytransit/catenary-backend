wget -O ./transitland-atlas/feeds/anteaterexpress.dmfr.json https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/anteaterexpress.dmfr.json
if [ ! -d gtfs_uncompressed/f-anteaterexpress/ ]; then
  mkdir gtfs_uncompressed/f-anteaterexpress/
fi
rm -rf ./gtfs_static_zips/f-anteaterexpress.zip
wget -O ./gtfs_static_zips/f-anteaterexpress.zip https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/f-anteaterexpress.zip
rm -rf ./gtfs_uncompressed/f-anteaterexpress/*
unzip -d ./gtfs_uncompressed/f-anteaterexpress/ ./gtfs_static_zips/f-anteaterexpress.zip
