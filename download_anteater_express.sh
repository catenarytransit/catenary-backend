curl https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/anteaterexpress.dmfr.json > ./transitland-atlas/feeds/anteaterexpress.dmfr.json
if [ ! -d gtfs_uncompressed/f-anteaterexpress/ ]; then
  mkdir gtfs_uncompressed/f-anteaterexpress/
fi
rm -rf ./gtfs_static_zips/f-anteaterexpress.zip
sudo curl https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/f-anteaterexpress.zip > ./gtfs_static_zips/f-anteaterexpress.zip
rm -rf ../gtfs_uncompressed/f-anteaterexpress/*
unzip ../../gtfs_static_zips/f-anteaterexpress.zip
