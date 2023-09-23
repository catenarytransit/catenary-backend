cd transitland-atlas/feeds
curl https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/anteaterexpress.dmfr.json > anteaterexpress.dmfr.json
cd ../..
if [ ! -d gtfs_uncompressed/f-anteaterexpress/ ]; then
  mkdir gtfs_uncompressed/f-anteaterexpress/
fi
cd gtfs_static_zips/f-anteaterexpress.zip
curl https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/f-anteaterexpress.zip > f-anteaterexpress.zip
cd ../../gtfs_uncompressed/f-anteaterexpress/
unzip ../../gtfs_static_zips/f-anteaterexpress.zip