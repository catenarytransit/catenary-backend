cd transitland-atlas/feeds
curl https://raw.githubusercontent.com/CatenaryMaps/zotgtfs/main/anteaterexpress.dmfr.json > anteaterexpress.dmfr.json
cd ../..
if [ ! -d gtfs_uncompressed/f-anteaterexpress/ ]; then
  mkdir gtfs_uncompressed/f-anteaterexpress/
fi