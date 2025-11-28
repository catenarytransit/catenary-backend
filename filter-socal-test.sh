osmium tags-filter socal-latest.osm.pbf --invert-match --expressions=ped_bike_routing_filter_file_drop.txt -o socal-latest-filtered.osm.pbf
osmium tags-filter socal-latest-filtered.osm.pbf --expressions=ped_bike_routing_filter_file.txt -o socal-latest-filtered-2.osm.pbf

mv socal-latest-filtered-2.osm.pbf socal-latest-filtered.osm.pbf