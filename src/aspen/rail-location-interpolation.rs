// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

//try to get it to work on MTA and BART using the same algorithms

//adapt for Vancouver Skytrain later, i don't want to worry about that right now.
async fn make_new_vehicle_feed(gtfs_rt_id: &str, gtfs_static_id: &str, gtfs_trips_data: gtfs_rt::FeedMessage) -> gtfs_rt::FeedMessage {
    //what do i return? Maybe GTFS RT?

    //or do I make it a sideeffect and insert into DB? Not sure yet.

    //also the feed needs to pull in GTFS static data like the stops coordinates right? But that's an excessive Postgres request.
}