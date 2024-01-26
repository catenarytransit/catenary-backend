use std::sync::Arc;
use crate::get_feeds_meta;

//Written by Kyler Chin at Catenary Transit Initiatives
//You are required under the APGL license to retain this annotation
pub fn refresh_feed_meta(
    transitland_meta: Arc<get_feeds_meta::TransitlandMetadata>,
    pool: &sqlx::Pool<sqlx::Postgres>,
) {
    //for each static feed, update the information except the hull
    //hullless static feeds will be culled out before http response

    //update realtime data relations

    //update operators list
}
