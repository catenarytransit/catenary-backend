use dmfr_folder_reader::ReturnDmfrAnalysis;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use chateau::Chateau;

// Written by Kyler Chin at Catenary Transit Initiatives
// https://github.com/CatenaryTransit/catenary-backend
//You are required under the APGL license to retain this annotation as is

pub async fn refresh_metadata_assignments(
    dmfr_result: &ReturnDmfrAnalysis,
    chateau_result: &HashMap<String, Chateau>,
    pool: &Arc<sqlx::Pool<sqlx::Postgres>>
) {
    //update or create realtime tables and static tables

    // if the new chateau id is different for any of the feeds, run the update function
}
