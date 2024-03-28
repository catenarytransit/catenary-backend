use chateau::Chateau;
use dmfr_folder_reader::ReturnDmfrAnalysis;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

// Written by Kyler Chin at Catenary Transit Initiatives
// https://github.com/CatenaryTransit/catenary-backend
//You are required under the APGL license to retain this annotation as is

pub async fn refresh_metadata_assignments(
    dmfr_result: &ReturnDmfrAnalysis,
    chateau_result: &HashMap<String, Chateau>,
    pool: Arc<catenary::postgres_tools::CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    //update or create realtime tables and static tables

    // if the new chateau id is different for any of the feeds, run the update function
    Ok(())
}
