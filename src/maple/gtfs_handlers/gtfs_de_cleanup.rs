use gtfs_structures::Gtfs;
use std::collections::HashMap;

pub fn gtfs_de_cleanup(gtfs: Gtfs) -> Gtfs {
    let mut gtfs = gtfs;
    let to_delete_agencies = ["Flixbus", "Stadtwerke München"];

    let agency_ids_to_remove = to_delete_agencies
        .iter()
        .map(|x| {
            gtfs.agencies
                .iter()
                .find(|y| y.name == *x)
                .unwrap()
                .id
                .clone()
        })
        .flatten()
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    gtfs.agencies
        .retain(|x| !agency_ids_to_remove.contains(&x.id.as_ref().unwrap()));

    let stops_to_agency_ids: HashMap<String, Vec<String>> = HashMap::new();

    gtfs
}
