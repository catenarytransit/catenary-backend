use gtfs_structures::Gtfs;

pub fn gtfs_de_cleanup(gtfs: Gtfs) -> Gtfs {
    let mut gtfs = gtfs;
    let to_delete_agencies = ["Flixbus", "Stadtwerke München"];

    let agency_ids_to_remove = to_delete_agencies
        .iter()
        .map(|x| {
            gtfs.agencies
                .iter()
                .find(|y| y.agency_name == *x)
                .unwrap()
                .agency_id
                .clone()
        })
        .collect::<Vec<String>>();

    gtfs.agencies
        .retain(|x| !to_delete_agencies.contains(&x.agency_name));

    let stops_to_agency_ids: HashMap<String, Vec<String>> = HashMap::new();

    gtfs
}
