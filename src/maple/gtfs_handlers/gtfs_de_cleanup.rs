use gtfs_structures::Gtfs;
use std::collections::{BTreeSet, HashMap};

pub fn gtfs_de_cleanup(gtfs: Gtfs) -> Gtfs {
    let mut gtfs = gtfs;
    let to_delete_agencies = [
        "Flixbus",
        "Stadtwerke München",
        "Berliner Verkehrsbetriebe",
        "Karlsruher Verkehrsverbund",
        "Hamburger Verkehrsverbund",
        "Freiburger Verkehrs AG",
        "Verkehrsverbund Rhein-Sieg",
        "Flixtrain",
        "Verkehrsverbund Rhein-Neckar",
        "NEB Niederbarnimer Eisenbahn"
    ];

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

    let mut stops_to_agency_ids: HashMap<String, BTreeSet<String>> = HashMap::new();

    for (trip_id, trip) in gtfs.trips.iter() {
        let agency_id = gtfs.routes.get(&trip.route_id).unwrap().agency_id.clone();

        if let Some(agency_id) = &agency_id {
            for stop_time in &trip.stop_times {
                let stop_id = stop_time.stop.id.clone();

                if agency_ids_to_remove.contains(agency_id) {
                    continue;
                }

                if stops_to_agency_ids.contains_key(&stop_id) {
                    stops_to_agency_ids
                        .get_mut(&stop_id)
                        .unwrap()
                        .insert(agency_id.clone());
                } else {
                    let mut agency_ids = BTreeSet::new();
                    agency_ids.insert(agency_id.clone());
                    stops_to_agency_ids.insert(stop_id.clone(), agency_ids);
                }
            }
        }
    }

    gtfs.trips
        .retain(|trip_id, trip| match gtfs.routes.get(&trip.route_id) {
            Some(route) => match &route.agency_id {
                Some(agency_id) => !agency_ids_to_remove.contains(agency_id),
                None => true,
            },
            //route id is not valid, remove trip
            None => false,
        });

    gtfs.stops
        .retain(|stop_id, stop| match stops_to_agency_ids.get(stop_id) {
            Some(agency_ids) => !agency_ids.is_empty(),
            None => true,
        });

    gtfs.routes
        .retain(|route_id, route| match &route.agency_id {
            Some(agency_id) => !agency_ids_to_remove.contains(agency_id),
            None => true,
        });

    gtfs
}
