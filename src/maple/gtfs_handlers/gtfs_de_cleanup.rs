use gtfs_structures::Gtfs;
use std::collections::{BTreeSet, HashMap};

pub fn gtfs_de_cleanup(gtfs: Gtfs) -> Gtfs {
    let to_delete_agencies = [
        "Flixbus",
        "Stadtwerke München",
        "Berliner Verkehrsbetriebe",
        "Karlsruher Verkehrsverbund",
        "Hamburger Verkehrsverbund",
        //  "Freiburger Verkehrs AG",
        "Verkehrsverbund Rhein-Sieg",
        "Flixtrain",
        "Verkehrsverbund Rhein-Neckar",
        "Südwestdeutsche Verkehrs-AG",
        "SBG SüdbadenBus GmbH",
        "Verkehrsverbund Schwarzwald-Baar",
        "Landkreis Calw",
        "Verkehrsverbund Stuttgart",
        "Verkehrsverbund Hegau Bodensee",
        //f-oebb~at
        "ÖBB",
        "erixx",
        "Flixtrain-de",
        "U-Bahn München",
        "Österreichische Bundesbahnen",
        "FlixBus-de",
        "Kölner VB",
        //f-u281z9-mvv
        "MVV-Regionalbus",
        "Straßenbahn München",
        "Regionalverkehr Oberbayer (überregional)",
        "Bus München",
        "NachtTram München",
        "NachtBus München",
        "MVV-Ruftaxi",
        "SNCF",
        "SBB",
        "Hochbahn U-Bahn",
        "RNV LU-MA (Strab)",
    ];

    let to_delete_agencies = Vec::from(to_delete_agencies)
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    crate::gtfs_handlers::remove_agencies::remove_agencies(gtfs, &to_delete_agencies)
}

pub fn gtfs_ch_cleanup(gtfs: Gtfs) -> Gtfs {
    let to_delete_agencies = [
        "Société Nationale des Chemins de fer Français",
        "DB Regio AG Baden-Württemberg",
        "DistriBus",
    ];

    let to_delete_agencies = Vec::from(to_delete_agencies)
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>();
    crate::gtfs_handlers::remove_agencies::remove_agencies(gtfs, &to_delete_agencies)
}
