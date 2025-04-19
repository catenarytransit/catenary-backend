use gtfs_structures::Gtfs;
use std::collections::{BTreeSet, HashMap};

pub fn gtfs_de_cleanup(gtfs: Gtfs) -> Gtfs {
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
        "NEB Niederbarnimer Eisenbahn",
        "SWEG Bahn Stuttgart",
        "Albtal-Verkehrs-Gesellschaft",
        "VGM/VRL",
        "metronom",
        "DB ZugBus Regionalverkehr Alb-Bodensee",
        "Südwestdeutsche Verkehrs-AG",
        "Regionalverkehr Alb-Bodensee",
        "SBG SüdbadenBus GmbH",
        "Verkehrsverbund Schwarzwald-Baar",
        "Landkreis Calw",
        "Verkehrsverbund Stuttgart",
        "Verkehrsverbund Hegau Bodensee",
        "Regionalverkehr Alb-Bodensee",
        //f-oebb~at
        "ÖBB",
        "Hanseatische Eisenbahn GmbH",
        "erixx",
        "Ostdeutsche Eisenbahn GmbH",
        "Flixtrain-de",
        "U-Bahn München",
        "Österreichische Bundesbahnen",
        "FlixBus-de",
        "Kölner VB",
        "Bus München",
        //f-u0z-vgn
        "VGN",
        //f-u281z9-mvv
        "MVV-Regionalbus",
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
