// Generated from local protobufs
pub mod mtarr {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime_mtarr.rs"));
}

pub mod nyct {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime_nyct.rs"));
}

pub const URLS_LOOKUP: &[(&str, &str)] = &[
    (
        "f-mta~nyc~rt~subway~alerts",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts",
    ),
    (
        "f-mta~nyc~rt~subway~1~2~3~4~5~6~7",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    ),
    (
        "f-mta~nyc~rt~subway~a~c~e",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    ),
    (
        "f-mta~nyc~rt~subway~b~d~f~m",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    ),
    (
        "f-mta~nyc~rt~subway~g",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    ),
    (
        "f-mta~nyc~rt~subway~j~z",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    ),
    (
        "f-mta~nyc~rt~subway~l",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    ),
    (
        "f-mta~nyc~rt~subway~n~q~r~w",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    ),
    (
        "f-mta~nyc~rt~subway~sir",
        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
    ),
];
