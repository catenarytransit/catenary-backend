use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use catenary::models::VehicleEntry;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use sqlx::query;
use std::sync::Arc;

#[derive(serde::Deserialize)]
struct GetVehicleDataQuery {
    label: String,
    chateau: String,
    route_id: Option<String>,
}

#[derive(serde::Serialize)]
struct ResponseVehicleIndividual {
    found_data: bool,
    vehicle: Option<VehicleEntry>,
}

async fn generic_number_lookup(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    file_path: &str,
    label: &str,
) -> HttpResponse {
    match label.parse::<i32>() {
        Ok(vehicle_number) => {
            match look_for_vehicle_number(conn, file_path, vehicle_number).await {
                Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                    found_data: true,
                    vehicle,
                }),
                Err(e) => HttpResponse::InternalServerError().json(ResponseVehicleIndividual {
                    found_data: false,
                    vehicle: None,
                }),
            }
        }
        Err(_) => HttpResponse::BadRequest().json(ResponseVehicleIndividual {
            found_data: false,
            vehicle: None,
        }),
    }
}

#[actix_web::get("/get_vehicle")]
pub async fn get_vehicle_data_endpoint(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: web::Query<GetVehicleDataQuery>,
    req: HttpRequest,
) -> impl Responder {
    let pool = pool.get_ref();
    let conn = &mut pool.get().await.unwrap();

    match query.chateau.as_str() {
        "metro~losangeles" => {
            match query.label.parse::<i32>() {
                Ok(vehicle_number) => {
                    match look_for_vehicle_number(
                        conn,
                        "north_america/united_states/california/losangelesmetro_bus",
                        vehicle_number,
                    )
                    .await
                    {
                        Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                            found_data: true,
                            vehicle,
                        }),
                        Err(e) => {
                            HttpResponse::InternalServerError().json(ResponseVehicleIndividual {
                                found_data: false,
                                vehicle: None,
                            })
                        }
                    }
                }
                Err(_) => {
                    //split by - and look for the first number

                    let vehicle_number = query
                        .label
                        .split("-")
                        .next()
                        .unwrap()
                        .parse::<i32>()
                        .unwrap();

                    match look_for_vehicle_number(
                        conn,
                        "north_america/united_states/california/losangelesmetro_rail",
                        vehicle_number,
                    )
                    .await
                    {
                        Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                            found_data: true,
                            vehicle,
                        }),
                        Err(e) => {
                            HttpResponse::InternalServerError().json(ResponseVehicleIndividual {
                                found_data: false,
                                vehicle: None,
                            })
                        }
                    }
                }
            }
        }
        "longbeachtransit" => {
            generic_number_lookup(
                conn,
                "north_america/united_states/california/longbeach",
                &query.label,
            )
            .await
        }
        "northcountytransitdistrict" => {
            generic_number_lookup(
                conn,
                "north_america/united_states/california/northcountytransitdistrict",
                &query.label,
            )
            .await
        }
        "orangecountytransportationauthority" => {
            generic_number_lookup(
                conn,
                "north_america/united_states/california/orangecounty",
                &query.label,
            )
            .await
        }
        "san-diego-mts" => {
            generic_number_lookup(
                conn,
                "north_america/united_states/california/mts",
                &query.label,
            )
            .await
        }
        "santacruzmetro" => {
            generic_number_lookup(
                conn,
                "north_america/united_states/california/santacruzmetro",
                &query.label,
            )
            .await
        }
        "vancouver-british-columbia-canada" => {
            generic_number_lookup(
                conn,
                "north_america/canada/british_columbia/translink",
                &query.label,
            )
            .await
        }
        "rseaudetransportdelacapitalertc" => {
            generic_number_lookup(conn, "north_america/canada/quebec/rtc", &query.label).await
        }
        "socitdetransportdemontral" => {
            generic_number_lookup(conn, "north_america/canada/quebec/stm", &query.label).await
        }
        "nyct" => {
            let only_numbers = query
                .label
                .chars()
                .filter(|x| x.is_numeric())
                .collect::<String>();

            generic_number_lookup(
                conn,
                "north_america/united_states/new_york/mta_buses",
                &only_numbers,
            )
            .await
        }
        "san-francisco-bay-area" => match &query.route_id {
            Some(route_id) => {
                let split_route_colon = route_id
                    .split(":")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();

                match split_route_colon.len() == 2 {
                    true => {
                        let agency_code = split_route_colon[0].clone();

                        match agency_code.as_str() {
                            "SM" => {
                                generic_number_lookup(
                                    conn,
                                    "north_america/united_states/california/smctd-samtrans",
                                    &query.label,
                                )
                                .await
                            }
                            "AC" => {
                                generic_number_lookup(
                                    conn,
                                    "north_america/united_states/california/alameda-actransit",
                                    &query.label,
                                )
                                .await
                            }
                            "SF" => {
                                let number = query.label.parse::<i32>().unwrap();

                                match look_for_vehicle_number(
                                    conn,
                                    "north_america/united_states/california/sfmta-muni_rail",
                                    number
                                )
                                .await
                                {
                                    Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                                        found_data: true,
                                        vehicle,
                                    }),
                                    Err(e) => {
                                        match look_for_vehicle_number(
                                            conn,
                                            "north_america/united_states/california/sfmta-muni_streetcar",
                                            number,
                                        )
                                        .await
                                        {
                                            Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                                                found_data: true,
                                                vehicle,
                                            }),
                                            Err(e) => {
                                                match look_for_vehicle_number(
                                                    conn,
                                                    "north_america/united_states/california/sfmta-muni_bus",
                                                    number,
                                                )
                                                .await
                                                {
                                                    Ok(vehicle) => HttpResponse::Ok().json(ResponseVehicleIndividual {
                                                        found_data: true,
                                                        vehicle,
                                                    }),
                                                    Err(e) => {
                                                        HttpResponse::InternalServerError().json(ResponseVehicleIndividual {
                                                            found_data: false,
                                                            vehicle: None,
                                                        })
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => HttpResponse::Ok().json(ResponseVehicleIndividual {
                                found_data: false,
                                vehicle: None,
                            }),
                        }
                    }
                    false => HttpResponse::Ok().json(ResponseVehicleIndividual {
                        found_data: false,
                        vehicle: None,
                    }),
                }
            }
            None => HttpResponse::Ok().json(ResponseVehicleIndividual {
                found_data: false,
                vehicle: None,
            }),
        },
        _ => HttpResponse::Ok().json(ResponseVehicleIndividual {
            found_data: false,
            vehicle: None,
        }),
    }
}

async fn look_for_vehicle_number(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    file_path: &str,
    vehicle_number: i32,
) -> Result<Option<VehicleEntry>, diesel::result::Error> {
    let vehicle = catenary::schema::gtfs::vehicles::dsl::vehicles
        .filter(catenary::schema::gtfs::vehicles::starting_range.le(vehicle_number))
        .filter(catenary::schema::gtfs::vehicles::ending_range.ge(vehicle_number))
        .filter(catenary::schema::gtfs::vehicles::file_path.eq(file_path))
        .first::<VehicleEntry>(conn)
        .await?;

    Ok(Some(vehicle))
}
