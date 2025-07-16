use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    cat::CatIndicesParts,
    http::{
        Url,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::{IndicesCreateParts, IndicesPutMappingParts},
};
use serde_json::{Value, json};
use std::error::Error;

pub fn single_elastic_connect(
    server_url: &str,
) -> Result<Elasticsearch, Box<dyn Error + Sync + Send>> {
    let url = Url::parse(server_url)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    Ok(client)
}

pub fn make_index_and_mappings(client: &Elasticsearch) -> Result<(), Box<dyn Error + Sync + Send>> {
    unimplemented!();

    let index_list = [
        (
            "osm",
            json!({
                "dynamic": "strict",
                "properties": {
                    "origin_file_name": {
                        "type": "text",
                    },
                    "origin_file_hash": {
                        "type": "text",
                    },
                    "admin_level_2_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_3_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_4_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_5_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_6_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_7_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_8_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_9_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "centroid": {
                        "type": "geo_point"
                    },
                    "shape": {
                        "type": "geo_shape"
                    },
                    "address_parts": {
                        "type": "object",
                        "dynamic": "strict",
                        "properties": {
                          "name": {
                            "type": "text",
                            "analyzer": "keyword",
                            "search_analyzer": "keyword",
                          },
                          "unit": {
                            "type": "text",
                            "analyzer": "keyword",
                          },
                          "number": {
                            "type": "text",
                            "analyzer": "keyword",
                          },
                          "street": {
                            "type": "text",
                            "analyzer": "keyword",
                          },
                          "cross_street": {
                            "type": "text",
                            "analyzer": "keyword",
                          },
                          "zip": {
                            "type": "text",
                            "analyzer": "keyword",
                          },
                        }
                      },
                      "name": {
                        "type": "object",
                        "dynamic": true,
                        "properties": {
                        "default": {
                            "type": "text",
                            "analyzer": "standard",
                            "copy_to": "name_search",
                            "fields": {
                            "raw": {
                                "type": "keyword"
                            },
                            "sort": {
                                "type": "icu_collation_keyword",
                                "language": "und"
                            }
                            }
                        }
                        }
                        },
                        "name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "category": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                }
            }),
        ),
        (
            "stops",
            json!({
                "dynamic": "strict",
                "properties": {
                    "stop_id": {
                        "type": "text",
                    },
                    "stop_code": {
                        "type": "text",
                    },
                    "chateau": {
                        "type": "text",
                    },
                    "onestop_feed_id": {
                        "type": "text",
                    },
                    "attempt_id": {

                        "type": "text",
                    },
                    "stop_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "centroid": {
                        "type": "geo_point"
                    },
                    "agency_names_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_2_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_3_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_4_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_5_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_6_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_7_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_8_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_9_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "point": {
                        "type": "geo_point"
                    },
                    "stop_name": {
                        "type": "object",
                        "dynamic": true,
                        "properties": {
                        "default": {
                            "type": "text",
                            "analyzer": "standard",
                            "copy_to": "stop_name_search",
                            "fields": {
                            "raw": {
                                "type": "keyword"
                            },
                            "sort": {
                                "type": "icu_collation_keyword",
                                "language": "und"
                            }
                            }
                        }
                        }
                    },
                    "route_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                }
            }),
        ),
        (
            "routes",
            json!({
                "dynamic": "strict",
                "properties": {
                    "chateau": {

                        "type": "text",
                    },
                    "onestop_feed_id": {

                        "type": "text",
                    },
                    "centroid": {
                        "type": "geo_point"
                    },
                    "attempt_id": {

                        "type": "text",
                    },
                    "route_id": {

                        "type": "text",
                    },
                    "agency_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_2_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_3_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_4_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_5_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_6_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_7_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_8_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_9_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "agency_name": {
                        "type": "object",
                        "dynamic": true,
                        "properties": {
                        "default": {
                            "type": "text",
                            "analyzer": "standard",
                            "copy_to": "agency_name_search",
                            "fields": {
                            "raw": {
                                "type": "keyword"
                            },
                            "sort": {
                                "type": "icu_collation_keyword",
                                "language": "und"
                            }
                            }
                        }
                        }
                    },
                }
            }),
        ),
        (
            "agencies",
            json!({
                "dynamic": "strict",
                "properties": {
                    "chateau": {

                        "type": "text",
                    },
                    "onestop_feed_id": {

                        "type": "text",
                    },
                    "attempt_id": {

                        "type": "text",
                    },
                    "agency_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_2_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_3_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_4_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_5_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_6_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_7_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "admin_level_8_names": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "agency_name": {
                        "type": "object",
                        "dynamic": true,
                        "properties": {
                        "default": {
                            "type": "text",
                            "analyzer": "standard",
                            "copy_to": "agency_name_search",
                            "fields": {
                            "raw": {
                                "type": "keyword"
                            },
                            "sort": {
                                "type": "icu_collation_keyword",
                                "language": "und"
                            }
                            }
                        }
                        }
                    },

                }
            }),
        ),
    ];

    Ok(())
}
