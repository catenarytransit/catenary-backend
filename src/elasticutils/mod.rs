use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    cat::CatIndicesParts,
    http::{
        Url,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::{IndicesCreateParts, IndicesPutMappingParts, IndicesPutSettingsParts},
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::error::Error;

pub fn single_elastic_connect(
    server_url: &str,
) -> Result<Elasticsearch, Box<dyn Error + Sync + Send>> {
    let url = Url::parse(server_url)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool)
        .cert_validation(elasticsearch::cert::CertificateValidation::None)
        .build()?;
    let client = Elasticsearch::new(transport);

    Ok(client)
}

pub async fn wipe_db(client: &Elasticsearch) -> Result<(), Box<dyn Error + Sync + Send>> {
    let idx_list = ["osm", "stops", "routes", "agencies"];

    for index_name in idx_list {
        let delete_response = client
            .indices()
            .delete(elasticsearch::indices::IndicesDeleteParts::Index(&[
                index_name,
            ]))
            .send()
            .await;
    }

    Ok(())
}

pub async fn make_index_and_mappings(
    client: &Elasticsearch,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let index_list = [
        (
            "osm",
            json!({
                            "settings": {


                                "analysis": {
                                      "tokenizer": {
                "my_korean_analyzer_user_dict": {
                  "type": "my_korean_analyzer_tokenizer",
                  "decompound_mode": "mixed",
             //     "user_dictionary": "user_dict_ko.txt"
                },
            },
                  "analyzer": {
                "my_korean_analyzer": {
                    "type": "custom",
                  "tokenizer": "my_korean_analyzer_user_dict",
                  "filter": [
                    "my_korean_analyzer_part_of_speech",
                    "my_korean_analyzer_readingform",
                    "lowercase"
                  ]
                }
              },
                                  "filter": {

                "my_korean_analyzer_part_of_speech": {
                    "type": "my_korean_analyzer_part_of_speech",
                    "stoptags": [
                      "E", "IC", "J", "MAJ", "MM", "SP", "SSC",
                      "SSO", "SC", "SE", "XPN", "XSA", "XSN", "XSV",
                      "UNA", "NA", "VSV"
                    ]
                  },
                                    "icu_collation_ar": { "type": "icu_collation", "language": "ar" },
                                    "icu_collation_ca": { "type": "icu_collation", "language": "ca" },
                                    "icu_collation_cs": { "type": "icu_collation", "language": "cs" },
                                    "icu_collation_de": { "type": "icu_collation", "language": "de" },
                                    "icu_collation_en": { "type": "icu_collation", "language": "en" },
                                    "icu_collation_es": { "type": "icu_collation", "language": "es" },
                                    "icu_collation_et": { "type": "icu_collation", "language": "et" },
                                    "icu_collation_fi": { "type": "icu_collation", "language": "fi" },
                                    "icu_collation_fr": { "type": "icu_collation", "language": "fr" },
                                    "icu_collation_hr": { "type": "icu_collation", "language": "hr" },
                                    "icu_collation_it": { "type": "icu_collation", "language": "it" },
                                    "icu_collation_ja": { "type": "icu_collation", "language": "ja" },
                                    "icu_collation_ko": { "type": "icu_collation", "language": "ko" },
                                    "icu_collation_nl": { "type": "icu_collation", "language": "nl" },
                                    "icu_collation_no": { "type": "icu_collation", "language": "no" },
                                    "icu_collation_pl": { "type": "icu_collation", "language": "pl" },
                                    "icu_collation_pt": { "type": "icu_collation", "language": "pt" },
                                    "icu_collation_ro": { "type": "icu_collation", "language": "ro" },
                                    "icu_collation_ru": { "type": "icu_collation", "language": "ru" },
                                    "icu_collation_sk": { "type": "icu_collation", "language": "sk" },
                                    "icu_collation_sr": { "type": "icu_collation", "language": "sr" },
                                    "icu_collation_sv": { "type": "icu_collation", "language": "sv" },
                                    "icu_collation_th": { "type": "icu_collation", "language": "th" },
                                    "icu_collation_zh_cn": { "type": "icu_collation", "language": "zh-CN" },
                                    "icu_collation_zh_tw": { "type": "icu_collation", "language": "zh-TW" }
                                  },
                                  "normalizer": {
                                    "ar_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ar"] },
                                    "ca_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ca"] },
                                    "cs_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_cs"] },
                                    "de_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_de"] },
                                    "en_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_en"] },
                                    "es_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_es"] },
                                    "et_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_et"] },
                                    "fi_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fi"] },
                                    "fr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fr"] },
                                    "hr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_hr"] },
                                    "it_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_it"] },
                                    "ja_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ja"] },
                                    "ko_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ko"] },
                                    "nl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_nl"] },
                                    "no_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_no"] },
                                    "pl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pl"] },
                                    "pt_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pt"] },
                                    "ro_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ro"] },
                                    "ru_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ru"] },
                                    "sk_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sk"] },
                                    "sr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sr"] },
                                    "sv_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sv"] },
                                    "th_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_th"] },
                                    "zh_cn_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_cn"] },
                                    "zh_tw_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_tw"] }
                                  }
                                }
                              },
                            "mappings": {
                                "dynamic": "strict",
                                "dynamic_templates": [
                                    {
                                        "copy_to_name_search": {
                                        "path_match": "name.*",
                                        "mapping": {
                                            "copy_to": "name_search"
                                        }
                                        }
                                    }
                                    ],
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
                                "bbox": {
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
                                        "properties": generate_language_properties()
                                    },
                                "category": {
                                    "type": "text",
                                    "analyzer": "standard"
                                },
                            }
                            }
                        }),
        ),
        (
            "stops",
            json!({
                "settings": {

                    "analysis": {
                        "tokenizer": {
                        "my_korean_analyzer_user_dict": {
                          "type": "my_korean_analyzer_tokenizer",
                          "decompound_mode": "mixed",
                        //  "user_dictionary": "user_dict_ko.txt"
                        },
                    },
                          "analyzer": {
                        "my_korean_analyzer": {
                          "tokenizer": "my_korean_analyzer_user_dict",
                          "filter": [
                            "my_korean_analyzer_part_of_speech",
                            "my_korean_analyzer_readingform",
                            "lowercase"
                          ]
                        }
                      },
                      "filter": {
                        "my_korean_analyzer_part_of_speech": {
                          "type": "my_korean_analyzer_part_of_speech",
                          "stoptags": [
                            "E", "IC", "J", "MAJ", "MM", "SP", "SSC",
                            "SSO", "SC", "SE", "XPN", "XSA", "XSN", "XSV",
                            "UNA", "NA", "VSV"
                          ]
                        },
                        "icu_collation_ar": { "type": "icu_collation", "language": "ar" },
                        "icu_collation_ca": { "type": "icu_collation", "language": "ca" },
                        "icu_collation_cs": { "type": "icu_collation", "language": "cs" },
                        "icu_collation_de": { "type": "icu_collation", "language": "de" },
                        "icu_collation_en": { "type": "icu_collation", "language": "en" },
                        "icu_collation_es": { "type": "icu_collation", "language": "es" },
                        "icu_collation_et": { "type": "icu_collation", "language": "et" },
                        "icu_collation_fi": { "type": "icu_collation", "language": "fi" },
                        "icu_collation_fr": { "type": "icu_collation", "language": "fr" },
                        "icu_collation_hr": { "type": "icu_collation", "language": "hr" },
                        "icu_collation_it": { "type": "icu_collation", "language": "it" },
                        "icu_collation_ja": { "type": "icu_collation", "language": "ja" },
                        "icu_collation_ko": { "type": "icu_collation", "language": "ko" },
                        "icu_collation_nl": { "type": "icu_collation", "language": "nl" },
                        "icu_collation_no": { "type": "icu_collation", "language": "no" },
                        "icu_collation_pl": { "type": "icu_collation", "language": "pl" },
                        "icu_collation_pt": { "type": "icu_collation", "language": "pt" },
                        "icu_collation_ro": { "type": "icu_collation", "language": "ro" },
                        "icu_collation_ru": { "type": "icu_collation", "language": "ru" },
                        "icu_collation_sk": { "type": "icu_collation", "language": "sk" },
                        "icu_collation_sr": { "type": "icu_collation", "language": "sr" },
                        "icu_collation_sv": { "type": "icu_collation", "language": "sv" },
                        "icu_collation_th": { "type": "icu_collation", "language": "th" },
                        "icu_collation_zh_cn": { "type": "icu_collation", "language": "zh-CN" },
                        "icu_collation_zh_tw": { "type": "icu_collation", "language": "zh-TW" }
                      },
                      "normalizer": {
                        "ar_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ar"] },
                        "ca_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ca"] },
                        "cs_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_cs"] },
                        "de_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_de"] },
                        "en_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_en"] },
                        "es_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_es"] },
                        "et_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_et"] },
                        "fi_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fi"] },
                        "fr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fr"] },
                        "hr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_hr"] },
                        "it_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_it"] },
                        "ja_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ja"] },
                        "ko_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ko"] },
                        "nl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_nl"] },
                        "no_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_no"] },
                        "pl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pl"] },
                        "pt_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pt"] },
                        "ro_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ro"] },
                        "ru_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ru"] },
                        "sk_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sk"] },
                        "sr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sr"] },
                        "sv_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sv"] },
                        "th_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_th"] },
                        "zh_cn_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_cn"] },
                        "zh_tw_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_tw"] }
                      }
                    }
                  },
                "mappings": {
                    "dynamic": "strict",
                    "dynamic_templates": [
                        {
                            "copy_to_stop_name_search": {
                            "path_match": "stop_name.*",
                            "mapping": {
                                "copy_to": "stop_name_search"
                            }
                            }
                        }
                    ],
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
                    "route_types": {
                        "type": "short"
                    },
                    "stop_name": {
                        "type": "object",
                        "dynamic": true,
                        "properties": generate_language_properties()
                    },
                    "route_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "agency_name_search": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                }
                }
            }),
        ),
        (
            "routes",
            json!({                "settings": {

                             "analysis": {
                                 "tokenizer": {
                         "my_korean_analyzer_user_dict": {
                           "type": "my_korean_analyzer_tokenizer",
                           "decompound_mode": "mixed",
            //               "user_dictionary": "user_dict_ko.txt"
                         },
                     },
                           "analyzer": {
                         "my_korean_analyzer": {
                           "tokenizer": "my_korean_analyzer_user_dict",
                           "filter": [
                             "my_korean_analyzer_part_of_speech",
                             "my_korean_analyzer_readingform",
                             "lowercase"
                           ]
                         }
                       },
                               "filter": {
                                 "my_korean_analyzer_part_of_speech": {
                                     "type": "my_korean_analyzer_part_of_speech",
                                     "stoptags": [
                                       "E", "IC", "J", "MAJ", "MM", "SP", "SSC",
                                       "SSO", "SC", "SE", "XPN", "XSA", "XSN", "XSV",
                                       "UNA", "NA", "VSV"
                                     ]
                                   },
                                 "icu_collation_ar": { "type": "icu_collation", "language": "ar" },
                                 "icu_collation_ca": { "type": "icu_collation", "language": "ca" },
                                 "icu_collation_cs": { "type": "icu_collation", "language": "cs" },
                                 "icu_collation_de": { "type": "icu_collation", "language": "de" },
                                 "icu_collation_en": { "type": "icu_collation", "language": "en" },
                                 "icu_collation_es": { "type": "icu_collation", "language": "es" },
                                 "icu_collation_et": { "type": "icu_collation", "language": "et" },
                                 "icu_collation_fi": { "type": "icu_collation", "language": "fi" },
                                 "icu_collation_fr": { "type": "icu_collation", "language": "fr" },
                                 "icu_collation_hr": { "type": "icu_collation", "language": "hr" },
                                 "icu_collation_it": { "type": "icu_collation", "language": "it" },
                                 "icu_collation_ja": { "type": "icu_collation", "language": "ja" },
                                 "icu_collation_ko": { "type": "icu_collation", "language": "ko" },
                                 "icu_collation_nl": { "type": "icu_collation", "language": "nl" },
                                 "icu_collation_no": { "type": "icu_collation", "language": "no" },
                                 "icu_collation_pl": { "type": "icu_collation", "language": "pl" },
                                 "icu_collation_pt": { "type": "icu_collation", "language": "pt" },
                                 "icu_collation_ro": { "type": "icu_collation", "language": "ro" },
                                 "icu_collation_ru": { "type": "icu_collation", "language": "ru" },
                                 "icu_collation_sk": { "type": "icu_collation", "language": "sk" },
                                 "icu_collation_sr": { "type": "icu_collation", "language": "sr" },
                                 "icu_collation_sv": { "type": "icu_collation", "language": "sv" },
                                 "icu_collation_th": { "type": "icu_collation", "language": "th" },
                                 "icu_collation_zh_cn": { "type": "icu_collation", "language": "zh-CN" },
                                 "icu_collation_zh_tw": { "type": "icu_collation", "language": "zh-TW" }
                               },
                               "normalizer": {
                                 "ar_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ar"] },
                                 "ca_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ca"] },
                                 "cs_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_cs"] },
                                 "de_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_de"] },
                                 "en_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_en"] },
                                 "es_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_es"] },
                                 "et_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_et"] },
                                 "fi_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fi"] },
                                 "fr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fr"] },
                                 "hr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_hr"] },
                                 "it_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_it"] },
                                 "ja_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ja"] },
                                 "ko_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ko"] },
                                 "nl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_nl"] },
                                 "no_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_no"] },
                                 "pl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pl"] },
                                 "pt_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pt"] },
                                 "ro_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ro"] },
                                 "ru_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ru"] },
                                 "sk_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sk"] },
                                 "sr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sr"] },
                                 "sv_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sv"] },
                                 "th_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_th"] },
                                 "zh_cn_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_cn"] },
                                 "zh_tw_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_tw"] }
                               }
                             }
                           },
                             "mappings": {
                                 "dynamic": "strict",
                                 "dynamic_templates": [
                                     {
                                         "copy_to_route_name_search": {
                                         "path_match": "route_name.*",
                                         "mapping": {
                                             "copy_to": "route_name_search"
                                         }
                                         }
                                     },
                                     {
                                         "copy_to_agency_name_search": {
                                         "path_match": "agency_name.*",
                                         "mapping": {
                                             "copy_to": "agency_name_search"
                                         }
                                         }
                                     }
                                     ],
                             "properties": {
                                "route_short_name": {
                                     "type": "object",
                                     "dynamic": true,
                                     "properties": generate_language_properties()
                                 },
                                "route_long_name": {
                                     "type": "object",
                                     "dynamic": true,
                                     "properties": generate_language_properties()
                                 },
                                "route_type": {
                                    "type": "short"
                                },
                                 "route_name_search": {
                                     "type": "text",
                                     "analyzer": "standard"
                                 },
                                 "route_name_search": {
                                     "type": "text",
                                     "analyzer": "standard"
                                 },
                                 "chateau": {
                                     "type": "text",
                                 },
                                 "onestop_feed_id": {

                                     "type": "text",
                                 },
                                 "centroid": {
                                     "type": "geo_point"
                                 },
                                "bbox": {
                                    "type": "geo_shape"
                                },
                                "important_points": {
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
                                     "properties": generate_language_properties()
                                 },
                             }
                             }
                         }),
        ),
        (
            "agencies",
            json!({
                    "settings": {

                "analysis": {    "tokenizer": {
                    "my_korean_analyzer_user_dict": {
                      "type": "my_korean_analyzer_tokenizer",
                      "decompound_mode": "mixed",
            //          "user_dictionary": "user_dict_ko.txt"
                    },
                },
                      "analyzer": {
                    "my_korean_analyzer": {
                      "tokenizer": "my_korean_analyzer_user_dict",
                      "filter": [
                        "my_korean_analyzer_part_of_speech",
                        "my_korean_analyzer_readingform",
                        "lowercase"
                      ]
                    }
                  },
                  "filter": {
                    "my_korean_analyzer_part_of_speech": {
                        "type": "my_korean_analyzer_part_of_speech",
                        "stoptags": [
                          "E", "IC", "J", "MAJ", "MM", "SP", "SSC",
                          "SSO", "SC", "SE", "XPN", "XSA", "XSN", "XSV",
                          "UNA", "NA", "VSV"
                        ]
                      },
                    "icu_collation_ar": { "type": "icu_collation", "language": "ar" },
                    "icu_collation_ca": { "type": "icu_collation", "language": "ca" },
                    "icu_collation_cs": { "type": "icu_collation", "language": "cs" },
                    "icu_collation_de": { "type": "icu_collation", "language": "de" },
                    "icu_collation_en": { "type": "icu_collation", "language": "en" },
                    "icu_collation_es": { "type": "icu_collation", "language": "es" },
                    "icu_collation_et": { "type": "icu_collation", "language": "et" },
                    "icu_collation_fi": { "type": "icu_collation", "language": "fi" },
                    "icu_collation_fr": { "type": "icu_collation", "language": "fr" },
                    "icu_collation_hr": { "type": "icu_collation", "language": "hr" },
                    "icu_collation_it": { "type": "icu_collation", "language": "it" },
                    "icu_collation_ja": { "type": "icu_collation", "language": "ja" },
                    "icu_collation_ko": { "type": "icu_collation", "language": "ko" },
                    "icu_collation_nl": { "type": "icu_collation", "language": "nl" },
                    "icu_collation_no": { "type": "icu_collation", "language": "no" },
                    "icu_collation_pl": { "type": "icu_collation", "language": "pl" },
                    "icu_collation_pt": { "type": "icu_collation", "language": "pt" },
                    "icu_collation_ro": { "type": "icu_collation", "language": "ro" },
                    "icu_collation_ru": { "type": "icu_collation", "language": "ru" },
                    "icu_collation_sk": { "type": "icu_collation", "language": "sk" },
                    "icu_collation_sr": { "type": "icu_collation", "language": "sr" },
                    "icu_collation_sv": { "type": "icu_collation", "language": "sv" },
                    "icu_collation_th": { "type": "icu_collation", "language": "th" },
                    "icu_collation_zh_cn": { "type": "icu_collation", "language": "zh-CN" },
                    "icu_collation_zh_tw": { "type": "icu_collation", "language": "zh-TW" }
                  },
                  "normalizer": {
                    "ar_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ar"] },
                    "ca_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ca"] },
                    "cs_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_cs"] },
                    "de_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_de"] },
                    "en_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_en"] },
                    "es_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_es"] },
                    "et_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_et"] },
                    "fi_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fi"] },
                    "fr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_fr"] },
                    "hr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_hr"] },
                    "it_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_it"] },
                    "ja_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ja"] },
                    "ko_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ko"] },
                    "nl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_nl"] },
                    "no_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_no"] },
                    "pl_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pl"] },
                    "pt_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_pt"] },
                    "ro_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ro"] },
                    "ru_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_ru"] },
                    "sk_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sk"] },
                    "sr_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sr"] },
                    "sv_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_sv"] },
                    "th_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_th"] },
                    "zh_cn_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_cn"] },
                    "zh_tw_collation": { "type": "custom", "filter": ["lowercase", "icu_collation_zh_tw"] }
                  }
                }
              },
                "mappings": {
                    "dynamic": "strict",
                    "dynamic_templates": [
                        {
                            "copy_to_agency_name_search": {
                            "path_match": "agency_name.*",
                            "mapping": {
                                "copy_to": "agency_name_search"
                            }
                            }
                        }
                        ],
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
                    "bbox": {
                        "type": "geo_shape"
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
                        "properties": generate_language_properties()
                    },

                }
                }
            }),
        ),
    ];

    for (index_name, mapping_json) in index_list {
        let create_response = client
            .indices()
            .create(IndicesCreateParts::Index(index_name))
            .send()
            .await;

        match create_response {
            Ok(response) => {
                if response.status_code().is_success() {
                    let response_body = response.json::<Value>().await?;
                    println!("Index created successfully: {:?}", response_body);
                } else {
                    let status = response.status_code();
                    let body = response.text().await?;
                    println!("Received non-success status [{}]: {}", status, body.trim());
                    if !body.contains("resource_already_exists_exception") {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to create index with status {}: {}", status, body),
                        )));
                    }
                    println!(
                        "Index '{}' already exists. Proceeding to update mapping.",
                        index_name
                    );
                }
            }
            Err(e) => {
                eprintln!("Error creating index: {:?}", e);
                return Err(Box::new(e));
            }
        }

        let put_settings_response = client
            .indices()
            .put_settings(IndicesPutSettingsParts::Index(&[index_name]))
            .reopen(true)
            .body(mapping_json.get("settings").unwrap())
            .send()
            .await?;

        if put_settings_response.status_code().is_success() {
            let response_body = put_settings_response.json::<Value>().await?;
            println!("Settings updated successfully: {:?}", response_body);
        } else {
            let status = put_settings_response.status_code();
            let error_body = put_settings_response.text().await?;
            eprintln!(
                "Error updating settings. Status: {}. Body: {}",
                status, error_body
            );
            // Create a custom error to return
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to put settings with status {}: {}",
                    status, error_body
                ),
            )));
        }

        let put_mapping_response = client
            .indices()
            .put_mapping(IndicesPutMappingParts::Index(&[index_name]))
            .body(mapping_json.get("mappings").unwrap())
            .send()
            .await?;

        if put_mapping_response.status_code().is_success() {
            let response_body = put_mapping_response.json::<Value>().await?;
            println!("Mapping updated successfully: {:?}", response_body);
        } else {
            let status = put_mapping_response.status_code();
            let error_body = put_mapping_response.text().await?;
            eprintln!(
                "Error updating mapping. Status: {}. Body: {}",
                status, error_body
            );
            // Create a custom error to return
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to put mapping with status {}: {}",
                    status, error_body
                ),
            )));
        }
    }

    Ok(())
}

pub fn generate_language_properties() -> HashMap<String, HashMap<String, String>> {
    let languages = [
        ("ar", "arabic"),
        ("ca", "catalan"),
        ("cs", "czech"),
        ("de", "german"),
        ("en", "english"),
        ("es", "spanish"),
        ("et", "standard"),
        ("fi", "finnish"),
        ("fr", "french"),
        ("hr", "standard"),
        ("it", "italian"),
        ("ja", "cjk"),
        ("ja-hrkt", "cjk"),
        ("ko", "cjk"),
        ("nl", "dutch"),
        ("no", "norwegian"),
        ("pl", "standard"),
        ("pt", "portuguese"),
        ("ro", "romanian"),
        ("ru", "russian"),
        ("sk", "standard"),
        ("sr", "standard"),
        ("sv", "swedish"),
        ("th", "thai"),
        ("zh", "cjk"),
        ("zh_hans", "cjk"),
        ("zh_hant", "cjk"),
        ("zh_cn", "cjk"),
        ("zh_tw", "cjk"),
        ("zh_hant_hk", "cjk"),
        ("zh_hans_hk", "cjk"),
    ];

    let mut map = HashMap::new();

    for (lang_code, analyser) in languages {
        let mut obj: HashMap<String, String> = HashMap::new();

        obj.insert("type".to_string(), "text".to_string());
        obj.insert("analyzer".to_string(), analyser.to_string());

        map.insert(lang_code.to_string(), obj);
    }

    map
}
