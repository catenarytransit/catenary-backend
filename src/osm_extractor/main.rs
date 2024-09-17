use ahash::AHashSet;
use osmpbfreader::Node;
use osmpbfreader::Way;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::process::exit;

use geofabrik_handler::poly_parser;

use osmpbfreader::OsmObj;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct ExportOsm {
    nodes: Vec<Node>,
    ways: Vec<Way>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = std::env::args();
    let arguments = arguments::parse(arguments).unwrap();

    let routing_export_path = arguments
        .get::<String>("routing_export_path")
        .expect("Missing parameter routing_export_path");
    let temp_dir = arguments
        .get::<String>("temp_dir")
        .expect("Missing parameter temp_dir");

    //create dirs if they don't exist

    use std::collections::HashSet;

    tokio::fs::create_dir_all(&routing_export_path).await?;
    // tokio::fs::create_dir_all(&temp_dir).await?;

    //download OSM

    let file_list = [
        "north-america/canada/quebec",
        //"north-america/canada/new-brunswick"
    ];

    for file_name in file_list {
        let full_url = format!("https://download.geofabrik.de/{}-latest.osm.pbf", file_name);
        let poly_url = format!("https://download.geofabrik.de/{}.poly", file_name);

        println!("Downloading OSM file {}", file_name);
        let download = reqwest::get(&full_url).await?;

        let bytes = download.bytes().await?;

        println!("Downloaded {}", full_url);

        let path = format!("{}/{}.osm.pbf", temp_dir, file_name.replace("/", "-"));
        let poly_export = format!("{}/{}.osm.pbf", temp_dir, file_name.replace("/", "-"));

        let poly_download = reqwest::get(&poly_url).await?;

        let poly_str = poly_download.text().await?;

        let poly_export_path = format!(
            "{}/{}.polygon.bincode",
            temp_dir,
            file_name.replace("/", "-")
        );

        let polygon = poly_parser(&poly_str)?;

        println!("Writing to {}", path);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.clone())
            .expect("Failed to open file");

        //write

        file.write_all(&bytes)?;

        //write poly

        let bincoded_poly = bincode::serialize(&polygon)?;

        let mut poly_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(poly_export_path.clone())
            .unwrap();

        poly_file.write_all(&bincoded_poly)?;

        println!("Wrote to {}", poly_export_path);

        //filter OSM into what's useful for road network

        let mut nodes: usize = 0;
        let mut ways: usize = 0;

        let mut keep_node_ped_bike_count: usize = 0;
        let mut keep_way_ped_bike_count: usize = 0;

        let mut keep_place_node_count: usize = 0;
        let mut keep_place_way_count: usize = 0;

        let mut read_osm = osmpbfreader::OsmPbfReader::new(bytes.as_ref());

        let mut kept_ped_bike_nodes: Vec<Node> = Vec::new();
        let mut kept_ped_bike_ways: Vec<Way> = Vec::new();
        let mut kept_place_search_list: Vec<OsmObj> = Vec::new();

        let mut nodes_to_keep_places: HashSet<i64> = HashSet::new();
        let mut nodes_already_kept_places: HashSet<i64> = HashSet::new();

        let mut nodes_to_keep_ped_and_bike: HashSet<i64> = HashSet::new();
        let mut nodes_already_kept_ped_and_bike: HashSet<i64> = HashSet::new();

        let mut pruned_car_only: usize = 0;

        let mut objs = Vec::new();

        for obj in read_osm.iter() {
            match obj {
                Err(e) => {
                    println!("Error reading OSM: {}", e);
                }
                Ok(obj) => {
                    objs.push(obj);
                }
            }
        }

        println!("Finished moving OSM to vec");

        drop(read_osm);

        for obj in objs.iter() {
            let mut keep_ped_bike = false;
            let mut keep_place_search = false;

            //sidewalk and bike lane extraction
            match &obj {
                OsmObj::Node(e) => {
                    nodes += 1;

                    if let Some((_, place_name)) =
                        e.tags.clone().iter().find(|(k, _)| k.eq(&"name"))
                    {
                        keep_place_search = true;
                    }

                    if let Some((_, road_type)) =
                        e.tags.clone().iter().find(|(k, _)| k.eq(&"highway"))
                    {
                        keep_ped_bike = true;

                        if road_type == "motorway" {
                            keep_ped_bike = false;
                        }
                    }

                    if let Some(pt) = e
                        .tags
                        .clone()
                        .iter()
                        .find(|(k, _)| k.eq(&"public_transport"))
                    {
                        keep_place_search = false;
                    }

                    if let (Some((_, bicycle)), Some((_, foot))) = (
                        e.tags.clone().iter().find(|(k, _)| k.eq(&"bicycle")),
                        e.tags.clone().iter().find(|(k, _)| k.eq(&"foot")),
                    ) {
                        if bicycle.eq(&"yes") || foot.eq(&"yes") {
                            keep_ped_bike = true;
                        }

                        if bicycle.eq(&"no") || foot.eq(&"no") {
                            pruned_car_only += 1;
                            keep_ped_bike = false;
                        }
                    }

                    if keep_ped_bike {
                        keep_node_ped_bike_count += 1;
                        nodes_already_kept_ped_and_bike.insert(e.id.0);

                        kept_ped_bike_nodes.push(e.clone());
                    }

                    /*  if keep_place_search {
                        keep_place_node_count += 1;
                        nodes_already_kept_places.insert(e.id.0);
                    }*/
                }
                OsmObj::Way(way) => {
                    ways += 1;

                    if let Some((_, name)) = way.tags.clone().iter().find(|(k, _)| k.eq(&"name")) {
                        keep_place_search = true;
                    }

                    if let Some((_, addr)) =
                        way.tags.clone().iter().find(|(k, _)| k.eq(&"addr:street"))
                    {
                        keep_place_search = true;
                    }

                    if let Some((_, road_type)) =
                        way.tags.clone().iter().find(|(k, _)| k.eq(&"highway"))
                    {
                        keep_ped_bike = true;

                        if road_type == "motorway" || road_type == "motorway_link" {
                            keep_ped_bike = false;
                            pruned_car_only += 1;
                        }
                    }

                    if let (Some((_, bicycle)), Some((_, foot))) = (
                        way.tags.clone().iter().find(|(k, _)| k.eq(&"bicycle")),
                        way.tags.clone().iter().find(|(k, _)| k.eq(&"foot")),
                    ) {
                        if bicycle.eq(&"yes") || foot.eq(&"yes") {
                            keep_ped_bike = true;
                        }

                        if bicycle.eq(&"no") || foot.eq(&"no") {
                            keep_ped_bike = false;
                        }
                    }

                    if let Some((_, access)) =
                        way.tags.clone().iter().find(|(k, _)| k.eq(&"access"))
                    {
                        if access == "no" || access == "private" || access == "military" {
                            keep_ped_bike = false;
                        }
                    }

                    if keep_ped_bike {
                        keep_way_ped_bike_count += 1;

                        for node in way.nodes.iter() {
                            nodes_to_keep_ped_and_bike.insert(node.0);
                        }

                        kept_ped_bike_ways.push(way.clone());
                    }

                    /*
                    if keep_place_search {
                        keep_place_way_count += 1;

                        for node in e.nodes.iter() {
                            nodes_to_keep_places.insert(node.0);
                        }
                    }*/
                }
                _ => {}
            }

            //push to vecs

            if keep_place_search {
                //   kept_place_search_list.push(obj.clone());
            }
        }

        let mut keep_node_ped_bike_count_second_round: usize = 0;

        //println!("kept place search list: {:?}", nodes_to_keep_ped_and_bike);
        // get nodes again

        for obj in objs.iter() {
            match obj {
                OsmObj::Node(e) => {
                    //   println!("Node: {:?}", e.id.0);
                    if nodes_to_keep_ped_and_bike.contains(&e.id.0) {
                        keep_node_ped_bike_count_second_round += 1;

                        if !nodes_already_kept_ped_and_bike.contains(&e.id.0) {
                            kept_ped_bike_nodes.push(e.clone());
                            keep_node_ped_bike_count += 1;
                        }
                    }

                    /*
                    if nodes_to_keep_places.contains(&e.id.0) {
                        if !nodes_already_kept_places.contains(&e.id.0) {
                            kept_place_search_list.push(obj.clone());
                            keep_place_node_count += 1;
                        }
                    }*/
                }
                _ => {
                    continue;
                }
            }
        }

        println!("Nodes: {}", nodes);
        println!("Ways: {}", ways);

        println!(
            "Nodes to keep for pedestrians and cyclists: {}",
            keep_node_ped_bike_count
        );
        println!(
            "Ways to keep for pedestrians and cyclists: {}",
            keep_way_ped_bike_count
        );

        println!(
            "Nodes that are from ways for ped and bike: {}, second round: {}",
            nodes_to_keep_ped_and_bike.len(),
            keep_node_ped_bike_count_second_round
        );

        println!("Rejected car-only ways: {}", pruned_car_only);

        println!("Nodes to keep for place search: {}", keep_place_node_count);

        println!("Ways to keep for place search: {}", keep_place_way_count);

        //write to file

        let ped_bike_file_path = format!(
            "{}/ped-and-bike-{}.osm.bincode",
            routing_export_path,
            file_name.replace("/", "-")
        );

        let mut ped_bike_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(ped_bike_file_path.clone())
            .expect("Failed to open file");

        let export_ped_bike = ExportOsm {
            nodes: kept_ped_bike_nodes,
            ways: kept_ped_bike_ways,
        };

        let bincoded_ped_bike = bincode::serialize(&export_ped_bike)?;

        ped_bike_file.write_all(&bincoded_ped_bike)?;

        println!("Wrote to {}", ped_bike_file_path);
    }

    Ok(())
}
