use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::process::exit;

use geofabrik_handler::poly_parser;

use osmpbfreader::OsmObj;

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

    tokio::fs::create_dir_all(&routing_export_path).await?;
    // tokio::fs::create_dir_all(&temp_dir).await?;

    //download OSM

    let file_list = ["north-america/canada/quebec", "north-america/us/california"];

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

        let mut read_osm = osmpbfreader::OsmPbfReader::new(bytes.as_ref());

        let mut kept_ped_bike_list = Vec::new();

        for obj in read_osm.iter() {
            match obj {
                Err(e) => {
                    println!("Error reading OSM: {}", e);
                }
                Ok(obj) => {
                    let mut keep_ped_bike = false;

                    //sidewalk and bike lane extraction
                    match &obj {
                        OsmObj::Node(e) => {
                            nodes += 1;

                            if let Some(road_type) =
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"highway"))
                            {
                                keep_ped_bike = true;
                            }

                            if let (Some(bicycle), Some(foot)) = (
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"bicycle")),
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"foot")),
                            ) {
                                if bicycle.1.eq(&"yes") || foot.1.eq(&"yes") {
                                    keep_ped_bike = true;
                                }

                                if bicycle.1.eq(&"no") || foot.1.eq(&"no") {
                                    keep_ped_bike = false;
                                }
                            }

                            if keep_ped_bike {
                                keep_node_ped_bike_count += 1;
                            }
                        }
                        OsmObj::Way(e) => {
                            ways += 1;

                            if let Some(road_type) =
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"highway"))
                            {
                                keep_ped_bike = true;
                            }

                            if let (Some(bicycle), Some(foot)) = (
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"bicycle")),
                                e.tags.clone().iter().find(|(k, _)| k.eq(&"foot")),
                            ) {
                                if bicycle.1.eq(&"yes") || foot.1.eq(&"yes") {
                                    keep_ped_bike = true;
                                }

                                if bicycle.1.eq(&"no") || foot.1.eq(&"no") {
                                    keep_ped_bike = false;
                                }
                            }

                            if keep_ped_bike {
                                keep_way_ped_bike_count += 1;
                            }
                        }
                        _ => {}
                    }

                    //push to vecs

                    if keep_ped_bike {
                        kept_ped_bike_list.push(obj.clone());
                    }
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

        //write to file

        let ped_bike_path = format!(
            "{}/ped-and-bike-{}.osm.bincode",
            routing_export_path,
            file_name.replace("/", "-")
        );

        let mut ped_bike_path = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.clone())
            .expect("Failed to open file");

        let bincoded_ped_bike = bincode::serialize(&kept_ped_bike_list)?;

        ped_bike_path.write_all(&bincoded_ped_bike)?;
    }

    Ok(())
}
