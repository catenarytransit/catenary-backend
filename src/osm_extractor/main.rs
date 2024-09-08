use std::fs::File;
use std::io::prelude::*;
use std::process::exit;
use std::fs::OpenOptions;

use geofabrik_handler::poly_parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let arguments = std::env::args();
    let arguments = arguments::parse(arguments).unwrap();

    let routing_export_path = arguments.get::<String>("routing_export_path").expect("Missing parameter routing_export_path");
    //let temp_dir = arguments.get::<String>("temp_dir").expect("Missing parameter temp_dir");

    //create dirs if they don't exist

    tokio::fs::create_dir_all(&routing_export_path).await?;
   // tokio::fs::create_dir_all(&temp_dir).await?;

    //download OSM

    let file_list = [
        "north-america/canada/quebec",
        "north-america/us/california"
    ];

    for file in file_list {
        let full_url = format!("https://download.geofabrik.de/{}.osm.pbf", file);
        let poly_url = format!("https://download.geofabrik.de/{}.poly", file);

        println!("Downloading OSM file {}", file);
        let download = reqwest::get(&full_url).await?;

        let bytes = download.bytes().await?;

        println!("Downloaded {}", full_url);

        let path = format!("{}/{}.osm.pbf", routing_export_path, file.replace("/", "-"));
        let poly_export = format!("{}/{}.osm.pbf", routing_export_path, file.replace("/", "-"));

        let poly_download = reqwest::get(&poly_url).await?;

        let poly_str = poly_download.text().await?;

        let poly_export_path = format!("{}/{}.polygon.bincode", routing_export_path, file.replace("/", "-"));

        let polygon = poly_parser(&poly_str)?;

        println!("Writing to {}", path);

        let mut file = OpenOptions::new().write(true)
                             .create_new(true)
                             .open(path)?;

        //write

        file.write_all(&bytes)?;

        //write poly

        let bincoded_poly = bincode::serialize(&polygon)?;

        let mut poly_file = OpenOptions::new().write(true)
                             .create_new(true)
                             .open(poly_export_path)?;

        poly_file.write_all(&bincoded_poly)?;

        println!("Wrote to {}", poly_export_path);

        //filter OSM into what's useful for road network


        

    }

    Ok(())
}