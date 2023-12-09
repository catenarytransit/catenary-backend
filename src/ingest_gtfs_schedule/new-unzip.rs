use std::fs;
use std::process::Command;

fn main() {
    // Set the source and destination directories
    let source_dir = "gtfs_static_zips";
    let destination_dir = "gtfs_uncompressed";

    // Change to the source directory
    std::env::set_current_dir(&source_dir).expect("Failed to change to the source directory");

    // Loop through each ZIP file in the source directory
    for entry in fs::read_dir(".").expect("Failed to read source directory") {
        if let Ok(entry) = entry {
            if let Some(zip_file) = entry.file_name().to_str() {
                if zip_file.ends_with(".zip") {
                    // Create a folder with the same name as the ZIP file (without the .zip extension) in the destination directory
                    let folder_name = &zip_file[..zip_file.len() - 4];
                    let destination_folder = format!("{}/{}", destination_dir, folder_name);

                    // Check if the destination folder already exists, if not, create it
                    if let Err(e) = fs::create_dir_all(&destination_folder) {
                        eprintln!("Failed to create destination folder: {}", e);
                        continue;
                    }

                    // Unzip the file into the destination folder
                    let unzip_result = Command::new("unzip").arg("-o").arg(zip_file).arg("-d").arg(&destination_folder).status();

                    if unzip_result.is_err() {
                        eprintln!("Failed to unzip {}: {}", zip_file, unzip_result.unwrap_err());
                        continue;
                    } 
                    println!("Unzipped {}", zip_file);
                }
            }
        }
    }

    println!("Unzipping complete!");

    // Flatten each feed
    let flatten_result = Command::new("cargo").arg("run").arg("--bin").arg("flattenuncompressed").status();

    if let Err(e) = flatten_result {
        eprintln!("Failed to run flattenuncompressed: {}", e);
    }

    // Change the permissions
    let chmod_result = Command::new("chmod").arg("-R").arg("+r").arg(destination_dir).status();

    if let Err(e) = chmod_result {
        eprintln!("Failed to change permissions: {}", e);
    }
}
