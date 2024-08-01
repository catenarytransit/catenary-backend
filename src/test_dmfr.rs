use dmfr_dataset_reader::read_folders;

use std::error::Error;

fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    println!("Reading transitland directory");
    let dmfr_result = read_folders("./transitland-atlas/")?;

    println!("Broken feeds: {:?}", dmfr_result.list_of_bad_files);

    println!(
        "Transitland directory read with {} feeds and {} operators",
        dmfr_result.feed_hashmap.len(),
        dmfr_result.operator_hashmap.len()
    );

    Ok(())
}
