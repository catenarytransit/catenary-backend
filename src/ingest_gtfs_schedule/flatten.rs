use std::fs;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //list all folders in directory

    let destination_dir = "gtfs_uncompressed";

    let feeds = fs::read_dir(&destination_dir)?
    .map(|res| res.map(|e| e.path()))
    .collect::<Result<Vec<_>, std::io::Error>>()?;

    //println!("{:?}", feeds);

    for feed in feeds.into_iter() {

            

            let readdir = fs::read_dir(&feed);

            if readdir.is_ok() {
                let contents = readdir.unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;

            //println!("{:?}", contents);

            let listofdirectories = contents.clone().into_iter()
            .filter(|x| {

                let my_str = x.clone().into_os_string().into_string().unwrap();

                !my_str.contains("MACOSX")

            })
            .filter(|x| 
            std::fs::metadata(x).unwrap().is_dir() == true)
            .collect::<Vec<_>>();

            for directory in listofdirectories.into_iter() {
                println!("feed {:?}", feed);
                println!("{:?} contents need to be moved!", directory);
            }
            }
   
    }

    Ok(())
}