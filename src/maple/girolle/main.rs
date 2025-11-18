use std::{collections::BTreeMap, fs, sync::Arc};

use catenary::GirolleFeedDownloadResult;
use futures::StreamExt;
use std::time::Instant;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(long)]
    transitland: String,
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long)]
    outfile: String,
}

#[derive(Clone)]
struct StaticFeedToDownload {
    pub feed_id: String,
    pub url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let args = Args::parse();

    let threads: usize = match args.threads {
        Some(threads) => threads,
        _ => 512,
    };

    let client = reqwest::ClientBuilder::new()
        .use_rustls_tls()
        .user_agent("Catenary Girolle")
        .timeout(std::time::Duration::from_secs(60 * 5))
        .connect_timeout(std::time::Duration::from_secs(30))
        .danger_accept_invalid_certs(true)
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .cookie_store(true)
        .build()
        .unwrap();

    let mut feed_output: BTreeMap<String, GirolleFeedDownloadResult> = BTreeMap::new();

    let _ = fs::read_dir(format!("{}/feeds", args.transitland))?;

    let dmfr_result = dmfr_dataset_reader::read_folders(&args.transitland)?;

    let feeds_to_download = dmfr_result
        .feed_hashmap
        .iter()
        .filter(|(string, feed)| feed.urls.static_current.is_some())
        .filter(|(_, feed)| {
            !feed
                .urls
                .static_current
                .as_ref()
                .unwrap()
                .contains("?acl:consumerKey=")
        })
        .map(|(string, feed)| StaticFeedToDownload {
            feed_id: feed.id.clone(),
            url: feed.urls.static_current.as_ref().unwrap().to_string(),
        })
        .collect::<Vec<StaticFeedToDownload>>();

    println!("Downloading zip files now");

    let download_progress: Arc<std::sync::Mutex<u16>> = Arc::new(std::sync::Mutex::new(0));
    let total_feeds_to_download = feeds_to_download.len();

    let returned_feed_data =
        futures::stream::iter(feeds_to_download.into_iter().map(|staticfeed| {
            let client = client.clone();
            let download_progress = Arc::clone(&download_progress);

            let mut data_about_feed = GirolleFeedDownloadResult {
                feed_id: staticfeed.feed_id.clone(),
                seahash: None,
                download_timestamp_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
                valid_gtfs: false,
                time_to_download_ms: None,
                byte_size: None,
                url: staticfeed.url.clone(),
            };

            async move {
                let start_time = Instant::now();
                let download = client.get(staticfeed.url.clone()).send().await;

                let duration = start_time.elapsed();

                match download {
                    Ok(response) => {
                        let bytes = response.bytes().await;

                        data_about_feed.time_to_download_ms = Some(duration.as_millis() as u64);

                        if let Ok(bytes) = bytes {
                            let data = bytes.as_ref();
                            let hash = seahash::hash(data);

                            data_about_feed.seahash = Some(hash);
                            data_about_feed.byte_size = Some(bytes.len());

                            let gtfs =
                                gtfs_structures::Gtfs::from_reader(std::io::Cursor::new(bytes));

                            match gtfs {
                                Ok(_gtfs_data) => {
                                    data_about_feed.valid_gtfs = true;
                                }
                                Err(_e) => {
                                    data_about_feed.valid_gtfs = false;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        println!(
                            "Error with downloading {}: {}, {:?}",
                            &staticfeed.feed_id, &staticfeed.url, error
                        );
                    }
                }

                {
                    let mut progress = download_progress.lock().unwrap();
                    *progress += 1;
                    println!("Downloaded {}/{} feeds", *progress, total_feeds_to_download);
                }

                data_about_feed
            }
        }))
        .buffer_unordered(threads)
        .collect::<Vec<GirolleFeedDownloadResult>>();

    //save as ron file to args.outfile

    let feed_results = returned_feed_data.await;

    for feed in feed_results {
        feed_output.insert(feed.feed_id.clone(), feed);
    }

    let ron_string = ron::ser::to_string_pretty(&feed_output, ron::ser::PrettyConfig::new())?;

    fs::write(args.outfile, ron_string)?;

    Ok(())
}
