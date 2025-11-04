pub struct GirolleFeedDownloadResult {
    feed_id: String,
    seahash: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut feed_output: BTreeMap<String, GirolleFeedDownloadResult> = BTreeMap::new();

    

    Ok(())
}