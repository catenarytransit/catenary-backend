use reqwest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http_client = reqwest::Client::new();
    let y = 47.37;
    let x = 8.54;
    
    let req = http_client
        .get("https://cypress.catenarymaps.org/v2/reverse")
        .query(&[
            ("point.lat", y),
            ("point.lon", x),
            ("size", 10.0),
        ])
        .build()?;
        
    println!("URL: {}", req.url());
    Ok(())
}
