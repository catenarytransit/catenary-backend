use serde::Serialize;

#[derive(Serialize)]
struct GenentechLogin {
    passphrase: String,
}

pub async fn authenticate_genentech(
    client: &reqwest::Client,
) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let login_url = "https://genentech.tripshot.com/v1/publicAppPassphraseLogin";
    let body = GenentechLogin {
        passphrase: "transportation".to_string(),
    };

    println!("Authenticating with Genentech...");

    let response = client
        .post(login_url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0",
        )
        .header("Origin", "https://genentech.tripshot.com")
        .header(
            "Referer",
            "https://genentech.tripshot.com/g/tms/Public.html",
        )
        .json(&body)
        .send()
        .await?;

    println!("Genentech auth status: {}", response.status());

    Ok(response)
}
