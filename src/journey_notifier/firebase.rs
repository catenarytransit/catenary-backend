//! OAuth2 service-account authentication and FCM HTTP v1 delivery.
//! The private key is only read on this server. Never include it in an Android build.
use anyhow::{Context, Result, bail};
use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}
fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_owned()
}
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'static str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}
#[derive(Deserialize)]
struct AccessToken {
    access_token: String,
    expires_in: Option<u64>,
}

pub struct Firebase {
    project_id: String,
    credentials: ServiceAccount,
    http: reqwest::Client,
    token: Mutex<Option<(String, Instant)>>,
}
impl Firebase {
    pub fn new(project_id: String, credential_path: &str) -> Result<Self> {
        anyhow::ensure!(
            !project_id.trim().is_empty(),
            "Firebase project_id is missing"
        );
        let contents = fs::read_to_string(credential_path)
            .with_context(|| format!("reading Firebase credentials at {credential_path}"))?;
        let credentials: ServiceAccount =
            serde_json::from_str(&contents).context("parsing Firebase service account JSON")?;
        anyhow::ensure!(
            credentials.token_uri.starts_with("https://"),
            "OAuth token_uri must use HTTPS"
        );
        Ok(Self {
            project_id,
            credentials,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?,
            token: Mutex::new(None),
        })
    }

    async fn bearer(&self) -> Result<String> {
        let mut cache = self.token.lock().await;
        if let Some((token, expires)) = cache.as_ref() {
            if Instant::now() < *expires {
                return Ok(token.clone());
            }
        }
        let now = Utc::now().timestamp();
        let claims = Claims {
            iss: &self.credentials.client_email,
            scope: "https://www.googleapis.com/auth/firebase.messaging",
            aud: &self.credentials.token_uri,
            iat: now,
            exp: now + 3500,
        };
        let jwt = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(self.credentials.private_key.as_bytes())
                .context("invalid Firebase service-account private_key")?,
        )?;
        let res = self
            .http
            .post(&self.credentials.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ])
            .send()
            .await
            .context("Firebase OAuth exchange failed")?;
        let status = res.status();
        if !status.is_success() {
            bail!(
                "Firebase OAuth HTTP {status}: {}",
                res.text().await.unwrap_or_default()
            );
        }
        let access: AccessToken = res.json().await?;
        let expires = Instant::now()
            + Duration::from_secs(
                access
                    .expires_in
                    .unwrap_or(3600)
                    .saturating_sub(120)
                    .max(60),
            );
        *cache = Some((access.access_token.clone(), expires));
        Ok(access.access_token)
    }

    pub async fn send(
        &self,
        device_token: &str,
        event_id: &str,
        journey_id: &str,
        kind: &str,
        title: &str,
        body: &str,
    ) -> Result<()> {
        let bearer = self.bearer().await?;
        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            self.project_id
        );
        let message = json!({
            "message": {
                "token": device_token,
                "notification": { "title": title, "body": body },
                "data": {
                    "eventId": event_id,
                    "journeyId": journey_id,
                    "eventType": kind
                },
                "android": { "priority": "HIGH", "notification": { "channel_id": "journey_changes" } }
            }
        });
        let response = self
            .http
            .post(&url)
            .bearer_auth(bearer)
            .json(&message)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "FCM HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }
}
