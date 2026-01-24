use ahash::AHashSet;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting Aspen Alert HTML Checker...");

    // 1. Initialize Postgres Pool
    println!("Connecting to Postgres...");
    let pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;
    let mut conn = pool.get().await.map_err(|e| anyhow::anyhow!(e))?;

    // 2. Fetch all Chateaus
    println!("Fetching active chateaus...");
    use catenary::schema::gtfs::chateaus::dsl::*;
    let all_chateaus: Vec<String> = chateaus.select(chateau).load(&mut conn).await?;

    println!("Found {} chateaus.", all_chateaus.len());

    // 3. Initialize Etcd Client
    let etcd_urls_env = env::var("ETCD_URLS").unwrap_or_else(|_| "localhost:2379".to_string());
    let etcd_urls: Vec<String> = etcd_urls_env.split(',').map(|s| s.to_string()).collect();

    let etcd_username = env::var("ETCD_USERNAME").ok();
    let etcd_password = env::var("ETCD_PASSWORD").ok();

    let connect_options = if let (Some(username), Some(password)) = (etcd_username, etcd_password) {
        Some(etcd_client::ConnectOptions::new().with_user(username, password))
    } else {
        None
    };

    println!("Connecting to Etcd at {:?}...", etcd_urls);
    let mut etcd_client = etcd_client::Client::connect(etcd_urls, connect_options).await?;

    // 4. Iterate and Check Alerts
    for chateau_id in all_chateaus {
        process_chateau(&chateau_id, &mut etcd_client).await;
    }

    println!("Done.");
    Ok(())
}

async fn process_chateau(chateau_id: &str, etcd_client: &mut etcd_client::Client) {
    // resolve aspen worker from etcd
    let key = format!("/aspen_assigned_chateaux/{}", chateau_id);
    let resp = match etcd_client.get(key.as_str(), None).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error fetching etcd key for {}: {}", chateau_id, e);
            return;
        }
    };

    let kv = match resp.kvs().first() {
        Some(k) => k,
        None => {
            // println!("No Aspen worker assigned for {}", chateau_id);
            return;
        }
    };

    let metadata: ChateauMetadataEtcd = match catenary::bincode_deserialize(kv.value()) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error deserializing metadata for {}: {}", chateau_id, e);
            return;
        }
    };

    // connect to aspen rpc
    let client = match catenary::aspen::lib::spawn_aspen_client_from_ip(&metadata.socket).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Error connecting to Aspen RPC for {} at {:?}: {}",
                chateau_id, metadata.socket, e
            );
            return;
        }
    };

    // fetch alerts
    let alerts_map = match client
        .get_all_alerts(tarpc::context::current(), chateau_id.to_string())
        .await
    {
        Ok(Some(alerts)) => alerts,
        Ok(None) => return, // No alerts
        Err(e) => {
            eprintln!("RPC Error fetching alerts for {}: {}", chateau_id, e);
            return;
        }
    };

    if alerts_map.is_empty() {
        return;
    }

    // check for html
    for (alert_id, alert) in alerts_map {
        check_alert_for_html(chateau_id, &alert_id, &alert);
    }
}

fn check_alert_for_html(chateau_id: &str, alert_id: &str, alert: &AspenisedAlert) {
    let mut found_html = false;

    if let Some(header) = &alert.header_text {
        for trans in &header.translation {
            if contains_html(&trans.text) {
                println!(
                    "[{}] Alert {} (Header - {}): Found HTML: \"{}\"",
                    chateau_id,
                    alert_id,
                    trans.language.as_deref().unwrap_or("?"),
                    truncate(&trans.text, 100)
                );
                found_html = true;
            }
        }
    }

    if let Some(desc) = &alert.description_text {
        for trans in &desc.translation {
            if contains_html(&trans.text) {
                println!(
                    "[{}] Alert {} (Description - {}): Found HTML: \"{}\"",
                    chateau_id,
                    alert_id,
                    trans.language.as_deref().unwrap_or("?"),
                    truncate(&trans.text, 100)
                );
                found_html = true;
            }
        }
    }
}

fn contains_html(text: &str) -> bool {
    // Simple heuristic: look for <tag> pattern
    // A regex like <[^>]+> is common, but might be too broad.
    // Let's look for known tags or just general start/end tags.
    // Ideally we'd use a parser, but quick check:

    // Check for common tags
    let tags = [
        "<br", "<p", "<div", "<span", "<b", "<i", "<strong", "<em", "<a ", "<ul", "<li", "<h1",
        "<h2", "<h3", "href=",
    ];
    let lower = text.to_lowercase();
    for tag in tags {
        if lower.contains(tag) {
            return true;
        }
    }

    // Check for closing tags
    if lower.contains("/>") || lower.contains("</") {
        return true;
    }

    false
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() > max_chars {
        s.chars().take(max_chars).collect::<String>() + "..."
    } else {
        s.to_string()
    }
}
