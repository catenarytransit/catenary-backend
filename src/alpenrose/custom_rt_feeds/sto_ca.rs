use chrono::Utc;
use hex;
use reqwest;
use sha2::{Digest, Sha256};
use std::error::Error;

const HOST: &str = "https://gtfs.sto.ca/download.php";

/// Fonction principale pour télécharger le fichier GTFS.
/// Elle retourne les octets (bytes) du fichier en mémoire.
pub async fn telecharger_gtfs(
    file_id: &str,
    key: &str,
    secret: &str,
    client: reqwest::Client,
) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
    // --- 1. Génération du Hash ---
    let now = Utc::now();
    let date_iso8601 = now.format("%Y%m%dT%H%MZ").to_string();
    let salted_secret = format!("{}{}", secret, date_iso8601);
    let mut hasher = Sha256::new();
    hasher.update(salted_secret.as_bytes());
    let hash_bytes = hasher.finalize();
    let hash_value = hex::encode(hash_bytes).to_uppercase();

    // --- 2. Construction de l'URL ---
    let mut url = reqwest::Url::parse(HOST)?;
    url.query_pairs_mut()
        .append_pair("hash", &hash_value)
        .append_pair("file", file_id)
        .append_pair("key", key);

    println!("Téléchargement depuis : {}", url);

    // --- 3. Requête HTTP et Récupération des Octets ---

    let response = client.get(url).send().await?;

    if response.status().is_success() {
        let bytes = response.bytes().await?;

        println!("Téléchargement terminé. {} octets reçus.", bytes.len());

        // Retourner les octets sous forme de Bytes
        Ok(bytes.to_vec())
    } else {
        // Retourner une erreur si le statut HTTP n'est pas 2xx
        Err(format!("Échec de la requête {}: {}", file_id, response.status()).into())
    }
}

pub async fn recuperer_les_donnees_sto(
    etcd: &mut etcd_client::KvClient,
    client: &reqwest::Client,
    feed_id: &str,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let le_cle_publique = "CF6C513F1E672F014212DEE34F14E980";
    let secret = "FA56F1BC6524315CF452A0BA7BA3B855745E9A8C27CB3C3753271D1EFC4D218E";

    let donnes_vehicle =
        telecharger_gtfs("vehicule", le_cle_publique, secret, client.clone()).await;

    if let Err(erreur) = &donnes_vehicle {
        eprintln!("{:?}", erreur);
    }

    let donnes_voyage = telecharger_gtfs("trip", le_cle_publique, secret, client.clone()).await;

    if let Err(erreur) = &donnes_voyage {
        eprintln!("{:?}", erreur);
    }

    let donnes_alert = telecharger_gtfs("alert", le_cle_publique, secret, client.clone()).await;

    if let Err(erreur) = &donnes_alert {
        eprintln!("{:?}", erreur);
    }

    let fetch_assigned_node_meta =
        catenary::get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await;

        if let Err(e) = aspen_client {
            eprintln!("Échec de la connexion à Aspen à {}: {}", data.socket, e);
            return Err(e);
        }
        let aspen_client = aspen_client.unwrap();

        let tarpc_send_to_aspen = aspen_client
            .from_alpenrose(
                tarpc::context::current(),
                data.chateau_id.clone(),
                String::from(feed_id),
                donnes_vehicle.ok(),
                donnes_voyage.ok(),
                donnes_alert.ok(),
                true,
                true,
                true,
                Some(200),
                Some(200),
                Some(200),
                catenary::duration_since_unix_epoch().as_millis() as u64,
            )
            .await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gtfs_fetching() {
        let client = reqwest::Client::new();

        let le_cle_publique = "CF6C513F1E672F014212DEE34F14E980";
        let secret = "FA56F1BC6524315CF452A0BA7BA3B855745E9A8C27CB3C3753271D1EFC4D218E";

        let donnes_vehicle =
            telecharger_gtfs("vehicule", le_cle_publique, secret, client.clone()).await;

        if let Err(erreur) = &donnes_vehicle {
            eprintln!("{:?}", erreur);
        }

        let donnes_voyage = telecharger_gtfs("trip", le_cle_publique, secret, client.clone()).await;

        if let Err(erreur) = &donnes_voyage {
            eprintln!("{:?}", erreur);
        }

        let donnes_alert = telecharger_gtfs("alert", le_cle_publique, secret, client.clone()).await;

        if let Err(erreur) = &donnes_alert {
            eprintln!("{:?}", erreur);
        }
    }
}
