use dashmap::DashMap;
use etcd_client::{GetOptions, WatchOptions};
use serde::de::DeserializeOwned;
use std::sync::Arc;

#[derive(Clone)]
pub struct EtcdCache<T> {
    cache: Arc<DashMap<String, T>>,
    prefix: Arc<str>,
}

impl<T: DeserializeOwned + Send + Sync + 'static + Clone> EtcdCache<T> {
    pub async fn new(
        etcd_connection_ips: Arc<crate::EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        prefix: &'static str,
    ) -> Result<Self, String> {
        let mut client = etcd_client::Client::connect(
            etcd_connection_ips.ip_addresses.as_slice(),
            (*etcd_connection_options).clone(),
        )
        .await
        .map_err(|e| format!("EtcdCache ({prefix}) failed to connect to etcd: {e:?}"))?;

        let get_opts = GetOptions::new().with_prefix();
        let get_res = client
            .get(prefix, Some(get_opts))
            .await
            .map_err(|e| format!("EtcdCache ({prefix}) get error: {e:?}"))?;

        let revision = get_res
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();

        let mut decoded = Vec::with_capacity(get_res.kvs().len());

        for kv in get_res.kvs() {
            let key = kv
                .key_str()
                .map_err(|e| format!("invalid etcd key: {e:?}"))?;

            let id = key
                .strip_prefix(prefix)
                .ok_or_else(|| format!("key {key:?} does not match prefix {prefix:?}"))?;

            let value = crate::bincode_deserialize::<T>(kv.value())
                .map_err(|e| format!("failed decoding etcd key {key}: {e:?}"))?;

            decoded.push((id.to_string(), value));
        }

        let cache = Arc::new(DashMap::new());
        let entries_count = decoded.len();
        for (id, value) in decoded {
            cache.insert(id, value);
        }

        println!(
            "EtcdCache {}: loaded {} entries at revision {}",
            prefix, entries_count, revision
        );

        let cache_clone = cache.clone();
        let prefix_str = prefix.to_string();
        tokio::spawn(async move {
            let mut next_revision = revision + 1;
            let mut is_first = true;
            loop {
                let mut client = match etcd_client::Client::connect(
                    etcd_connection_ips.ip_addresses.as_slice(),
                    (*etcd_connection_options).clone(),
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("EtcdCache ({}) connect error: {:?}", prefix_str, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                if !is_first {
                    let get_opts = GetOptions::new().with_prefix();
                    match client.get(prefix_str.as_str(), Some(get_opts)).await {
                        Ok(res) => {
                            let mut decoded = Vec::with_capacity(res.kvs().len());
                            let mut decode_ok = true;
                            for kv in res.kvs() {
                                if let Some(key_str) = kv.key_str().ok() {
                                    if let Some(id) = key_str.strip_prefix(prefix_str.as_str()) {
                                        match crate::bincode_deserialize::<T>(kv.value()) {
                                            Ok(value) => {
                                                decoded.push((id.to_string(), value));
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "EtcdCache ({}) failed decoding etcd key {}: {:?}",
                                                    prefix_str, key_str, e
                                                );
                                                decode_ok = false;
                                                break;
                                            }
                                        }
                                    } else {
                                        eprintln!(
                                            "EtcdCache ({}) key {} does not match prefix",
                                            prefix_str, key_str
                                        );
                                        decode_ok = false;
                                        break;
                                    }
                                }
                            }
                            if decode_ok {
                                cache_clone.clear();
                                for (id, value) in decoded {
                                    cache_clone.insert(id, value);
                                }
                                if let Some(header) = res.header() {
                                    next_revision = header.revision() + 1;
                                }
                            } else {
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        }
                        Err(e) => {
                            eprintln!("EtcdCache ({}) get error: {:?}", prefix_str, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }
                is_first = false;

                let watch_opts = WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(next_revision);
                match client.watch(prefix_str.as_str(), Some(watch_opts)).await {
                    Ok((mut _watcher, mut stream)) => loop {
                        match stream.message().await {
                            Ok(Some(resp)) => {
                                for event in resp.events() {
                                    if let Some(kv) = event.kv() {
                                        if let Some(key_str) = kv.key_str().ok() {
                                            if let Some(id) =
                                                key_str.strip_prefix(prefix_str.as_str())
                                            {
                                                if event.event_type() == etcd_client::EventType::Put
                                                {
                                                    match crate::bincode_deserialize::<T>(
                                                        kv.value(),
                                                    ) {
                                                        Ok(metadata) => {
                                                            cache_clone
                                                                .insert(id.to_string(), metadata);
                                                        }
                                                        Err(e) => {
                                                            eprintln!(
                                                                "EtcdCache ({}) failed decoding watch put key {}: {:?}",
                                                                prefix_str, key_str, e
                                                            );
                                                        }
                                                    }
                                                } else if event.event_type()
                                                    == etcd_client::EventType::Delete
                                                {
                                                    cache_clone.remove(id);
                                                }
                                            } else {
                                                eprintln!(
                                                    "EtcdCache ({}) watch key {} does not match prefix",
                                                    prefix_str, key_str
                                                );
                                            }
                                        }
                                    }
                                }
                                if let Some(header) = resp.header() {
                                    next_revision = header.revision() + 1;
                                }
                            }
                            Ok(None) => {
                                eprintln!("EtcdCache ({}) watch stream closed.", prefix_str);
                                break;
                            }
                            Err(e) => {
                                eprintln!("EtcdCache ({}) watch stream error: {:?}", prefix_str, e);
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("EtcdCache ({}) watch error: {:?}", prefix_str, e);
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });

        Ok(Self {
            cache,
            prefix: Arc::from(prefix),
        })
    }

    pub fn get(&self, id: &str) -> Option<T> {
        self.cache.get(id).map(|r| r.value().clone())
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, T> {
        self.cache.iter()
    }
}
