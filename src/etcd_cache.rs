use dashmap::DashMap;
use etcd_client::{GetOptions, WatchOptions};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone)]
pub struct EtcdCache<T> {
    pub cache: Arc<DashMap<String, T>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned + Send + Sync + 'static + Clone> EtcdCache<T> {
    pub async fn new(
        etcd_connection_ips: Arc<crate::EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        prefix: &'static str,
    ) -> Result<Self, String> {
        let cache = Arc::new(DashMap::new());
        let cache_clone = cache.clone();
        let prefix_str = prefix.to_string();

        tokio::spawn(async move {
            loop {
                // Connect to etcd
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

                let get_opts = GetOptions::new().with_prefix();
                let get_res = client.get(prefix_str.as_str(), Some(get_opts)).await;

                let mut watch_rev = 0;
                match get_res {
                    Ok(res) => {
                        cache_clone.clear();
                        if let Some(header) = res.header() {
                            watch_rev = header.revision();
                        }

                        for kv in res.kvs() {
                            let key_str = kv.key_str().unwrap_or("");
                            if let Some(id) = key_str.strip_prefix(prefix_str.as_str()) {
                                if let Ok(metadata) = crate::bincode_deserialize::<T>(kv.value()) {
                                    cache_clone.insert(id.to_string(), metadata);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("EtcdCache ({}) get error: {:?}", prefix_str, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                }

                let watch_opts = WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(watch_rev + 1);
                match client.watch(prefix_str.as_str(), Some(watch_opts)).await {
                    Ok((mut _watcher, mut stream)) => loop {
                        match stream.message().await {
                            Ok(Some(resp)) => {
                                for event in resp.events() {
                                    if let Some(kv) = event.kv() {
                                        let key_str = kv.key_str().unwrap_or("");
                                        if let Some(id) = key_str.strip_prefix(prefix_str.as_str())
                                        {
                                            if event.event_type() == etcd_client::EventType::Put {
                                                if let Ok(metadata) =
                                                    crate::bincode_deserialize::<T>(kv.value())
                                                {
                                                    cache_clone.insert(id.to_string(), metadata);
                                                }
                                            } else if event.event_type()
                                                == etcd_client::EventType::Delete
                                            {
                                                cache_clone.remove(id);
                                            }
                                        }
                                    }
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
            _marker: PhantomData,
        })
    }

    pub fn get(&self, id: &str) -> Option<T> {
        self.cache.get(id).map(|r| r.value().clone())
    }
}
