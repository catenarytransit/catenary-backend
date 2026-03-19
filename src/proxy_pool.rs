use rand::Rng;
use rand::seq::IndexedRandom;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::OnceCell;

const PROXY_LIST_URL: &str = "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/countries/US/data.txt";

const COOLDOWN_SECS: u64 = 300;

static GLOBAL_POOL: OnceCell<ProxyPool> = OnceCell::const_new();

/// Returns the global shared proxy pool, initializing it on first call.
pub async fn global_proxy_pool() -> &'static ProxyPool {
    GLOBAL_POOL
        .get_or_init(|| async {
            match ProxyPool::new().await {
                Ok(pool) => pool,
                Err(e) => {
                    eprintln!("Failed to initialize ProxyPool: {}. Using empty pool.", e);
                    ProxyPool::empty()
                }
            }
        })
        .await
}

pub struct ProxyPool {
    proxies: Vec<String>,
    /// Epoch second when direct access resumes; 0 means "use direct".
    direct_blocked_until: AtomicU64,
}

impl ProxyPool {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let body = reqwest::get(PROXY_LIST_URL).await?.text().await?;
        let proxies: Vec<String> = body
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|l| l.to_string())
            .collect();

        if proxies.is_empty() {
            return Err("Downloaded proxy list is empty".into());
        }

        println!("ProxyPool: loaded {} proxies", proxies.len());

        Ok(Self {
            proxies,
            direct_blocked_until: AtomicU64::new(0),
        })
    }

    fn empty() -> Self {
        Self {
            proxies: Vec::new(),
            direct_blocked_until: AtomicU64::new(0),
        }
    }

    pub fn should_use_proxy(&self) -> bool {
        if self.proxies.is_empty() {
            return false;
        }
        let until = self.direct_blocked_until.load(Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        let now = crate::duration_since_unix_epoch().as_secs();
        now < until
    }

    pub fn mark_direct_failed(&self) {
        let expires = crate::duration_since_unix_epoch().as_secs() + COOLDOWN_SECS;
        self.direct_blocked_until.store(expires, Ordering::Relaxed);
    }

    pub fn pick_random_proxy_url(&self) -> &str {
        &self.proxies[rand::rng().random_range(0..self.proxies.len())]
    }

    pub fn random_proxy_client(
        &self,
    ) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
        let url = self.pick_random_proxy_url();
        let proxy = reqwest::Proxy::all(url)?;
        let client = reqwest::Client::builder()
            .proxy(proxy)
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(10))
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .build()?;
        Ok(client)
    }

    pub fn proxy_urls(&self) -> &[String] {
        &self.proxies
    }

    /// Build a reqwest Client configured with a random proxy, plus
    /// standard decompression and timeouts. Suitable for the general
    /// alpenrose fetch path where any request might need proxying.
    pub fn build_proxy_client(&self) -> Option<reqwest::Client> {
        if self.proxies.is_empty() {
            return None;
        }
        self.random_proxy_client().ok()
    }

    /// Execute a request with automatic fallback to proxied clients.
    ///
    /// `build_request` is called each attempt to produce a fresh
    /// `reqwest::RequestBuilder` from the given `reqwest::Client`.
    pub async fn proxy_request<F>(
        &self,
        direct_client: &reqwest::Client,
        build_request: F,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        if !self.should_use_proxy() {
            let result = build_request(direct_client).send().await;
            match result {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    eprintln!(
                        "Direct request failed, switching to proxies for {}s: {}",
                        COOLDOWN_SECS, e
                    );
                    self.mark_direct_failed();
                }
            }
        }

        if self.proxies.is_empty() {
            return Err("No proxies available and direct request failed".into());
        }

        let mut last_err: Box<dyn std::error::Error + Send + Sync> = "no proxies attempted".into();
        for _ in 0..3 {
            match self.random_proxy_client() {
                Ok(proxy_client) => match build_request(&proxy_client).send().await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => {
                        eprintln!("Proxied request failed: {}", e);
                        last_err = e.into();
                    }
                },
                Err(e) => {
                    eprintln!("Failed to build proxy client: {}", e);
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }
}
