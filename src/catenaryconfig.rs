use serde::Deserialize;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::OnceLock;

static CATENARY_CONFIG: OnceLock<CatenaryConfig> = OnceLock::new();

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CatenaryConfig {
    #[serde(default)]
    pub maple: MapleConfig,
    #[serde(default)]
    pub aspen: AspenConfig,
    #[serde(default)]
    pub aspenleader: AspenLeaderConfig,
    #[serde(default)]
    pub alpenrose: AlpenroseConfig,
    #[serde(default)]
    pub birch: BirchConfig,
    #[serde(default)]
    pub spruce: SpruceConfig,
    #[serde(default)]
    pub ramonda: RamondaConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub elasticsearch: ElasticsearchConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MapleConfig {
    pub transitland: Option<String>,
    pub use_girolle: Option<bool>,
    pub no_elastic: Option<bool>,
    pub delete_before_ingest: Option<bool>,
    pub only_feed_ids: Option<Vec<String>>,
    pub only_feed_id: Option<String>,
    pub gtfs_zip_temp: Option<String>,
    pub gtfs_uncompressed_temp: Option<String>,
    pub threads_gtfs: Option<usize>,
    pub force_ingest_all: Option<bool>,
    pub bypass_temp_delete: Option<bool>,
    pub discord_log: Option<String>,
    #[serde(default)]
    pub transitland_download: TransitlandDownloadConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TransitlandDownloadConfig {
    pub download_threads: Option<usize>,
    pub croatia_npt_username: Option<String>,
    pub croatia_npt_password: Option<String>,
    pub grand_lyon_username: Option<String>,
    pub grand_lyon_password: Option<String>,
    pub de_username: Option<String>,
    pub de_password: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AspenConfig {
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
    pub channels: Option<usize>,
    pub alpenrose_thread_count: Option<usize>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AspenLeaderConfig {
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlpenroseConfig {
    pub request_limit: Option<usize>,
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
    pub only_feed_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BirchConfig {
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
    pub worker_amount: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PostgresConfig {
    pub database_url: Option<String>,
    pub max_connections: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SpruceConfig {
    pub worker_amount: Option<usize>,
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RamondaConfig {
    pub worker_amount: Option<usize>,
    pub etcd_urls: Option<Vec<String>>,
    pub etcd_username: Option<String>,
    pub etcd_password: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ElasticsearchConfig {
    pub url: Option<String>,
    pub urls: Option<Vec<String>>,
}

pub fn config() -> &'static CatenaryConfig {
    CATENARY_CONFIG.get_or_init(load_config)
}

fn load_config() -> CatenaryConfig {
    let path =
        std::env::var("CATENARYCONFIG").unwrap_or_else(|_| "catenaryconfig.toml".to_string());

    let mut config = match fs::read_to_string(Path::new(&path)) {
        Ok(contents) => match toml::from_str::<CatenaryConfig>(&contents) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("Failed to parse {}: {}", path, error);
                CatenaryConfig::default()
            }
        },
        Err(error) if error.kind() == ErrorKind::NotFound => CatenaryConfig::default(),
        Err(error) => {
            eprintln!("Failed to read {}: {}", path, error);
            CatenaryConfig::default()
        }
    };

    config.apply_env_overrides();
    config
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_comma_separated(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

impl CatenaryConfig {
    fn apply_env_overrides(&mut self) {
        self.maple.apply_env_overrides();
        self.aspen.apply_env_overrides();
        self.aspenleader.apply_env_overrides();
        self.alpenrose.apply_env_overrides();
        self.birch.apply_env_overrides();
        self.spruce.apply_env_overrides();
        self.ramonda.apply_env_overrides();
        self.postgres.apply_env_overrides();
        self.elasticsearch.apply_env_overrides();
    }
}

impl MapleConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("TRANSITLAND") {
            self.transitland = Some(value);
        }

        if let Ok(value) = std::env::var("USE_GIROLLE") {
            self.use_girolle = parse_bool(&value).or(self.use_girolle);
        }

        if let Ok(value) = std::env::var("NO_ELASTIC") {
            self.no_elastic = parse_bool(&value).or(self.no_elastic);
        }

        if let Ok(value) = std::env::var("DELETE_BEFORE_INGEST") {
            self.delete_before_ingest = parse_bool(&value).or(self.delete_before_ingest);
        }

        if let Ok(value) = std::env::var("ONLY_FEED_IDS") {
            self.only_feed_ids = Some(parse_comma_separated(&value));
        } else if let Ok(value) = std::env::var("ONLY_FEED_ID") {
            self.only_feed_id = Some(value);
        }

        if let Ok(value) = std::env::var("GTFS_ZIP_TEMP") {
            self.gtfs_zip_temp = Some(value);
        }

        if let Ok(value) = std::env::var("GTFS_UNCOMPRESSED_TEMP") {
            self.gtfs_uncompressed_temp = Some(value);
        }

        if let Ok(value) = std::env::var("THREADS_GTFS") {
            self.threads_gtfs = value.parse::<usize>().ok().or(self.threads_gtfs);
        }

        if let Ok(value) = std::env::var("FORCE_INGEST_ALL") {
            self.force_ingest_all = parse_bool(&value).or(Some(true));
        }

        if let Ok(value) = std::env::var("BYPASS_TEMP_DELETE") {
            self.bypass_temp_delete = parse_bool(&value).or(Some(true));
        }

        if let Ok(value) = std::env::var("DISCORD_LOG") {
            self.discord_log = Some(value);
        }
    }
}

impl TransitlandDownloadConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("DOWNLOAD_THREADS") {
            self.download_threads = value.parse::<usize>().ok().or(self.download_threads);
        }

        if let Ok(value) = std::env::var("CROATIA_NPT_USERNAME") {
            self.croatia_npt_username = Some(value);
        }

        if let Ok(value) = std::env::var("CROATIA_NPT_PASSWORD") {
            self.croatia_npt_password = Some(value);
        }

        if let Ok(value) = std::env::var("GRAND_LYON_USERNAME") {
            self.grand_lyon_username = Some(value);
        }

        if let Ok(value) = std::env::var("GRAND_LYON_PASSWORD") {
            self.grand_lyon_password = Some(value);
        }

        if let Ok(value) = std::env::var("DE_USERNAME") {
            self.de_username = Some(value);
        }

        if let Ok(value) = std::env::var("DE_PASSWORD") {
            self.de_password = Some(value);
        }
    }
}

impl AspenConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }

        if let Ok(value) = std::env::var("CHANNELS") {
            self.channels = value.parse::<usize>().ok().or(self.channels);
        }

        if let Ok(value) = std::env::var("ALPENROSETHREADCOUNT") {
            self.alpenrose_thread_count =
                value.parse::<usize>().ok().or(self.alpenrose_thread_count);
        }

        if let Ok(value) = std::env::var("PORT") {
            self.port = value.parse::<u16>().ok().or(self.port);
        }
    }
}

impl AspenLeaderConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }
    }
}

impl AlpenroseConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("REQUEST_LIMIT") {
            self.request_limit = value.parse::<usize>().ok().or(self.request_limit);
        }

        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }

        if let Ok(value) = std::env::var("ONLY_FEED_IDS") {
            self.only_feed_ids = Some(parse_comma_separated(&value));
        }
    }
}

impl BirchConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("WORKER_AMOUNT") {
            self.worker_amount = value.parse::<usize>().ok().or(self.worker_amount);
        }

        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }
    }
}

impl PostgresConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("DATABASE_URL") {
            self.database_url = Some(value);
        }

        if let Ok(value) = std::env::var("POSTGRES_MAX_CONNECTIONS") {
            self.max_connections = value.parse::<u32>().ok().or(self.max_connections);
        }
    }
}

impl SpruceConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("WORKER_AMOUNT") {
            self.worker_amount = value.parse::<usize>().ok().or(self.worker_amount);
        }

        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }
    }
}

impl RamondaConfig {
    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("WORKER_AMOUNT") {
            self.worker_amount = value.parse::<usize>().ok().or(self.worker_amount);
        }

        if let Ok(value) = std::env::var("ETCD_URLS") {
            self.etcd_urls = Some(parse_comma_separated(&value));
        }

        if let Ok(value) = std::env::var("ETCD_USERNAME") {
            self.etcd_username = Some(value);
        }

        if let Ok(value) = std::env::var("ETCD_PASSWORD") {
            self.etcd_password = Some(value);
        }

        if let Ok(value) = std::env::var("PORT") {
            self.port = value.parse::<u16>().ok().or(self.port);
        }
    }
}

impl ElasticsearchConfig {
    pub fn get_urls(&self) -> Vec<String> {
        if let Some(urls) = &self.urls {
            urls.clone()
        } else if let Some(url) = &self.url {
            vec![url.clone()]
        } else {
            vec!["http://localhost:9200".to_string()]
        }
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("ELASTICSEARCH_URLS") {
            self.urls = Some(parse_comma_separated(&value));
        } else if let Ok(value) = std::env::var("ELASTICSEARCH_URL") {
            self.urls = Some(parse_comma_separated(&value));
        }
    }
}
