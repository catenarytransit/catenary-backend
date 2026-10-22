//! Geofabrik -> OSR data manager. Independent of the backend's heavyweight dependencies.
use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use geojson::{Feature, FeatureCollection, GeoJson, Geometry, Value};
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, ETAG, LAST_MODIFIED};
use serde::{Deserialize, Serialize};
use serde_json::{Map, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
#[command(about = "Download Geofabrik extracts and safely publish OSR datasets")]
struct Args {
    #[arg(long, default_value = "catenaryconfig.toml", global = true)]
    config: PathBuf,
    #[arg(long, global = true, help = "Re-extract even if the source is unchanged")]
    force: bool,
    #[command(subcommand)]
    command: Option<Action>,
}

#[derive(Subcommand)]
enum Action {
    /// Check for Geofabrik updates, build and publish regions.
    Run,
    /// Find the smallest configured region containing a coordinate (offline).
    Locate {
        #[arg(long)]
        lat: f64,
        #[arg(long)]
        lon: f64,
    },
}

#[derive(Deserialize)]
struct FileConfig {
    osm_manager: ManagerConfig,
}

#[derive(Deserialize)]
struct ManagerConfig {
    data_dir: PathBuf,
    temp_dir: PathBuf,
    #[serde(default = "default_extractor")]
    osr_extract: PathBuf,
    regions: Vec<String>,
}

fn default_extractor() -> PathBuf {
    PathBuf::from("osr-extract")
}

#[derive(Clone, Debug)]
struct Region {
    id: String,
    pbf_url: String,
    boundary_geojson_url: String,
    boundary_poly_url: String,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    id: String,
    pbf_url: String,
    revision: String,
    pbf_md5: Option<String>,
    boundary_url: String,
    generated_unix_seconds: u64,
    extractor: String,
    /// Fingerprint of the executable that wrote routing.bin. Older manifests
    /// have no fingerprint and must be regenerated.
    #[serde(default)]
    extractor_md5: Option<String>,
}

struct Lock {
    path: PathBuf,
    _file: File,
}

impl Lock {
    fn acquire(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(".osm_manager.lock");
        let file = OpenOptions::new().write(true).create_new(true).open(&path)
            .with_context(|| format!("another osm_manager may be running (or remove a stale lock): {}", path.display()))?;
        Ok(Self { path, _file: file })
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config: FileConfig = toml::from_str(
        &fs::read_to_string(&args.config)
            .with_context(|| format!("reading {}", args.config.display()))?,
    ).context("parsing [osm_manager] in catenaryconfig.toml")?;
    let cfg = config.osm_manager;
    ensure!(!cfg.regions.is_empty(), "osm_manager.regions must not be empty");
    ensure!(cfg.data_dir != cfg.temp_dir, "data_dir and temp_dir must be different");

    match args.command.unwrap_or(Action::Run) {
        Action::Locate { lat, lon } => {
            ensure!(lat.is_finite() && lon.is_finite() && (-90.0..=90.0).contains(&lat)
                && (-180.0..=180.0).contains(&lon), "invalid latitude or longitude");
            let region = locate(&cfg.data_dir.join("regions.geojson"), lat, lon)?;
            println!("{}", serde_json::to_string_pretty(&json!({ "region": region }))?);
            Ok(())
        }
        Action::Run => run(cfg, args.force),
    }
}

fn region_from_spec(spec: &str) -> Result<Region> {
    let normalized = if spec.starts_with("https://") {
        let url = reqwest::Url::parse(spec)?;
        ensure!(url.host_str() == Some("download.geofabrik.de")
            && url.port().is_none() && url.query().is_none()
            && url.fragment().is_none(), "not a canonical Geofabrik HTTPS URL: {spec}");
        url.path().trim_start_matches('/').to_owned()
    } else {
        let s = spec.trim_matches('/');
        if s.ends_with("-latest.osm.pbf") {
            s.to_owned()
        } else {
            format!("{s}-latest.osm.pbf")
        }
    };
    let base = normalized.strip_suffix("-latest.osm.pbf")
        .context("region must be a Geofabrik -latest.osm.pbf URL or region path")?;
    ensure!(!base.is_empty() && base.split('/').all(|part|
        !part.is_empty() && part != "." && part != ".." &&
        part.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')),
        "invalid Geofabrik region: {spec}");
    let root = "https://download.geofabrik.de/";
    Ok(Region {
        id: base.to_owned(),
        pbf_url: format!("{root}{normalized}"),
        boundary_geojson_url: format!("{root}{base}.geojson"),
        boundary_poly_url: format!("{root}{base}.poly"),
    })
}

fn client() -> Result<Client> {
    Ok(Client::builder().user_agent("catenary-osm-manager/1.0")
        .timeout(None).connect_timeout(std::time::Duration::from_secs(30))
        .build()?)
}

/// Prefer Geofabrik's checksum file; HEAD validators are a fallback.
fn remote_revision(client: &Client, region: &Region) -> Result<(String, Option<String>)> {
    if let Ok(response) = client.get(format!("{}.md5", region.pbf_url)).send() {
        if response.status().is_success() {
            let text = response.text()?;
            if let Some(md5) = text.split_whitespace().next() {
                if md5.len() == 32 && md5.bytes().all(|c| c.is_ascii_hexdigit()) {
                    let md5 = md5.to_ascii_lowercase();
                    return Ok((format!("md5:{md5}"), Some(md5)));
                }
            }
        }
    }
    let response = client.head(&region.pbf_url).send()?.error_for_status()?;
    let headers = response.headers();
    let etag = headers.get(ETAG).and_then(|v| v.to_str().ok()).unwrap_or("");
    let modified = headers.get(LAST_MODIFIED).and_then(|v| v.to_str().ok()).unwrap_or("");
    let size = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).unwrap_or("");
    ensure!(!etag.is_empty() || (!modified.is_empty() && !size.is_empty()),
        "Geofabrik did not provide an MD5 or usable HTTP validators for {}", region.id);
    Ok((format!("http:{etag}|{modified}|{size}"), None))
}

/// Detect a changed OSR serializer even when the upstream PBF is unchanged.
/// MD5 is used solely as a local cache identity, not as an authenticity check.
fn extractor_fingerprint(extractor: &Path) -> Result<String> {
    let executable = if extractor.components().count() > 1 {
        extractor.to_path_buf()
    } else {
        let path = std::env::var_os("PATH").context("PATH is unset; cannot find OSR extractor")?;
        std::env::split_paths(&path)
            .map(|dir| dir.join(extractor))
            .find(|candidate| candidate.is_file())
            .with_context(|| format!("OSR extractor not found in PATH: {}", extractor.display()))?
    };
    let mut file = File::open(&executable)
        .with_context(|| format!("reading OSR extractor {}", executable.display()))?;
    let mut digest = md5::Context::new();
    let mut buf = [0u8; 128 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        digest.consume(&buf[..n]);
    }
    Ok(format!("md5:{:x}", digest.compute()))
}

fn download_pbf(client: &Client, url: &str, destination: &Path, expected_md5: Option<&str>) -> Result<()> {
    let mut response = client.get(url).send()?.error_for_status()
        .with_context(|| format!("downloading {url}"))?;
    let expected_len = response.content_length();
    let mut file = File::create(destination)?;
    let mut digest = md5::Context::new();
    let mut count = 0u64;
    let mut buf = [0u8; 128 * 1024];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 { break; }
        file.write_all(&buf[..n])?;
        digest.consume(&buf[..n]);
        count += n as u64;
    }
    file.sync_all()?;
    ensure!(count > 0, "empty Geofabrik download: {url}");
    if let Some(len) = expected_len {
        ensure!(count == len, "truncated PBF: expected {len} bytes, received {count}");
    }
    if let Some(expected) = expected_md5 {
        let actual = format!("{:x}", digest.compute());
        ensure!(actual == expected, "PBF MD5 mismatch: expected {expected}, got {actual}");
    }
    Ok(())
}

fn extract_geometry(text: &str) -> Result<Geometry> {
    let geojson = GeoJson::from_str(text).context("invalid Geofabrik GeoJSON")?;
    let mut polygons = Vec::new();
    let mut add = |geometry: Geometry| -> Result<()> {
        match geometry.value {
            Value::Polygon(rings) => polygons.push(rings),
            Value::MultiPolygon(parts) => polygons.extend(parts),
            _ => bail!("boundary must be Polygon or MultiPolygon"),
        }
        Ok(())
    };
    match geojson {
        GeoJson::Geometry(g) => add(g)?,
        GeoJson::Feature(f) => add(f.geometry.context("boundary feature has no geometry")?)?,
        GeoJson::FeatureCollection(fc) => {
            for f in fc.features {
                if let Some(g) = f.geometry { add(g)?; }
            }
        }
    }
    ensure!(!polygons.is_empty(), "boundary contains no polygons");
    for polygon in &polygons {
        ensure!(!polygon.is_empty() && polygon.iter().all(|ring| ring.len() >= 4),
            "boundary contains an invalid polygon ring");
    }
    Ok(Geometry::new(Value::MultiPolygon(polygons)))
}

/// Geofabrik also distributes .poly boundaries. Use them if the GeoJSON URL is unavailable.
fn parse_poly(text: &str) -> Result<Geometry> {
    let mut polygons: Vec<Vec<Vec<Vec<f64>>>> = Vec::new();
    let mut ring: Vec<Vec<f64>> = Vec::new();
    let mut hole = false;
    let mut saw_header = false;
    let mut inside_ring = false;
    for line in text.lines().map(str::trim).filter(|s| !s.is_empty()) {
        if !saw_header {
            saw_header = true;
            continue; // .poly title
        }
        if line == "END" {
            if inside_ring {
                ensure!(ring.len() >= 3, "invalid .poly ring");
                if ring.first() != ring.last() {
                    ring.push(ring[0].clone());
                }
                if hole {
                    let exterior = polygons.last_mut()
                        .context(".poly hole without preceding exterior")?;
                    exterior.push(std::mem::take(&mut ring));
                } else {
                    polygons.push(vec![std::mem::take(&mut ring)]);
                }
                inside_ring = false;
            } else {
                break;
            }
        } else if !inside_ring {
            hole = line.starts_with('!');
            inside_ring = true;
        } else {
            let mut fields = line.split_whitespace();
            let lon: f64 = fields.next().context("missing longitude")?.parse()?;
            let lat: f64 = fields.next().context("missing latitude")?.parse()?;
            ensure!(lon.is_finite() && lat.is_finite(), "invalid .poly coordinate");
            ring.push(vec![lon, lat]);
        }
    }
    ensure!(!polygons.is_empty() && !inside_ring, "empty or incomplete Geofabrik .poly");
    Ok(Geometry::new(Value::MultiPolygon(polygons)))
}

fn fetch_boundary(client: &Client, region: &Region) -> Result<(Geometry, String)> {
    let response = client.get(&region.boundary_geojson_url).send()?;
    if response.status().is_success() {
        return Ok((extract_geometry(&response.text()?)?, region.boundary_geojson_url.clone()));
    }
    let response = client.get(&region.boundary_poly_url).send()?.error_for_status()
        .with_context(|| format!("downloading fallback .poly for {}", region.id))?;
    Ok((parse_poly(&response.text()?)?, region.boundary_poly_url.clone()))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

// Files opened by the pinned MOTIS OSR ways::ways()/ways::routing::read().
// The graph properties are serialized together in routing.bin; they are NOT
 // written as node_properties.bin / way_properties.bin.
const REQUIRED_OSR_FILES: &[&str] = &[
    "routing.bin",
    "node_to_osm.bin",
    "way_osm_idx.bin",
    "way_polylines_data.bin",
    "way_polylines_index.bin",
    "way_osm_nodes_data.bin",
    "way_osm_nodes_index.bin",
    "strings_idx.bin",
    "way_names.bin",
];

fn validate_osr_output(dir: &Path) -> Result<()> {
    ensure!(dir.is_dir(), "OSR output directory does not exist: {}", dir.display());
    let mut missing = Vec::new();
    let mut empty = Vec::new();
    for &name in REQUIRED_OSR_FILES {
        match fs::metadata(dir.join(name)) {
            Ok(metadata) if metadata.is_file() && metadata.len() > 0 => {}
            Ok(_) => empty.push(name),
            Err(_) => missing.push(name),
        }
    }
    if !missing.is_empty() || !empty.is_empty() {
        let files: Vec<_> = fs::read_dir(dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        bail!(
            "OSR output at {} is incomplete; missing={missing:?}; empty_or_not_regular={empty:?}; present_files={files:?}",
            dir.display()
        );
    }
    Ok(())
}

fn good_output(dir: &Path) -> bool {
    validate_osr_output(dir).is_ok()
        && dir.join("boundary.geojson").is_file()
        && dir.join("manifest.json").is_file()
}

fn nonce() -> Result<String> {
    Ok(format!("{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()))
}

/// Same-filesystem replacement; never destroy old output before a valid extract exists.
fn publish(staging: &Path, destination: &Path) -> Result<()> {
    let backup = destination.with_file_name(format!(
        ".{}.backup-{}", destination.file_name().unwrap().to_string_lossy(), nonce()?
    ));
    let had_previous = destination.exists();
    if had_previous { fs::rename(destination, &backup)?; }
    if let Err(error) = fs::rename(staging, destination) {
        if had_previous {
            fs::rename(&backup, destination).context("restoring previous OSR output after failed publish")?;
        }
        return Err(error).context("publishing OSR output");
    }
    if had_previous {
        if let Err(error) = fs::remove_dir_all(&backup) {
            eprintln!("warning: could not remove previous output {}: {error}", backup.display());
        }
    }
    Ok(())
}

fn run_region(client: &Client, cfg: &ManagerConfig, region: &Region,
              extractor_md5: &str, force: bool) -> Result<()> {
    let output = cfg.data_dir.join("regions").join(&region.id);
    let parent = output.parent().context("no output parent")?;
    fs::create_dir_all(parent)?;
    let (revision, md5) = remote_revision(client, region)?;
    let manifest_path = output.join("manifest.json");
    if !force && good_output(&output) {
        if let Ok(bytes) = fs::read(&manifest_path) {
            if let Ok(previous) = serde_json::from_slice::<Manifest>(&bytes) {
                if previous.pbf_url == region.pbf_url
                    && previous.revision == revision
                    && previous.extractor_md5.as_deref() == Some(extractor_md5)
                {
                    println!("{}: current ({revision}), skipping extract", region.id);
                    return Ok(());
                }
            }
        }
    }

    println!("{}: changed or absent ({revision}), downloading and extracting", region.id);
    let suffix = nonce()?;
    let temp = cfg.temp_dir.join(format!("osm-manager-{suffix}"));
    let staging = parent.join(format!(
        ".{}.staging-{suffix}", output.file_name().unwrap().to_string_lossy()
    ));
    fs::create_dir_all(&temp)?;
    // OSR's -o recreates the output directory, so pass a nonexistent staging path.
    let result = (|| -> Result<()> {
        let pbf = temp.join("region.osm.pbf");
        download_pbf(client, &region.pbf_url, &pbf, md5.as_deref())?;
        let (geometry, boundary_url) = fetch_boundary(client, region)?;
        let status = Command::new(&cfg.osr_extract)
            .arg("-i").arg(&pbf).arg("-o").arg(&staging)
            .status().with_context(|| format!("launching OSR extractor {}", cfg.osr_extract.display()))?;
        ensure!(status.success(), "OSR extractor failed for {}: {status}", region.id);
        // Verify expected files. Actual serializer compatibility is checked
        // by opening the output with the matching osr-ffi reader in CI.
        validate_osr_output(&staging)
            .with_context(|| format!("validating native OSR output for {}", region.id))?;
        write_json(&staging.join("boundary.geojson"), &geometry)?;
        let manifest = Manifest {
            id: region.id.clone(),
            pbf_url: region.pbf_url.clone(),
            revision,
            pbf_md5: md5,
            boundary_url,
            generated_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            extractor: cfg.osr_extract.to_string_lossy().into_owned(),
            extractor_md5: Some(extractor_md5.to_owned()),
        };
        write_json(&staging.join("manifest.json"), &manifest)?;
        publish(&staging, &output)?;
        Ok(())
    })();
    let _ = fs::remove_dir_all(&temp);
    if result.is_err() { let _ = fs::remove_dir_all(&staging); }
    result
}

fn feature_for_region(cfg: &ManagerConfig, region: &Region) -> Result<Feature> {
    let dir = cfg.data_dir.join("regions").join(&region.id);
    ensure!(good_output(&dir), "incomplete OSR output for {}", region.id);
    let manifest: Manifest = serde_json::from_slice(&fs::read(dir.join("manifest.json"))?)?;
    let geometry: Geometry = serde_json::from_slice(&fs::read(dir.join("boundary.geojson"))?)?;
    let bbox = boundary_bbox(&geometry)?;
    let mut properties = Map::new();
    properties.insert("id".to_owned(), json!(region.id));
    properties.insert("pbf_url".to_owned(), json!(manifest.pbf_url));
    properties.insert("revision".to_owned(), json!(manifest.revision));
    properties.insert("directory".to_owned(), json!(format!("regions/{}", region.id)));
    properties.insert("boundary_url".to_owned(), json!(manifest.boundary_url));
    Ok(Feature {
        bbox: Some(bbox.to_vec()),
        geometry: Some(geometry),
        id: None,
        properties: Some(properties),
        foreign_members: None,
    })
}

fn rings(geometry: &Geometry) -> Result<Vec<&Vec<Vec<f64>>>> {
    match &geometry.value {
        Value::MultiPolygon(polygons) => Ok(polygons.iter().flat_map(|p| p.iter()).collect()),
        Value::Polygon(polygon) => Ok(polygon.iter().collect()),
        _ => bail!("expected Polygon or MultiPolygon"),
    }
}

fn boundary_bbox(geometry: &Geometry) -> Result<[f64; 4]> {
    let mut b = [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY];
    for ring in rings(geometry)? {
        for point in ring {
            ensure!(point.len() >= 2 && point[0].is_finite() && point[1].is_finite(),
                "invalid boundary coordinates");
            b[0] = b[0].min(point[0]);
            b[1] = b[1].min(point[1]);
            b[2] = b[2].max(point[0]);
            b[3] = b[3].max(point[1]);
        }
    }
    ensure!(b[0].is_finite(), "empty boundary");
    Ok(b)
}

fn point_in_ring(ring: &[Vec<f64>], lat: f64, lon: f64) -> bool {
    let mut inside = false;
    if ring.len() < 3 { return false; }
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let a = &ring[i];
        let b = &ring[j];
        let intersects = ((a[1] > lat) != (b[1] > lat))
            && lon < (b[0] - a[0]) * (lat - a[1]) / (b[1] - a[1]) + a[0];
        if intersects { inside = !inside; }
        j = i;
    }
    inside
}

fn contains(geometry: &Geometry, lat: f64, lon: f64) -> bool {
    let polygons: Vec<&Vec<Vec<Vec<f64>>>> = match &geometry.value {
        Value::MultiPolygon(p) => p.iter().collect(),
        Value::Polygon(p) => vec![p],
        _ => return false,
    };
    polygons.iter().any(|p| p.first().is_some_and(|outer|
        point_in_ring(outer, lat, lon)
        && !p.iter().skip(1).any(|hole| point_in_ring(hole, lat, lon))))
}

/// Most specific (smallest bounding box) configured region wins.
fn locate(registry: &Path, lat: f64, lon: f64) -> Result<Option<String>> {
    let fc: FeatureCollection = serde_json::from_slice(
        &fs::read(registry).with_context(|| format!("reading {}", registry.display()))?
    )?;
    let mut matches = Vec::new();
    for feature in fc.features {
        if let Some(geometry) = feature.geometry {
            let bbox = boundary_bbox(&geometry)?;
            if lon < bbox[0] || lon > bbox[2] || lat < bbox[1] || lat > bbox[3] {
                continue;
            }
            if contains(&geometry, lat, lon) {
                let id = feature.properties.as_ref().and_then(|p| p.get("id"))
                    .and_then(|v| v.as_str()).context("missing region id")?;
                let size = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]);
                matches.push((size, id.to_owned()));
            }
        }
    }
    matches.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    Ok(matches.first().map(|(_, id)| id.clone()))
}

fn run(cfg: ManagerConfig, force: bool) -> Result<()> {
    fs::create_dir_all(&cfg.data_dir)?;
    fs::create_dir_all(&cfg.temp_dir)?;
    let _lock = Lock::acquire(&cfg.data_dir)?;
    let mut regions = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for spec in &cfg.regions {
        let region = region_from_spec(spec)?;
        ensure!(seen.insert(region.id.clone()), "duplicate region {}", region.id);
        regions.push(region);
    }
    let fingerprint = extractor_fingerprint(&cfg.osr_extract)?;
    println!("OSR extractor cache identity: {fingerprint}");
    let client = client()?;
    for region in &regions {
        run_region(&client, &cfg, region, &fingerprint, force)
            .with_context(|| format!("OSR region {}", region.id))?;
    }
    let features = regions.iter().map(|r| feature_for_region(&cfg, r))
        .collect::<Result<Vec<_>>>()?;
    let collection = FeatureCollection { bbox: None, features, foreign_members: None };
    let staged = cfg.data_dir.join(format!(".regions.geojson-{}.tmp", nonce()?));
    write_json(&staged, &collection)?;
    fs::rename(&staged, cfg.data_dir.join("regions.geojson"))?;
    println!("Published {} region(s) with coordinate lookup metadata", regions.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geofabrik_urls_and_paths() {
        let a = region_from_spec("https://download.geofabrik.de/europe/andorra-latest.osm.pbf").unwrap();
        assert_eq!(a.id, "europe/andorra");
        assert_eq!(a.boundary_geojson_url, "https://download.geofabrik.de/europe/andorra.geojson");
        assert_eq!(region_from_spec("north-america").unwrap().id, "north-america");
        assert!(region_from_spec("../etc").is_err());
        assert!(region_from_spec("https://evil.example/europe-latest.osm.pbf").is_err());
    }

    #[test]
    fn polygon_holes_and_specificity() {
        let shape = extract_geometry(r#"{"type":"Polygon","coordinates":[[[0,0],[5,0],[5,5],[0,5],[0,0]],[[1,1],[2,1],[2,2],[1,2],[1,1]]]}"#).unwrap();
        assert!(contains(&shape, 3.0, 3.0));
        assert!(!contains(&shape, 1.5, 1.5));
        assert!(!contains(&shape, 8.0, 8.0));
    }

    #[test]
    fn parse_geofabrik_poly() {
        let shape = parse_poly("example\n1\n 0 0\n 5 0\n 5 5\n 0 5\nEND\nEND\n").unwrap();
        assert!(contains(&shape, 2.0, 2.0));
        assert!(!contains(&shape, 8.0, 8.0));
    }

    #[test]
    fn validates_real_osr_file_layout_not_nonexistent_property_files() {
        let root = std::env::temp_dir().join(format!("osm-manager-files-{}", nonce().unwrap()));
        fs::create_dir_all(&root).unwrap();
        for &file in REQUIRED_OSR_FILES {
            fs::write(root.join(file), b"fixture data").unwrap();
        }
        // The OSR extractor writes routing.bin, NOT individual
        // node_properties.bin / way_properties.bin files.
        assert!(validate_osr_output(&root).is_ok());
        assert!(!good_output(&root)); // published output needs metadata too
        fs::write(root.join("boundary.geojson"), "{}").unwrap();
        fs::write(root.join("manifest.json"), "{}").unwrap();
        assert!(good_output(&root));

        fs::remove_file(root.join("routing.bin")).unwrap();
        let error = validate_osr_output(&root).unwrap_err().to_string();
        assert!(error.contains("routing.bin"), "{error}");
        assert!(!good_output(&root));
        fs::write(root.join("routing.bin"), b"").unwrap();
        let error = validate_osr_output(&root).unwrap_err().to_string();
        assert!(error.contains("routing.bin"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn extractor_fingerprint_changes_when_binary_changes() {
        let path = std::env::temp_dir().join(format!("osr-extractor-fingerprint-{}", nonce().unwrap()));
        fs::write(&path, b"extractor-version-a").unwrap();
        let first = extractor_fingerprint(&path).unwrap();
        assert_eq!(first.len(), 36);
        assert_eq!(first, extractor_fingerprint(&path).unwrap());
        fs::write(&path, b"extractor-version-b").unwrap();
        assert_ne!(first, extractor_fingerprint(&path).unwrap());
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn old_manifest_requires_rebuild_for_extractor_revision() {
        let source = r#"{"id":"x","pbf_url":"url","revision":"md5:x",
                          "pbf_md5":null,"boundary_url":"url",
                          "generated_unix_seconds":0,"extractor":"./osr-extract"}"#;
        let old: Manifest = serde_json::from_str(source).unwrap();
        assert_eq!(old.extractor_md5, None);
        assert_ne!(old.extractor_md5.as_deref(), Some("md5:new"));
    }

    #[test]
    fn failed_publish_preserves_existing_data() {
        let root = std::env::temp_dir().join(format!("osm-manager-test-{}", nonce().unwrap()));
        let old = root.join("old");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("keep"), "ok").unwrap();
        assert!(publish(&root.join("missing-staging"), &old).is_err());
        assert_eq!(fs::read_to_string(old.join("keep")).unwrap(), "ok");
        fs::remove_dir_all(root).unwrap();
    }
}
