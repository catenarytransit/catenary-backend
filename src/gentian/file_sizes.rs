//! On-disk timetable sizes for configured routing regions.
//!
//! These are filesystem lengths of published `.bin` files, not estimates from
//! the compiler's in-memory columns. Missing regions are reported explicitly.
use anyhow::{Context, Result, ensure};
use catenary::routing_common::routing_regions::RoutingRegionsConfig;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
struct RegionSize {
    id: String,
    path: PathBuf,
    bytes: Option<u64>,
}

fn file_size(path: &Path) -> Result<Option<u64>> {
    match fs::metadata(path) {
        Ok(meta) => {
            ensure!(
                meta.is_file(),
                "timetable path is not a file: {}",
                path.display()
            );
            Ok(Some(meta.len()))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("stat timetable {}", path.display())),
    }
}

fn configured_sizes(config: &RoutingRegionsConfig, config_path: &Path, dir: Option<&Path>) -> Result<Vec<RegionSize>> {
    config
        .regions
        .iter()
        .map(|region| {
            let path = config.timetable_path(config_path, &region.id, dir)?;
            Ok(RegionSize {
                id: region.id.clone(),
                bytes: file_size(&path)?,
                path,
            })
        })
        .collect()
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

/// Called only after successful publication, so the size belongs to the
/// actual final file and never to an in-progress staging file.
pub(super) fn print_published(region_id: &str, output: &Path) -> Result<()> {
    let bytes = file_size(output)?
        .with_context(|| format!("published timetable does not exist: {}", output.display()))?;
    println!(
        "Gentian timetable size — region {}: {} bytes ({}) [{}]",
        region_id,
        bytes,
        human_bytes(bytes),
        output.display(),
    );
    Ok(())
}

/// List every configured region without opening Postgres or mapping timetables.
/// Does not silently count absent region files as zero-byte timetables.
pub(super) fn print_configured(config: &RoutingRegionsConfig, config_path: &Path, dir: Option<&Path>) -> Result<()> {
    let sizes = configured_sizes(config, config_path, dir)?;
    println!("Timetable file sizes for configured regions:");
    let mut available = 0usize;
    let mut total = 0u64;
    for item in &sizes {
        match item.bytes {
            Some(bytes) => {
                total = total
                    .checked_add(bytes)
                    .context("timetable size total overflow")?;
                available += 1;
                println!("  {}: {} bytes ({})", item.id, bytes, human_bytes(bytes));
            }
            None => println!("  {}: missing [{}]", item.id, item.path.display()),
        }
    }
    println!(
        "Total published timetables: {} bytes ({}) across {}/{} configured regions",
        total,
        human_bytes(total),
        available,
        sizes.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::routing_common::routing_regions::RoutingRegion;

    fn config() -> RoutingRegionsConfig {
        RoutingRegionsConfig {
            schema_version: 1,
            country_boundaries: None,
            data_dir: PathBuf::from("compiled/routing"),
            regions: ["alpha", "beta", "gamma"]
                .into_iter()
                .map(|id| RoutingRegion {
                    id: id.to_owned(),
                    name: id.to_owned(),
                    timetable_path: None,
                    geojson: vec![],
                    countries: vec!["US".to_owned()],
                })
                .collect(),
        }
    }

    #[test]
    fn formats_binary_size_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.00 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.00 GiB");
    }

    #[test]
    fn enumerates_configured_regions_and_missing_files() {
        let dir = std::env::temp_dir().join(format!("gentian-sizes-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("alpha.bin"), vec![0u8; 1234]).unwrap();
        fs::write(dir.join("gamma.bin"), vec![0u8; 789]).unwrap();
        let sizes = configured_sizes(&config(), Path::new("routing_regions.toml"), Some(&dir)).unwrap();
        assert_eq!(
            sizes.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        assert_eq!(
            sizes.iter().map(|s| s.bytes).collect::<Vec<_>>(),
            vec![Some(1234), None, Some(789)]
        );
        assert_eq!(sizes.iter().filter_map(|s| s.bytes).sum::<u64>(), 2023);
        print_configured(&config(), Path::new("routing_regions.toml"), Some(&dir)).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_directory_masquerading_as_timetable() {
        let dir = std::env::temp_dir().join(format!("gentian-sizes-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        fs::create_dir(dir.join("alpha.bin")).unwrap();
        assert!(configured_sizes(&config(), &dir).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
