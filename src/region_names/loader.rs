use super::{
    AgencyRegionOverrides, AgencyRegionOverridesFile, CountriesFile, GeographyConfig,
    GeographyIndex, RegionNamesError, RegionsFile, SUPPORTED_SCHEMA_VERSION,
};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct RegionNamesStore {
    pub geography: GeographyIndex,
    pub agency_overrides: AgencyRegionOverrides,
}

impl RegionNamesStore {
    /// Loads the canonical RegionNames directory.
    ///
    /// Country definitions are normally stored as one TOML file per country in
    /// `countries/`. For compatibility with the original RegionNames layout,
    /// `countries.toml` is still accepted when the directory does not exist.
    pub fn load_from_dir(root: impl AsRef<Path>) -> Result<Self, RegionNamesError> {
        let root = root.as_ref();
        let config: GeographyConfig = read_toml(&root.join("config.toml"))?;

        let countries_directory = root.join("countries");
        let countries = if countries_directory.is_dir() {
            read_country_files(&countries_directory)?
        } else {
            read_toml(&root.join("countries.toml"))?
        };

        let region_files = read_region_files(&root.join("regions"))?;

        let geography = GeographyIndex::from_files(config, countries, region_files)?;
        let overrides_path = root.join("agency_region_overrides.toml");
        let agency_overrides = if overrides_path.exists() {
            AgencyRegionOverrides::from_file(read_toml(&overrides_path)?, &geography)?
        } else {
            AgencyRegionOverrides::default()
        };

        Ok(Self {
            geography,
            agency_overrides,
        })
    }
}

pub fn read_config(path: impl AsRef<Path>) -> Result<GeographyConfig, RegionNamesError> {
    read_toml(path.as_ref())
}

pub fn read_countries(path: impl AsRef<Path>) -> Result<CountriesFile, RegionNamesError> {
    read_toml(path.as_ref())
}

pub fn read_regions(path: impl AsRef<Path>) -> Result<RegionsFile, RegionNamesError> {
    read_toml(path.as_ref())
}

pub fn read_agency_region_overrides(
    path: impl AsRef<Path>,
) -> Result<AgencyRegionOverridesFile, RegionNamesError> {
    read_toml(path.as_ref())
}

fn read_country_files(directory: &Path) -> Result<CountriesFile, RegionNamesError> {
    let paths = read_toml_paths(directory)?;
    let mut countries = Vec::new();

    for path in paths {
        let country_file: CountriesFile = read_toml(&path)?;
        if country_file.schema_version != SUPPORTED_SCHEMA_VERSION {
            return Err(RegionNamesError::UnsupportedSchemaVersion {
                location: path.display().to_string(),
                expected: SUPPORTED_SCHEMA_VERSION,
                actual: country_file.schema_version,
            });
        }
        countries.extend(country_file.countries);
    }

    Ok(CountriesFile {
        schema_version: SUPPORTED_SCHEMA_VERSION,
        countries,
    })
}

fn read_region_files(directory: &Path) -> Result<Vec<RegionsFile>, RegionNamesError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }

    read_toml_paths(directory)?
        .iter()
        .map(|path| read_toml(path))
        .collect()
}

fn read_toml_paths(directory: &Path) -> Result<Vec<PathBuf>, RegionNamesError> {
    let entries = fs::read_dir(directory).map_err(|source| RegionNamesError::Io {
        path: directory.to_path_buf(),
        source,
    })?;

    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| RegionNamesError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("toml")
        {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}

fn read_toml<T: DeserializeOwned>(path: &Path) -> Result<T, RegionNamesError> {
    let contents = fs::read_to_string(path).map_err(|source| RegionNamesError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    toml::from_str(&contents).map_err(|source| RegionNamesError::Toml {
        path: path.to_path_buf(),
        source,
    })
}
