use super::{GeoId, GeographyIndex, RegionNamesError, UnifiedAgencyId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgencyRegionOverridesFile {
    pub schema_version: u32,

    #[serde(default, rename = "agency")]
    pub agencies: Vec<AgencyRegionOverride>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgencyRegionOverride {
    pub unified_agency_id: UnifiedAgencyId,

    /// Human-readable only. Never use this field as an identifier.
    #[serde(default)]
    pub label: Option<String>,

    pub mode: AgencyRegionOverrideMode,

    #[serde(default)]
    pub regions: Vec<GeoId>,

    #[serde(default)]
    pub primary_region: Option<GeoId>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgencyRegionOverrideMode {
    /// Ignore inferred regions and use exactly the configured regions.
    Lock,

    /// Keep inferred regions and add the configured regions.
    Augment,
}

#[derive(Debug, Default)]
pub struct AgencyRegionOverrides {
    by_agency: HashMap<UnifiedAgencyId, AgencyRegionOverride>,
}

impl AgencyRegionOverrides {
    pub fn from_file(
        file: AgencyRegionOverridesFile,
        geography: &GeographyIndex,
    ) -> Result<Self, RegionNamesError> {
        super::index::validate_schema_version("agency_region_overrides.toml", file.schema_version)?;

        let mut by_agency = HashMap::new();
        for agency in file.agencies {
            validate_override(&agency, geography)?;
            let id = agency.unified_agency_id.clone();
            if by_agency.insert(id.clone(), agency).is_some() {
                return Err(RegionNamesError::DuplicateAgencyOverride(id));
            }
        }

        Ok(Self { by_agency })
    }

    pub fn get(&self, unified_agency_id: &str) -> Option<&AgencyRegionOverride> {
        self.by_agency.get(unified_agency_id)
    }

    pub fn contains(&self, unified_agency_id: &str) -> bool {
        self.by_agency.contains_key(unified_agency_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&UnifiedAgencyId, &AgencyRegionOverride)> {
        self.by_agency.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAgencyGeography {
    /// Exact geographic entities assigned to the agency.
    pub assigned_regions: Vec<GeoId>,

    /// Ancestors implied by those assignments. Assigned regions themselves
    /// are not repeated here.
    pub ancestor_regions: Vec<GeoId>,

    pub primary_region: Option<GeoId>,
}

impl ResolvedAgencyGeography {
    pub fn all_region_ids(&self) -> Vec<GeoId> {
        deduplicate(
            self.ancestor_regions
                .iter()
                .chain(self.assigned_regions.iter())
                .cloned(),
        )
    }

    /// Compatibility helper for existing `level_0s`, `level_1s`, etc.
    pub fn regions_at_depth(&self, geography: &GeographyIndex, depth: usize) -> Vec<GeoId> {
        self.all_region_ids()
            .into_iter()
            .filter(|region_id| {
                geography
                    .node(region_id)
                    .is_some_and(|node| node.depth == depth)
            })
            .collect()
    }

    pub fn primary_region_at_depth(
        &self,
        geography: &GeographyIndex,
        depth: usize,
    ) -> Option<GeoId> {
        geography
            .ancestor_at_depth(self.primary_region.as_deref()?, depth)
            .ok()
            .flatten()
    }
}

pub fn resolve_agency_geography(
    geography: &GeographyIndex,
    overrides: &AgencyRegionOverrides,
    unified_agency_id: &str,
    inferred_regions: &[GeoId],
) -> Result<ResolvedAgencyGeography, RegionNamesError> {
    for region in inferred_regions {
        if !geography.contains(region) {
            return Err(RegionNamesError::UnknownRegion(region.clone()));
        }
    }

    let configured = overrides.get(unified_agency_id);

    let mut assigned_regions = match configured {
        Some(config) if config.mode == AgencyRegionOverrideMode::Lock => config.regions.clone(),
        _ => deduplicate(inferred_regions.iter().cloned()),
    };

    if let Some(config) = configured {
        if config.mode == AgencyRegionOverrideMode::Augment {
            assigned_regions.extend(config.regions.iter().cloned());
            assigned_regions = deduplicate(assigned_regions);
        }
    }

    let primary_region = configured
        .and_then(|config| config.primary_region.clone())
        .or_else(|| assigned_regions.first().cloned());

    let assigned_set = assigned_regions.iter().cloned().collect::<HashSet<_>>();
    let mut ancestor_regions = Vec::new();

    for region in &assigned_regions {
        for ancestor in geography.ancestor_ids(region)? {
            if !assigned_set.contains(&ancestor) && !ancestor_regions.contains(&ancestor) {
                ancestor_regions.push(ancestor);
            }
        }
    }

    Ok(ResolvedAgencyGeography {
        assigned_regions,
        ancestor_regions,
        primary_region,
    })
}

fn validate_override(
    agency: &AgencyRegionOverride,
    geography: &GeographyIndex,
) -> Result<(), RegionNamesError> {
    if agency.regions.is_empty() {
        return Err(RegionNamesError::AgencyOverrideHasNoRegions(
            agency.unified_agency_id.clone(),
        ));
    }

    let mut seen = HashSet::new();
    for region in &agency.regions {
        if !geography.contains(region) {
            return Err(RegionNamesError::AgencyOverrideUnknownRegion {
                agency: agency.unified_agency_id.clone(),
                region: region.clone(),
            });
        }
        if !seen.insert(region.clone()) {
            return Err(RegionNamesError::AgencyOverrideDuplicateRegion {
                agency: agency.unified_agency_id.clone(),
                region: region.clone(),
            });
        }
    }

    if let Some(primary_region) = &agency.primary_region {
        if !agency.regions.contains(primary_region) {
            return Err(RegionNamesError::AgencyOverridePrimaryRegionNotConfigured {
                agency: agency.unified_agency_id.clone(),
                region: primary_region.clone(),
            });
        }
    }

    Ok(())
}

fn deduplicate(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RegionNames::{CountriesFile, GeographyConfig, RegionsFile};

    fn geography() -> GeographyIndex {
        let config = GeographyConfig {
            schema_version: 1,
            fallback_locale: "en".to_string(),
            public_locales: vec![],
            require_public_locale_slugs: false,
            allow_unicode_slugs: true,
        };
        let countries: CountriesFile = toml::from_str(
            r#"
schema_version = 1
[[country]]
id = "DE"
kind = "country"
official_languages = ["de"]
[country.names]
en = "Germany"
de = "Deutschland"
[country.slugs]
en = "germany"
de = "deutschland"
"#,
        )
        .unwrap();
        let regions: RegionsFile = toml::from_str(
            r#"
schema_version = 1
parent = "DE"
[[region]]
id = "DE-BY"
kind = "state"
official_languages = ["de"]
[region.names]
en = "Bavaria"
de = "Bayern"
[region.slugs]
en = "bavaria"
de = "bayern"
"#,
        )
        .unwrap();
        GeographyIndex::from_files(config, countries, vec![regions]).unwrap()
    }

    #[test]
    fn lock_replaces_inference() {
        let geography = geography();
        let file: AgencyRegionOverridesFile = toml::from_str(
            r#"
schema_version = 1
[[agency]]
unified_agency_id = "deutsche_bahn"
mode = "lock"
regions = ["DE"]
primary_region = "DE"
"#,
        )
        .unwrap();
        let overrides = AgencyRegionOverrides::from_file(file, &geography).unwrap();

        let resolved = resolve_agency_geography(
            &geography,
            &overrides,
            "deutsche_bahn",
            &["DE-BY".to_string()],
        )
        .unwrap();

        assert_eq!(resolved.assigned_regions, vec!["DE"]);
        assert!(resolved.ancestor_regions.is_empty());
    }
}
