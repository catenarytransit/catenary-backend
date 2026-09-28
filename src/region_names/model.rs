use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type GeoId = String;
pub type LocaleId = String;
pub type UnifiedAgencyId = String;

pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GeographyConfig {
    pub schema_version: u32,
    pub fallback_locale: LocaleId,

    #[serde(default)]
    pub public_locales: Vec<LocaleId>,

    #[serde(default)]
    pub require_public_locale_slugs: bool,

    #[serde(default)]
    pub allow_unicode_slugs: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CountriesFile {
    pub schema_version: u32,

    #[serde(default, rename = "country")]
    pub countries: Vec<GeoEntityDefinition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegionsFile {
    pub schema_version: u32,
    pub parent: GeoId,

    #[serde(default, rename = "region")]
    pub regions: Vec<GeoEntityDefinition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GeoEntityDefinition {
    pub id: GeoId,
    pub kind: GeoKind,

    #[serde(default)]
    pub official_languages: Vec<LocaleId>,

    #[serde(default)]
    pub names: BTreeMap<LocaleId, String>,

    #[serde(default)]
    pub slugs: BTreeMap<LocaleId, String>,

    #[serde(default)]
    pub slug_aliases: BTreeMap<LocaleId, Vec<String>>,

    /// External identifiers such as ISO-3166-2, GADM, or Wikidata IDs.
    #[serde(default)]
    pub codes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeoKind {
    Country,
    State,
    Province,
    Region,
    Territory,
    AdministrativeDistrict,
    County,
    District,
    Municipality,
    City,
    Borough,
    Other,
}

#[derive(Debug, Clone)]
pub struct GeoNode {
    pub id: GeoId,
    pub parent: Option<GeoId>,
    pub depth: usize,
    pub kind: GeoKind,
    pub official_languages: Vec<LocaleId>,
    pub names: BTreeMap<LocaleId, String>,
    pub slugs: BTreeMap<LocaleId, String>,
    pub slug_aliases: BTreeMap<LocaleId, Vec<String>>,
    pub codes: BTreeMap<String, String>,
}

impl GeoNode {
    pub(crate) fn from_definition(
        definition: GeoEntityDefinition,
        parent: Option<GeoId>,
    ) -> Self {
        Self {
            id: definition.id,
            parent,
            depth: 0,
            kind: definition.kind,
            official_languages: definition.official_languages,
            names: definition.names,
            slugs: definition.slugs,
            slug_aliases: definition.slug_aliases,
            codes: definition.codes,
        }
    }
}
