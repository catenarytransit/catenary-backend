use super::{GeoId, LocaleId, UnifiedAgencyId};
use std::io;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegionNamesError {
    #[error("failed to read RegionNames file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse RegionNames TOML file {path}: {source}")]
    Toml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error(
        "unsupported RegionNames schema version in {location}: expected {expected}, got {actual}"
    )]
    UnsupportedSchemaVersion {
        location: String,
        expected: u32,
        actual: u32,
    },

    #[error("duplicate geography id: {0}")]
    DuplicateGeoId(GeoId),

    #[error("unknown geographic region: {0}")]
    UnknownRegion(GeoId),

    #[error("region {region} references unknown parent geographic region {parent}")]
    UnknownParent { region: GeoId, parent: GeoId },

    #[error("region {0} cannot be its own parent")]
    SelfParent(GeoId),

    #[error("geography hierarchy contains a cycle involving {0}")]
    Cycle(GeoId),

    #[error("region {region} does not have a name for fallback locale {locale}")]
    MissingFallbackName { region: GeoId, locale: LocaleId },

    #[error(
        "region {region} declares {locale} as an official language but has no name for it"
    )]
    MissingOfficialName { region: GeoId, locale: LocaleId },

    #[error("region {region} is missing a slug for required public locale {locale}")]
    MissingPublicSlug { region: GeoId, locale: LocaleId },

    #[error("region {region} has an empty slug for locale {locale}")]
    EmptySlug { region: GeoId, locale: LocaleId },

    #[error("region {region} has invalid slug {slug:?} for locale {locale}: {reason}")]
    InvalidSlug {
        region: GeoId,
        locale: LocaleId,
        slug: String,
        reason: String,
    },

    #[error(
        "slug collision under parent {parent:?}: locale {locale}, slug {slug:?}, regions {first_region} and {second_region}"
    )]
    DuplicateSlug {
        parent: Option<GeoId>,
        locale: LocaleId,
        slug: String,
        first_region: GeoId,
        second_region: GeoId,
    },

    #[error("cannot generate localized path for {region}: no slug exists for locale {locale}")]
    MissingSlugForPath { region: GeoId, locale: LocaleId },

    #[error("duplicate agency region override for unified agency {0}")]
    DuplicateAgencyOverride(UnifiedAgencyId),

    #[error("agency region override for {0} must contain at least one region")]
    AgencyOverrideHasNoRegions(UnifiedAgencyId),

    #[error("agency region override for {agency} references unknown region {region}")]
    AgencyOverrideUnknownRegion {
        agency: UnifiedAgencyId,
        region: GeoId,
    },

    #[error("agency region override for {agency} contains region {region} more than once")]
    AgencyOverrideDuplicateRegion {
        agency: UnifiedAgencyId,
        region: GeoId,
    },

    #[error(
        "agency region override for {agency} uses primary region {region}, but that region is not present in its configured regions"
    )]
    AgencyOverridePrimaryRegionNotConfigured {
        agency: UnifiedAgencyId,
        region: GeoId,
    },
}
