//! Localized country and administrative-region names for Catenary.
//!
//! The public identity of a geographic entity is its stable [`GeoId`].
//! Localized names and URL slugs are metadata and may change without changing
//! that identity.
//!
//! Expected data layout:
//!
//! ```text
//! geography/
//! ├── config.toml
//! ├── countries.toml
//! ├── agency_region_overrides.toml
//! └── regions/
//!     ├── BE.toml
//!     ├── DE.toml
//!     ├── DE-BY.toml
//!     └── ...
//! ```
//!
//! `regions/<PARENT_ID>.toml` contains the immediate children of that parent,
//! so the hierarchy can grow to arbitrary depth without adding Rust fields for
//! `level_2`, `level_3`, and so on.

mod agency;
mod error;
mod index;
mod loader;
mod model;

pub use agency::{
    resolve_agency_geography, AgencyRegionOverride, AgencyRegionOverrideMode,
    AgencyRegionOverrides, AgencyRegionOverridesFile, ResolvedAgencyGeography,
};
pub use error::RegionNamesError;
pub use index::{GeographyIndex, ResolvedGeoSlug};
pub use loader::{
    read_agency_region_overrides, read_config, read_countries, read_regions, RegionNamesStore,
};
pub use model::{
    CountriesFile, GeoEntityDefinition, GeoId, GeoKind, GeoNode, GeographyConfig, LocaleId,
    RegionsFile, UnifiedAgencyId, SUPPORTED_SCHEMA_VERSION,
};
