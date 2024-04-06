/// Copyright: Kyler Chin <kyler@catenarymaps.org>
/// Catenary Transit Initiatives
/// Removal of the attribution is not allowed, as covered under the AGPL license
#[macro_use]
extern crate diesel_derive_newtype;

pub mod agency_secret;
pub mod aspen;
pub mod custom_pg_types;
pub mod enum_to_int;
pub mod gtfs_rt_handlers;
pub mod maple_syrup;
pub mod models;
pub mod postgis_to_diesel;
pub mod postgres_tools;
pub mod schema;

pub const WGS_84_SRID: u32 = 4326;
