//! A separate, versioned resident snapshot format for processed Aspen data.
//!
//! Mutable processing and the legacy RPC types remain in `aspen_dataset`.
//! Conversion consumes the input: compressed snapshots retain no expanded trip
//! or stop vectors. Non-trip metadata is moved without recompression.
#[path = "compressed_aspen_dataset/columns.rs"]
mod columns;
#[path = "compressed_aspen_dataset/trips.rs"]
mod trips;

use crate::aspen_dataset::*;
use ahash::AHashMap;
use compact_str::CompactString;
use ecow::EcoString;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub use trips::{
    CompressedTripUpdates, RawTripRef, StopTimeEventRef, StopTimeUpdateRef, TripUpdateRef,
};

/// Explicit wire version, independent of AspenisedData and CompactFeedMessageV2.
/// Append future versions; do not change V1's serialized field order or encodings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompressedAspenisedData {
    V1(Box<AspenSnapshotV1>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenSnapshotV1 {
    metadata: AspenMetadata,
    trips: CompressedTripUpdates,
}

/// All non-trip data, moved from the ordinary dataset. Cache serialization keeps
/// the same skip/default rules as AspenisedData; compression itself retains caches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenMetadata {
    pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
    pub vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache>,
    pub vehicle_routes_cache_hash: u64,
    pub vehicle_label_to_gtfs_id: AHashMap<String, String>,
    pub trip_updates_lookup_by_trip_id_to_trip_update_ids:
        AHashMap<CompactString, Vec<CompactString>>,
    pub trip_updates_lookup_by_route_id_to_trip_update_ids:
        AHashMap<CompactString, Vec<CompactString>>,
    pub vehicle_positions_rtree_by_route_type:
        AHashMap<i16, rstar::RTree<AspenisedVehiclePositionBBox>>,
    pub aspenised_alerts: Arc<AHashMap<Arc<str>, AspenisedAlert>>,
    pub impacted_routes_alerts: AHashMap<CompactString, Box<[Arc<str>]>>,
    pub impacted_stops_alerts: AHashMap<CompactString, Box<[Arc<str>]>>,
    pub impacted_trips_alerts: AHashMap<CompactString, Box<[Arc<str>]>>,
    pub trip_id_to_vehicle_gtfs_rt_id: AHashMap<String, Vec<String>>,
    pub last_updated_time_ms: u64,
    #[serde(skip, default = "ItineraryPatternInternalCache::new")]
    pub itinerary_pattern_internal_cache: ItineraryPatternInternalCache,
    #[serde(skip, default = "CompressedTripInternalCache::new")]
    pub compressed_trip_internal_cache: CompressedTripInternalCache,
    pub stop_id_to_stop: AHashMap<CompactString, AspenisedStop>,
    pub shape_id_to_shape: AHashMap<CompactString, Option<EcoString>>,
    pub trip_modifications: AHashMap<CompactString, AspenisedTripModification>,
    pub trip_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_non_scheduled_trip_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_parent_id: AHashMap<CompactString, CompactString>,
    pub parent_id_to_children_ids: AHashMap<CompactString, Vec<CompactString>>,
}

impl From<AspenisedData> for CompressedAspenisedData {
    fn from(data: AspenisedData) -> Self {
        let AspenisedData {
            trip_updates,
            vehicle_positions,
            vehicle_routes_cache,
            vehicle_routes_cache_hash,
            vehicle_label_to_gtfs_id,
            trip_updates_lookup_by_trip_id_to_trip_update_ids,
            trip_updates_lookup_by_route_id_to_trip_update_ids,
            vehicle_positions_rtree_by_route_type,
            aspenised_alerts,
            impacted_routes_alerts,
            impacted_stops_alerts,
            impacted_trips_alerts,
            trip_id_to_vehicle_gtfs_rt_id,
            last_updated_time_ms,
            itinerary_pattern_internal_cache,
            compressed_trip_internal_cache,
            stop_id_to_stop,
            shape_id_to_shape,
            trip_modifications,
            trip_id_to_trip_modification_ids,
            stop_id_to_trip_modification_ids,
            stop_id_to_non_scheduled_trip_ids,
            stop_id_to_parent_id,
            parent_id_to_children_ids,
        } = data;
        Self::V1(Box::new(AspenSnapshotV1 {
            trips: trip_updates.into(),
            metadata: AspenMetadata {
                vehicle_positions,
                vehicle_routes_cache,
                vehicle_routes_cache_hash,
                vehicle_label_to_gtfs_id,
                trip_updates_lookup_by_trip_id_to_trip_update_ids,
                trip_updates_lookup_by_route_id_to_trip_update_ids,
                vehicle_positions_rtree_by_route_type,
                aspenised_alerts,
                impacted_routes_alerts,
                impacted_stops_alerts,
                impacted_trips_alerts,
                trip_id_to_vehicle_gtfs_rt_id,
                last_updated_time_ms,
                itinerary_pattern_internal_cache,
                compressed_trip_internal_cache,
                stop_id_to_stop,
                shape_id_to_shape,
                trip_modifications,
                trip_id_to_trip_modification_ids,
                stop_id_to_trip_modification_ids,
                stop_id_to_non_scheduled_trip_ids,
                stop_id_to_parent_id,
                parent_id_to_children_ids,
            },
        }))
    }
}

impl AspenMetadata {
    fn into_expanded(
        self,
        trip_updates: AHashMap<CompactString, AspenisedTripUpdate>,
    ) -> AspenisedData {
        let Self {
            vehicle_positions,
            vehicle_routes_cache,
            vehicle_routes_cache_hash,
            vehicle_label_to_gtfs_id,
            trip_updates_lookup_by_trip_id_to_trip_update_ids,
            trip_updates_lookup_by_route_id_to_trip_update_ids,
            vehicle_positions_rtree_by_route_type,
            aspenised_alerts,
            impacted_routes_alerts,
            impacted_stops_alerts,
            impacted_trips_alerts,
            trip_id_to_vehicle_gtfs_rt_id,
            last_updated_time_ms,
            itinerary_pattern_internal_cache,
            compressed_trip_internal_cache,
            stop_id_to_stop,
            shape_id_to_shape,
            trip_modifications,
            trip_id_to_trip_modification_ids,
            stop_id_to_trip_modification_ids,
            stop_id_to_non_scheduled_trip_ids,
            stop_id_to_parent_id,
            parent_id_to_children_ids,
        } = self;
        AspenisedData {
            trip_updates,
            vehicle_positions,
            vehicle_routes_cache,
            vehicle_routes_cache_hash,
            vehicle_label_to_gtfs_id,
            trip_updates_lookup_by_trip_id_to_trip_update_ids,
            trip_updates_lookup_by_route_id_to_trip_update_ids,
            vehicle_positions_rtree_by_route_type,
            aspenised_alerts,
            impacted_routes_alerts,
            impacted_stops_alerts,
            impacted_trips_alerts,
            trip_id_to_vehicle_gtfs_rt_id,
            last_updated_time_ms,
            itinerary_pattern_internal_cache,
            compressed_trip_internal_cache,
            stop_id_to_stop,
            shape_id_to_shape,
            trip_modifications,
            trip_id_to_trip_modification_ids,
            stop_id_to_trip_modification_ids,
            stop_id_to_non_scheduled_trip_ids,
            stop_id_to_parent_id,
            parent_id_to_children_ids,
        }
    }
}

impl CompressedAspenisedData {
    pub fn trip_updates(&self) -> &CompressedTripUpdates {
        match self {
            Self::V1(snapshot) => &snapshot.trips,
        }
    }

    pub fn metadata(&self) -> &AspenMetadata {
        match self {
            Self::V1(snapshot) => &snapshot.metadata,
        }
    }

    /// Expand only at an existing API boundary. Normal reads should use views.
    pub fn to_expanded(&self) -> AspenisedData {
        self.metadata()
            .clone()
            .into_expanded(self.trip_updates().to_expanded())
    }

    /// Moves all non-trip metadata back, expanding only the compressed trip table.
    pub fn into_expanded(self) -> AspenisedData {
        match self {
            Self::V1(snapshot) => snapshot
                .metadata
                .into_expanded(snapshot.trips.to_expanded()),
        }
    }
}

impl AspenisedData {
    pub fn into_compressed(self) -> CompressedAspenisedData {
        self.into()
    }
}

#[cfg(test)]
#[path = "compressed_aspen_dataset/tests.rs"]
mod tests;
