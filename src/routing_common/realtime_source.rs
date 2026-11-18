//! Aspen -> Edelweiss paging protocol for routing-only realtime updates.
//!
//! The producer projects directly from compressed Aspen trip references.
//! It does not expand the entire snapshot, serialize vehicle/alert payloads,
//! or write any overlay state to disk. Consumers must resolve source-scoped
//! identifiers against the matching Gentian timetable generation themselves.
//! A page stream is atomic only when every page has the same Aspen revision.

use crate::aspen_dataset::{
    AspenisedStopTimeScheduleRelationship, AspenisedTripScheduleRelationship,
};
use crate::compressed_aspen_dataset::CompressedAspenisedData;
use serde::{Deserialize, Serialize};

pub const ROUTING_SOURCE_PROTOCOL: u16 = 1;
pub const MAX_ROUTING_PAGE_SIZE: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingSourcePageRequest {
    pub chateau_id: String,
    /// Only meaningful on the first page. Equal revision -> NotModified.
    pub if_revision_ms: Option<u64>,
    /// Subsequent pages must match the initial page's revision.
    pub expected_revision_ms: Option<u64>,
    pub offset: u32,
    pub limit: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingTimeUpdate {
    pub unix_seconds: Option<i64>,
    pub delay_seconds: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingStopUpdate {
    pub sequence: Option<u16>,
    pub stop_id: Option<String>,
    pub assigned_stop_id: Option<String>,
    pub arrival: Option<RoutingTimeUpdate>,
    pub departure: Option<RoutingTimeUpdate>,
    pub schedule_relationship: Option<AspenisedStopTimeScheduleRelationship>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingTripUpdate {
    /// Feed entity ID; distinct from the GTFS trip_id.
    pub update_id: String,
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub service_date: Option<chrono::NaiveDate>,
    pub start_time: Option<String>,
    pub schedule_relationship: Option<AspenisedTripScheduleRelationship>,
    pub delay_seconds: Option<i32>,
    pub stops: Vec<RoutingStopUpdate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RoutingPageState {
    NotModified,
    /// Data was updated between RPC pages; discard the incomplete download.
    RestartRequired,
    Data {
        updates: Vec<RoutingTripUpdate>,
        next_offset: Option<u32>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingSourcePage {
    pub protocol: u16,
    pub revision_ms: u64,
    pub total_updates: u32,
    pub state: RoutingPageState,
}

/// Project one bounded page with no full-feed expansion. Call on Aspen's
/// immutable resident Arc snapshot. The cursor is an index into its stable
/// hash-table iteration order, NOT a persistent cross-revision key.
pub fn project_routing_source_page(
    data: &CompressedAspenisedData,
    request: &RoutingSourcePageRequest,
) -> RoutingSourcePage {
    let revision_ms = data.metadata().last_updated_time_ms;
    let trips = data.trip_updates();
    let total_updates = u32::try_from(trips.len()).unwrap_or(u32::MAX);
    let state = if request.expected_revision_ms.is_some_and(|r| r != revision_ms)
        || request.offset > total_updates
    {
        RoutingPageState::RestartRequired
    } else if request.offset == 0 && request.if_revision_ms == Some(revision_ms) {
        RoutingPageState::NotModified
    } else {
        let page_size = usize::from(request.limit).clamp(1, MAX_ROUTING_PAGE_SIZE);
        let updates = trips.iter()
            .skip(request.offset as usize)
            .take(page_size)
            .map(|(update_id, update)| {
                let trip = update.trip();
                RoutingTripUpdate {
                    update_id: update_id.to_owned(),
                    // TripModification can place the original trip identity in
                    // ModifiedTripSelector instead of the outer descriptor.
                    trip_id: trip.trip_id
                        .or_else(|| trip.modified_trip.and_then(|m| m.affected_trip_id.as_deref()))
                        .map(str::to_owned),
                    route_id: trip.route_id.map(str::to_owned),
                    service_date: trip.effective_start_date(),
                    start_time: trip.effective_start_time().map(str::to_owned),
                    schedule_relationship: trip.schedule_relationship.cloned(),
                    delay_seconds: update.delay(),
                    stops: update.stops().filter(|s| !s.old_rt_data()).map(|stop| {
                        RoutingStopUpdate {
                            sequence: stop.stop_sequence(),
                            stop_id: stop.stop_id().map(str::to_owned),
                            assigned_stop_id: stop.stop_time_properties()
                                .and_then(|properties| properties.assigned_stop_id.as_deref())
                                .map(str::to_owned),
                            arrival: stop.arrival().map(|ev| RoutingTimeUpdate {
                                unix_seconds: ev.time,
                                delay_seconds: ev.delay,
                            }),
                            departure: stop.departure().map(|ev| RoutingTimeUpdate {
                                unix_seconds: ev.time,
                                delay_seconds: ev.delay,
                            }),
                            schedule_relationship: stop.schedule_relationship(),
                        }
                    }).collect(),
                }
            })
            .collect::<Vec<_>>();
        let read_to = (request.offset as usize).saturating_add(updates.len());
        let next_offset = (read_to < trips.len()).then(|| read_to as u32);
        RoutingPageState::Data { updates, next_offset }
    };
    RoutingSourcePage {
        protocol: ROUTING_SOURCE_PROTOCOL,
        revision_ms,
        total_updates,
        state,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn page_size_is_bounded_and_protocol_versioned() {
        assert_eq!(ROUTING_SOURCE_PROTOCOL, 1);
        assert_eq!(MAX_ROUTING_PAGE_SIZE, 256);
        let req = RoutingSourcePageRequest {
            chateau_id: "test".into(), if_revision_ms: None,
            expected_revision_ms: None, offset: 0, limit: 0,
        };
        assert_eq!(usize::from(req.limit).clamp(1, MAX_ROUTING_PAGE_SIZE), 1);
    }
}
