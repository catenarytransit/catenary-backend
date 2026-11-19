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

/// Read a raw GTFS-RT TripUpdates protobuf *offline* in the SoCal CI
/// fixture. The production path is still Aspen's compressed tarpc RPC.
/// This adapter uses the same Aspen descriptor/relationship conversions
/// without ever starting an Aspen worker or touching its persistent stores.
pub fn project_raw_gtfs_rt_trip_feed(
    bytes: &[u8],
) -> anyhow::Result<Vec<RoutingTripUpdate>> {
    use prost::Message;
    anyhow::ensure!(bytes.len() <= 32 * 1024 * 1024, "GTFS-RT fixture exceeds 32 MiB");
    let feed = gtfs_realtime::FeedMessage::decode(bytes)?;
    // Differential messages cannot safely be interpreted as a complete
    // region update. Birch normally returns full snapshots; refuse a delta.
    anyhow::ensure!(
        feed.header.incrementality != Some(1),
        "differential GTFS-RT is not a standalone realtime snapshot",
    );
    let mut output = Vec::new();
    for entity in &feed.entity {
        if entity.is_deleted == Some(true) {
            // Entity deletion in a differential feed is NOT trip cancellation.
            continue;
        }
        let Some(update) = entity.trip_update.as_ref() else { continue };
        let trip: crate::aspen_dataset::AspenRawTripInfo = update.trip.clone().into();
        // A raw sequence above u16 must not be silently truncated or mapped
        // to a different occurrence in the original GTFS timetable.
        // Until Aspen's wire format is widened, reject the entire entity.
        if update.stop_time_update.iter().any(|s|
            s.stop_sequence.is_some_and(|n| u16::try_from(n).is_err())
        ) {
            continue;
        }
        let service_date = trip.effective_start_date();
        let start_time = trip.effective_start_time().map(str::to_owned);
        let trip_id = trip.trip_id.clone().or_else(|| {
            trip.modified_trip.as_ref().and_then(|m| m.affected_trip_id.clone())
        });
        output.push(RoutingTripUpdate {
            update_id: entity.id.clone(),
            trip_id,
            route_id: trip.route_id,
            service_date,
            start_time,
            schedule_relationship: trip.schedule_relationship,
            delay_seconds: update.delay,
            stops: update.stop_time_update.iter().map(|stop| RoutingStopUpdate {
                sequence: stop.stop_sequence.map(|n| n as u16),
                stop_id: stop.stop_id.clone(),
                assigned_stop_id: stop.stop_time_properties.as_ref()
                    .and_then(|p| p.assigned_stop_id.clone()),
                arrival: stop.arrival.as_ref().map(|event| RoutingTimeUpdate {
                    unix_seconds: event.time,
                    delay_seconds: event.delay,
                }),
                departure: stop.departure.as_ref().map(|event| RoutingTimeUpdate {
                    unix_seconds: event.time,
                    delay_seconds: event.delay,
                }),
                schedule_relationship: crate::aspen_dataset::
                    option_i32_to_stop_time_schedule_relationship(
                        &stop.schedule_relationship,
                    ),
            }).collect(),
        });
    }
    Ok(output)
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
    #[test]
    fn raw_protobuf_fixture_decodes_exact_times_and_local_service_date() {
        use gtfs_realtime::trip_update::{StopTimeEvent, StopTimeUpdate};
        use gtfs_realtime::{FeedEntity, FeedHeader, FeedMessage, TripDescriptor, TripUpdate};
        use prost::Message;
        let message = FeedMessage {
            header: FeedHeader {
                gtfs_realtime_version: "2.0".into(),
                ..Default::default()
            },
            entity: vec![
                FeedEntity {
                    id: "rt-trip-1".into(),
                    trip_update: Some(TripUpdate {
                        trip: TripDescriptor {
                            trip_id: Some("scheduled-123".into()),
                            route_id: Some("76".into()),
                            start_date: Some("20261101".into()),
                            start_time: Some("25:00:00".into()),
                            schedule_relationship: Some(0),
                            ..Default::default()
                        },
                        stop_time_update: vec![StopTimeUpdate {
                            stop_id: Some("A".into()),
                            stop_sequence: Some(17),
                            arrival: Some(StopTimeEvent {
                                time: Some(1_794_050_001),
                                delay: Some(61),
                                ..Default::default()
                            }),
                            departure: Some(StopTimeEvent {
                                time: Some(1_794_050_101),
                                ..Default::default()
                            }),
                            schedule_relationship: Some(1),
                            ..Default::default()
                        }],
                        delay: Some(41),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                FeedEntity { id: "vehicle-only".into(), ..Default::default() },
            ],
        };
        let updates = project_raw_gtfs_rt_trip_feed(&message.encode_to_vec()).unwrap();
        assert_eq!(updates.len(), 1);
        let trip = &updates[0];
        assert_eq!(trip.update_id, "rt-trip-1");
        assert_eq!(trip.trip_id.as_deref(), Some("scheduled-123"));
        assert_eq!(trip.service_date, chrono::NaiveDate::from_ymd_opt(2026, 11, 1));
        assert_eq!(trip.start_time.as_deref(), Some("25:00:00"));
        assert_eq!(trip.delay_seconds, Some(41));
        assert_eq!(trip.stops[0].sequence, Some(17));
        assert_eq!(trip.stops[0].arrival.as_ref().unwrap().unix_seconds, Some(1_794_050_001));
        assert_eq!(trip.stops[0].departure.as_ref().unwrap().unix_seconds, Some(1_794_050_101));
        assert!(matches!(
            trip.stops[0].schedule_relationship,
            Some(AspenisedStopTimeScheduleRelationship::Skipped)
        ));
    }

    #[test]
    fn raw_protobuf_fixture_rejects_differential_and_oversize_stop_sequences() {
        use gtfs_realtime::trip_update::StopTimeUpdate;
        use gtfs_realtime::{FeedEntity, FeedHeader, FeedMessage, TripUpdate};
        use prost::Message;
        let mut message = FeedMessage {
            header: FeedHeader {
                gtfs_realtime_version: "2.0".into(),
                ..Default::default()
            },
            entity: vec![FeedEntity {
                id: "large-sequence".into(),
                trip_update: Some(TripUpdate {
                    stop_time_update: vec![StopTimeUpdate {
                        stop_sequence: Some(70_000),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        };
        assert!(project_raw_gtfs_rt_trip_feed(&message.encode_to_vec()).unwrap().is_empty());
        message.header.incrementality = Some(1);
        assert!(project_raw_gtfs_rt_trip_feed(&message.encode_to_vec()).is_err());
    }

}
