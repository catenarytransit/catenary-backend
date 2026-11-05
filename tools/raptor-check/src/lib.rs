//! Compile the production routing modules without OSR, Postgres or GTFS imports.
#[path = "../../../src/routing_common/dial.rs"]
pub mod dial;
#[path = "../../../src/routing_common/interval_estimate.rs"]
pub mod interval_estimate;
#[path = "../../../src/routing_common/pong.rs"]
pub mod pong;
#[path = "../../../src/routing_common/nigiri_raptor.rs"]
pub mod nigiri_raptor;
#[path = "../../../src/routing_common/nigiri_reconstruct.rs"]
pub mod nigiri_reconstruct;
#[path = "../../../src/routing_common/profile_compact.rs"]
pub mod profile_compact;
#[path = "../../../src/routing_common/profile_compact_reconstruct.rs"]
pub mod profile_compact_reconstruct;
#[path = "../../../src/routing_common/profile_search.rs"]
pub mod profile_search;
#[path = "../../../src/routing_common/raptor_arrive_by.rs"]
pub mod raptor_arrive_by;
#[path = "../../../src/routing_common/raptor_query.rs"]
pub mod raptor_query;
#[path = "../../../src/routing_common/timetable.rs"]
pub mod timetable;
#[path = "../../../src/routing_common/osr_graph_identity.rs"]
pub mod osr_graph_identity;
#[path = "../../../src/routing_common/transfer_optimizer.rs"]
pub mod transfer_optimizer;
#[path = "../../../src/routing_common/transport_index.rs"]
pub mod transport_index;

pub mod routing_common {
    pub use crate::dial;
    pub use crate::interval_estimate;
    pub use crate::pong;
    pub use crate::nigiri_raptor;
    pub use crate::nigiri_reconstruct;
    pub use crate::profile_compact;
    pub use crate::profile_compact_reconstruct;
    pub use crate::profile_search;
    pub use crate::raptor_arrive_by;
    pub use crate::raptor_query;
    pub use crate::timetable;
    pub use crate::transfer_optimizer;
    pub use crate::transport_index;
}
