//! OJP-inspired public-transport mode filters for RAPTOR.
//! They are applied before boarding and during deferred reconstruction.
use crate::routing_common::realtime_overlay::RtPatternRef;
use crate::routing_common::realtime_region::{RegionRealtimeSnapshot, RegionRtRun};
use crate::routing_common::timetable::{Clasz, RouteIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PtMode {
    Train,
    Metro,
    Tram,
    Bus,
    Coach,
    Boat,
    Funicular,
    AerialTramway,
}

impl PtMode {
    fn mask(self) -> u16 {
        let classes: &[Clasz] = match self {
            Self::Train => &[
                Clasz::HighSpeed,
                Clasz::LongDistance,
                Clasz::Night,
                Clasz::Regional,
                Clasz::Suburban,
            ],
            Self::Metro => &[Clasz::Subway],
            Self::Tram => &[Clasz::Tram],
            Self::Bus => &[Clasz::Bus],
            Self::Coach => &[Clasz::Coach],
            Self::Boat => &[Clasz::Ship],
            Self::Funicular => &[Clasz::Funicular],
            Self::AerialTramway => &[Clasz::AerialLift],
        };
        classes
            .iter()
            .fold(0u16, |mask, class| mask | (1u16 << (*class as u8)))
    }
}

fn default_exclude() -> bool {
    true
}

/// OJP ModeAndModeOfOperationFilter-style selection. Multiple inclusive
/// entries are unioned; exclusion entries are then subtracted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModeAndModeOfOperationFilter {
    #[serde(default = "default_exclude")]
    pub exclude: bool,
    #[serde(rename = "ptModes")]
    pub pt_modes: Vec<PtMode>,
}

/// No inclusive filters means all modes are initially allowed. Absence of
/// the entire field is fully backward compatible, including Other classes.
pub fn allowed_classes(filters: &[ModeAndModeOfOperationFilter]) -> Result<u16> {
    let mut included = 0u16;
    let mut excluded = 0u16;
    let mut has_inclusion = false;
    for filter in filters {
        ensure!(
            !filter.pt_modes.is_empty(),
            "ptModes must contain at least one mode"
        );
        let selected = filter
            .pt_modes
            .iter()
            .fold(0u16, |mask, mode| mask | mode.mask());
        if filter.exclude {
            excluded |= selected;
        } else {
            has_inclusion = true;
            included |= selected;
        }
    }
    Ok((if has_inclusion { included } else { u16::MAX }) & !excluded)
}

#[inline]
pub fn allows_class(mask: u16, class: u8) -> bool {
    mask.checked_shr(u32::from(class))
        .is_some_and(|bits| bits & 1 != 0)
}

#[inline]
pub fn allows_route(tt: &ZeroCopyTimetable<'_>, route: RouteIdx, mask: u16) -> bool {
    tt.route_clasz()
        .get(route.idx())
        .is_some_and(|&class| allows_class(mask, class))
}

/// Unscheduled ADDED runs without a known static route have no GTFS route
/// type in the realtime overlay. Fail closed for a restricted query rather
/// than silently return a forbidden mode.
pub fn allows_realtime_run(
    tt: &ZeroCopyTimetable<'_>,
    snapshot: &RegionRealtimeSnapshot,
    handle: RegionRtRun,
    mask: u16,
) -> bool {
    if mask == u16::MAX {
        return true;
    }
    let Some(run) = snapshot.run(handle) else {
        return false;
    };
    let route = match run.pattern {
        RtPatternRef::Static(id) => Some(RouteIdx(id)),
        RtPatternRef::Dynamic(_) => run.original.and_then(|original| {
            tt.transport_route()
                .get(original.transport as usize)
                .copied()
        }),
    };
    route.is_some_and(|route| allows_route(tt, route, mask))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unfiltered_retains_all_classes() {
        assert_eq!(allowed_classes(&[]).unwrap(), u16::MAX);
    }
    #[test]
    fn includes_are_union_and_excludes_subtracted() {
        let filters = vec![
            ModeAndModeOfOperationFilter {
                exclude: false,
                pt_modes: vec![PtMode::Train, PtMode::Coach],
            },
            ModeAndModeOfOperationFilter {
                exclude: true,
                pt_modes: vec![PtMode::Coach],
            },
        ];
        let mask = allowed_classes(&filters).unwrap();
        assert!(allows_class(mask, Clasz::Regional as u8));
        assert!(allows_class(mask, Clasz::HighSpeed as u8));
        assert!(!allows_class(mask, Clasz::Coach as u8));
        assert!(!allows_class(mask, Clasz::Bus as u8));
    }
    #[test]
    fn exclusion_only_keeps_other_modes() {
        let mask = allowed_classes(&[ModeAndModeOfOperationFilter {
            exclude: true,
            pt_modes: vec![PtMode::Coach],
        }])
        .unwrap();
        assert!(allows_class(mask, Clasz::Other as u8));
        assert!(!allows_class(mask, Clasz::Coach as u8));
    }
    #[test]
    fn ojp_style_json_decodes() {
        let filters: Vec<ModeAndModeOfOperationFilter> =
            serde_json::from_str(r#"[{"exclude":false,"ptModes":["TRAIN","METRO"]}]"#).unwrap();
        assert!(allows_class(
            allowed_classes(&filters).unwrap(),
            Clasz::Subway as u8
        ));
        assert!(!allows_class(
            allowed_classes(&filters).unwrap(),
            Clasz::Bus as u8
        ));
    }
}
