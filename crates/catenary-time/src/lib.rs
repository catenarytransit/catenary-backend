//! Jiff timezone rules with a Chrono bridge for existing DB/GTFS/wire types.
//! All rules come explicitly from jiff-tzdb, never host-dependent zoneinfo.
use chrono::{Datelike, Offset, Timelike};
use jiff::{
    Timestamp,
    tz::{AmbiguousOffset, TimeZone as JiffZone},
};
use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

pub const TZDB_VERSION: Option<&str> = jiff_tzdb::VERSION;

pub fn utc_now() -> chrono::DateTime<chrono::Utc> {
    let now = Timestamp::now();
    chrono::DateTime::from_timestamp(now.as_second(), now.subsec_nanosecond() as u32)
        .expect("current Jiff timestamp is in Chrono's range")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Tz(&'static str);
pub const UTC: Tz = Tz::UTC;

impl Tz {
    pub const UTC: Self = Self("UTC");
    pub fn name(self) -> &'static str {
        self.0
    }
    pub fn from_str_insensitive(name: &str) -> Result<Self, String> {
        let (name, _) = jiff_tzdb::get(name).ok_or_else(|| format!("unknown timezone: {name}"))?;
        Ok(Self(name))
    }
    pub fn jiff(self) -> JiffZone {
        static CACHE: OnceLock<RwLock<HashMap<&'static str, JiffZone>>> = OnceLock::new();
        let cache = CACHE.get_or_init(Default::default);
        if let Some(zone) = cache.read().expect("timezone cache poisoned").get(self.0) {
            return zone.clone();
        }
        let (_, data) = jiff_tzdb::get(self.0).expect("validated timezone");
        let zone = JiffZone::tzif(self.0, data).expect("valid bundled TZif");
        cache
            .write()
            .expect("timezone cache poisoned")
            .insert(self.0, zone.clone());
        zone
    }
    fn offset(self, seconds: i32) -> TzOffset {
        TzOffset {
            zone: self,
            fixed: chrono::FixedOffset::east_opt(seconds).expect("IANA offset"),
        }
    }
}
impl std::str::FromStr for Tz {
    type Err = String;
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let (canonical, _) =
            jiff_tzdb::get(name).ok_or_else(|| format!("unknown timezone: {name}"))?;
        if canonical != name {
            return Err(format!("incorrect timezone capitalization: {name}"));
        }
        Ok(Self(canonical))
    }
}
impl std::fmt::Display for Tz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl serde::Serialize for Tz {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}
impl<'de> serde::Deserialize<'de> for Tz {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(serde::de::Error::custom)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TzOffset {
    zone: Tz,
    fixed: chrono::FixedOffset,
}
impl Offset for TzOffset {
    fn fix(&self) -> chrono::FixedOffset {
        self.fixed
    }
}
impl std::fmt::Display for TzOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fixed.fmt(f)
    }
}
impl chrono::TimeZone for Tz {
    type Offset = TzOffset;
    fn from_offset(offset: &TzOffset) -> Self {
        offset.zone
    }
    fn offset_from_local_date(&self, date: &chrono::NaiveDate) -> chrono::LocalResult<TzOffset> {
        self.offset_from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
    }
    fn offset_from_local_datetime(
        &self,
        dt: &chrono::NaiveDateTime,
    ) -> chrono::LocalResult<TzOffset> {
        let Ok(year) = i16::try_from(dt.year()) else {
            return chrono::LocalResult::None;
        };
        // Jiff uses Unix time: leap-second representations are not admitted here.
        if dt.nanosecond() >= 1_000_000_000 {
            return chrono::LocalResult::None;
        }
        let Ok(civil) = jiff::civil::DateTime::new(
            year,
            dt.month() as i8,
            dt.day() as i8,
            dt.hour() as i8,
            dt.minute() as i8,
            dt.second() as i8,
            dt.nanosecond() as i32,
        ) else {
            return chrono::LocalResult::None;
        };
        match self.jiff().to_ambiguous_timestamp(civil).offset() {
            AmbiguousOffset::Unambiguous { offset } => {
                chrono::LocalResult::Single(self.offset(offset.seconds()))
            }
            AmbiguousOffset::Gap { .. } => chrono::LocalResult::None,
            AmbiguousOffset::Fold { before, after } => chrono::LocalResult::Ambiguous(
                self.offset(before.seconds()),
                self.offset(after.seconds()),
            ),
        }
    }
    fn offset_from_utc_date(&self, date: &chrono::NaiveDate) -> TzOffset {
        self.offset_from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
    }
    fn offset_from_utc_datetime(&self, dt: &chrono::NaiveDateTime) -> TzOffset {
        let ts =
            Timestamp::from_second(dt.and_utc().timestamp()).expect("datetime outside Jiff range");
        self.offset(self.jiff().to_offset(ts).seconds())
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
pub mod America {
    use super::Tz;
    pub const Los_Angeles: Tz = Tz("America/Los_Angeles");
    pub const New_York: Tz = Tz("America/New_York");
    pub const Montreal: Tz = Tz("America/Montreal");
}
#[allow(non_snake_case, non_upper_case_globals)]
pub mod Europe {
    use super::Tz;
    pub const Berlin: Tz = Tz("Europe/Berlin");
}
#[allow(non_snake_case, non_upper_case_globals)]
pub mod US {
    use super::Tz;
    pub const Pacific: Tz = Tz("US/Pacific");
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{LocalResult, TimeZone};
    #[test]
    fn bundled_2026f_cuba_rule() {
        assert!(TZDB_VERSION.is_some_and(|version| version >= "2026f"));
        let tz: Tz = "America/Havana".parse().unwrap();
        let dt = tz.with_ymd_and_hms(2026, 11, 6, 12, 0, 0).single().unwrap();
        assert_eq!(dt.offset().fix().local_minus_utc(), -4 * 3600);
    }
    #[test]
    fn all_bundled_zones_parse() {
        for name in jiff_tzdb::available() {
            let (_, data) = jiff_tzdb::get(name).unwrap();
            assert!(
                JiffZone::tzif(name, data).is_ok(),
                "invalid TZif for {name}: {:?}",
                JiffZone::tzif(name, data)
            );
        }
    }
    #[test]
    fn gaps_and_folds_are_preserved() {
        let tz: Tz = "America/Los_Angeles".parse().unwrap();
        assert!(matches!(
            tz.with_ymd_and_hms(2026, 3, 8, 2, 30, 0),
            LocalResult::None
        ));
        let LocalResult::Ambiguous(a, b) = tz.with_ymd_and_hms(2026, 11, 1, 1, 30, 0) else {
            panic!("expected fold")
        };
        assert_eq!(b.timestamp() - a.timestamp(), 3600);
        assert_eq!(a.timezone(), tz);
    }
    #[test]
    fn gtfs_noon_minus_twelve_hours_and_overnight() {
        let tz: Tz = "America/Los_Angeles".parse().unwrap();
        let noon = tz.with_ymd_and_hms(2026, 3, 8, 12, 0, 0).single().unwrap();
        let anchor = noon - chrono::Duration::hours(12);
        assert_eq!(anchor.timestamp() + 12 * 3600, noon.timestamp());
        let overnight = anchor + chrono::Duration::hours(26);
        assert_eq!(overnight.timestamp() - anchor.timestamp(), 26 * 3600);
    }
    #[test]
    fn names_and_serialization() {
        let tz = Tz::from_str_insensitive("america/los_angeles").unwrap();
        assert_eq!(tz.name(), "America/Los_Angeles");
        assert!("america/los_angeles".parse::<Tz>().is_err());
        let json = serde_json::to_string(&tz).unwrap();
        assert_eq!(json, "\"America/Los_Angeles\"");
        assert_eq!(serde_json::from_str::<Tz>(&json).unwrap(), tz);
        assert!(Tz::from_str_insensitive("invalid/timezone").is_err());
    }
    #[test]
    fn offsets_and_utc_roundtrip() {
        for name in ["UTC", "Asia/Kathmandu", "Europe/Berlin", "Pacific/Apia"] {
            let tz: Tz = name.parse().unwrap();
            let ts = Timestamp::from_second(1_793_664_000).unwrap();
            let dt = tz.timestamp_opt(ts.as_second(), 0).single().unwrap();
            assert_eq!(
                dt.offset().fix().local_minus_utc(),
                tz.jiff().to_offset(ts).seconds()
            );
            assert_eq!(dt.with_timezone(&chrono::Utc).timestamp(), ts.as_second());
        }
    }
}
