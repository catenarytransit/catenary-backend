use anyhow::{Result, bail};
use std::collections::BTreeSet;

fn add_range(selected: &mut BTreeSet<i16>, start: i16, end: i16) {
    selected.extend(start..=end);
}

/// The default exactly preserves the historic streamed Globeflower run.
/// Named modes expand to standard and extended GTFS route_type values;
/// numeric codes select precisely one type (without implicit expansion).
pub fn parse_mots(raw: Option<&str>) -> Result<Vec<i16>> {
    let Some(raw) = raw else {
        return Ok(vec![0, 1]);
    };
    let mut selected = BTreeSet::new();
    for value in raw.split(',') {
        let name = value.trim().to_ascii_lowercase();
        if name.is_empty() {
            bail!("--mots requires nonempty comma-separated MOT names or GTFS route_type codes");
        }
        match name.as_str() {
            "tram" | "streetcar" | "light_rail" | "light-rail" => {
                selected.insert(0);
                add_range(&mut selected, 900, 906);
            }
            "metro" | "subway" => {
                selected.insert(1);
                add_range(&mut selected, 400, 405);
            }
            "rail" | "train" | "heavy_rail" | "heavy-rail" => {
                selected.insert(2);
                add_range(&mut selected, 100, 117);
            }
            "bus" => {
                selected.insert(3);
                add_range(&mut selected, 700, 716);
            }
            "ferry" => {
                selected.insert(4);
                selected.insert(1200);
            }
            "cable_tram" | "cable-tram" | "cablecar" | "cable_car" => {
                selected.insert(5);
            }
            "gondola" => {
                selected.insert(6);
                selected.insert(1300);
            }
            "funicular" => {
                selected.insert(7);
                selected.insert(1400);
            }
            "trolleybus" => {
                selected.insert(800);
            }
            "monorail" => {
                selected.insert(12);
                selected.insert(405);
            }
            "all" => {
                for (from, to) in [
                    (0, 7),
                    (100, 117),
                    (200, 209),
                    (400, 405),
                    (700, 716),
                    (800, 800),
                    (900, 906),
                    (1000, 1021),
                    (1100, 1100),
                    (1200, 1200),
                    (1300, 1300),
                    (1400, 1400),
                    (1500, 1507),
                    (1700, 1702),
                ] {
                    add_range(&mut selected, from, to);
                }
                selected.insert(12);
            }
            _ => {
                let type_id = name.parse::<i16>().map_err(|_| anyhow::anyhow!(
                    "unknown MOT {value:?}; use tram, metro, rail, bus, ferry, funicular, gondola, monorail, all, or a GTFS route_type number"
                ))?;
                if type_id < 0 {
                    bail!("GTFS route_type must be nonnegative: {value}");
                }
                selected.insert(type_id);
            }
        }
    }
    Ok(selected.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_preserved() {
        assert_eq!(parse_mots(None).unwrap(), vec![0, 1]);
    }
    #[test]
    fn names_expand_to_extended_types() {
        let types = parse_mots(Some("rail,metro,rail")).unwrap();
        for n in [1, 2, 100, 117, 400, 405] {
            assert!(types.contains(&n));
        }
        assert!(!types.contains(&0));
        assert!(!types.contains(&3));
    }
    #[test]
    fn explicit_numbers_are_exact() {
        assert_eq!(parse_mots(Some("2,100,2")).unwrap(), vec![2, 100]);
    }
    #[test]
    fn invalid_values_rejected() {
        for input in [
            "",
            "rail,",
            ",metro",
            "rail,,tram",
            "spaceship",
            "-1",
            "40000",
        ] {
            assert!(parse_mots(Some(input)).is_err(), "{input}");
        }
    }
}
