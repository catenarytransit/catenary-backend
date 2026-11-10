//! Stateless page descriptors; never contain RAPTOR buffers or server handles.
use anyhow::{Result, ensure};
use catenary::routing_common::raptor_query::Journey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct PageOptions {
    #[serde(default, alias = "requestId")]
    pub request_id: Option<String>,
    #[serde(default, alias = "numberOfResultsBefore")]
    pub number_of_results_before: Option<usize>,
    #[serde(default, alias = "numberOfResultsAfter")]
    pub number_of_results_after: Option<usize>,
    #[serde(default, alias = "pageCursor")]
    pub page_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Before,
    After,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    query_key: String,
    side: Side,
    time: i64,
}

pub fn digest(value: &impl Serialize) -> Result<String> {
    Ok(Sha256::digest(serde_json::to_vec(value)?)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

impl PageOptions {
    pub fn validate(&self) -> Result<()> {
        if let Some(id) = &self.request_id {
            ensure!(
                !id.is_empty() && id.len() <= 128,
                "request_id must contain 1..128 bytes"
            );
        }
        if self.number_of_results_before.is_some() || self.number_of_results_after.is_some() {
            let before = self.number_of_results_before.unwrap_or(0);
            let after = self.number_of_results_after.unwrap_or(0);
            ensure!(
                before <= 16 && after <= 16 && before + after > 0,
                "before/after counts must be 0..16, with at least one requested result"
            );
        }
        Ok(())
    }

    pub fn cursor(&self, query_key: &str) -> Result<Option<(Side, i64)>> {
        self.page_cursor
            .as_ref()
            .map(|text| {
                ensure!(text.len() <= 1024, "page_cursor is too large");
                let cursor: Cursor = serde_json::from_str(text)?;
                ensure!(
                    cursor.version == 1 && cursor.query_key == query_key,
                    "page_cursor belongs to a different query or version"
                );
                ensure!(cursor.time > 0, "page_cursor time must be positive");
                Ok((cursor.side, cursor.time))
            })
            .transpose()
    }
}

pub fn journey_id(journey: &Journey) -> Result<String> {
    digest(journey)
}

/// Preserve the entire second at the page edge, including tied Pareto
/// alternatives. Otherwise advancing by one second could silently skip ties.
pub fn select_page(journeys: &[Journey], side: Side, anchor: i64, count: usize) -> Vec<Journey> {
    let direct = journeys
        .iter()
        .filter(|j| j.boardings == 0)
        .cloned()
        .collect::<Vec<_>>();
    let mut page = journeys
        .iter()
        .filter(|j| j.boardings != 0)
        .filter(|j| match side {
            Side::Before => j.arrival_unix_seconds <= anchor,
            Side::After => j.departure_unix_seconds >= anchor,
        })
        .cloned()
        .collect::<Vec<_>>();
    page.sort_by(|a, b| match side {
        Side::Before => b
            .arrival_unix_seconds
            .cmp(&a.arrival_unix_seconds)
            .then(a.boardings.cmp(&b.boardings)),
        Side::After => a
            .departure_unix_seconds
            .cmp(&b.departure_unix_seconds)
            .then(a.boardings.cmp(&b.boardings)),
    });
    if count == 0 {
        return Vec::new();
    }
    if page.len() > count {
        let key = |j: &Journey| match side {
            Side::Before => j.arrival_unix_seconds,
            Side::After => j.departure_unix_seconds,
        };
        let boundary = key(&page[count - 1]);
        page.retain(|j| match side {
            Side::Before => key(j) >= boundary,
            Side::After => key(j) <= boundary,
        });
    }
    page.extend(direct);
    page
}

pub fn cursors(journeys: &[Journey], query_key: &str) -> Result<(Option<String>, Option<String>)> {
    // Direct walking has no discrete service to paginate.
    let transit = || journeys.iter().filter(|j| j.boardings != 0);
    let encode = |side, time| {
        serde_json::to_string(&Cursor {
            version: 1,
            query_key: query_key.into(),
            side,
            time,
        })
    };
    let before = transit()
        .map(|j| j.arrival_unix_seconds)
        .min()
        .and_then(|t| t.checked_sub(1))
        .filter(|&t| t > 0)
        .map(|t| encode(Side::Before, t))
        .transpose()?;
    let after = transit()
        .map(|j| j.departure_unix_seconds)
        .max()
        .and_then(|t| t.checked_add(1))
        .map(|t| encode(Side::After, t))
        .transpose()?;
    Ok((before, after))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn journey(dep: i64, arr: i64, boardings: usize) -> Journey {
        Journey {
            departure_unix_seconds: dep,
            arrival_unix_seconds: arr,
            boardings,
            legs: vec![],
        }
    }
    #[test]
    fn ties_survive_and_cursors_exclude_the_complete_boundary_second() {
        let journeys = vec![
            journey(100, 200, 1),
            journey(100, 210, 2),
            journey(300, 400, 1),
        ];
        let page = select_page(&journeys, Side::After, 100, 1);
        assert_eq!(page.len(), 2);
        let (before, after) = cursors(&page, "key").unwrap();
        let opts = PageOptions {
            page_cursor: after,
            ..Default::default()
        };
        assert_eq!(opts.cursor("key").unwrap(), Some((Side::After, 101)));
        assert!(opts.cursor("other").is_err());
        let opts = PageOptions {
            page_cursor: before,
            ..Default::default()
        };
        assert_eq!(opts.cursor("key").unwrap(), Some((Side::Before, 199)));
        assert_eq!(select_page(&journeys, Side::Before, 199, 2).len(), 0);
    }
    #[test]
    fn direct_walk_does_not_consume_a_transit_page_slot() {
        let journeys = vec![
            journey(100, 110, 0),
            journey(100, 200, 1),
            journey(300, 400, 1),
        ];
        let page = select_page(&journeys, Side::After, 100, 1);
        assert_eq!(page.len(), 2);
        assert_eq!(page.iter().filter(|j| j.boardings > 0).count(), 1);
        let (_, after) = cursors(&page, "key").unwrap();
        assert_eq!(
            PageOptions {
                page_cursor: after,
                ..Default::default()
            }
            .cursor("key")
            .unwrap(),
            Some((Side::After, 101))
        );
    }
    #[test]
    fn before_pages_select_latest_arrivals_and_validate_counts() {
        let journeys = vec![journey(10, 30, 1), journey(20, 40, 1), journey(21, 40, 2)];
        assert_eq!(select_page(&journeys, Side::Before, 40, 1).len(), 2);
        for (b, a, valid) in [(0, 0, false), (17, 1, false), (16, 16, true), (1, 0, true)] {
            assert_eq!(
                PageOptions {
                    number_of_results_before: Some(b),
                    number_of_results_after: Some(a),
                    ..Default::default()
                }
                .validate()
                .is_ok(),
                valid
            );
        }
    }
}
