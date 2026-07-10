use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct SncfStopTrack {
    pub arrival_platform: Option<String>,
    pub departure_platform: Option<String>,
    pub aimed_arrival_time: Option<i64>,
    pub expected_arrival_time: Option<i64>,
    pub aimed_departure_time: Option<i64>,
    pub expected_departure_time: Option<i64>,
}

fn extract_time(call_xml: &str, tag: &str) -> Option<i64> {
    if let Some(start) = call_xml.find(tag) {
        let sub = &call_xml[start + tag.len()..];
        let end_tag = format!("</{}>", &tag[1..]);
        if let Some(end) = sub.find(&end_tag) {
            let time_str = sub[..end].trim();
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(time_str) {
                return Some(dt.timestamp());
            }
        }
    }
    None
}

#[derive(Clone, Debug)]
pub struct SncfTrackData {
    pub track_lookup: HashMap<String, HashMap<String, SncfStopTrack>>,
}

pub fn normalize_train_num(s: &str) -> String {
    s.trim().trim_start_matches('0').to_string()
}

pub fn extract_station_code(stop_ref: &str) -> Option<String> {
    let mut code = String::new();
    for c in stop_ref.chars().rev() {
        if c.is_ascii_digit() {
            code.push(c);
        } else if !code.is_empty() {
            break;
        }
    }
    if code.len() >= 7 && code.len() <= 10 {
        Some(code.chars().rev().collect())
    } else {
        None
    }
}

pub fn parse_sncf_siri(xml: &str) -> SncfTrackData {
    let mut track_lookup: HashMap<String, HashMap<String, SncfStopTrack>> = HashMap::new();

    let mut start_idx = 0;
    while let Some(journey_start) = xml[start_idx..].find("<EstimatedVehicleJourney>") {
        let absolute_journey_start = start_idx + journey_start;
        let remaining = &xml[absolute_journey_start..];

        let journey_end = match remaining.find("</EstimatedVehicleJourney>") {
            Some(end) => end,
            None => break,
        };

        let journey_xml = &remaining[..journey_end];
        start_idx = absolute_journey_start + journey_end + "</EstimatedVehicleJourney>".len();

        let mut train_numbers = Vec::new();
        let mut t_search_idx = 0;
        while let Some(t_start) = journey_xml[t_search_idx..].find("<TrainNumberRef>") {
            let abs_t_start = t_search_idx + t_start + "<TrainNumberRef>".len();
            if let Some(t_end) = journey_xml[abs_t_start..].find("</TrainNumberRef>") {
                train_numbers.push(journey_xml[abs_t_start..abs_t_start + t_end].trim().to_string());
                t_search_idx = abs_t_start + t_end + "</TrainNumberRef>".len();
            } else {
                break;
            }
        }

        if !train_numbers.is_empty() {
            let mut stop_map = HashMap::new();

            let mut call_start_idx = 0;
            while call_start_idx < journey_xml.len() {
                let next_recorded = journey_xml[call_start_idx..].find("<RecordedCall>");
                let next_estimated = journey_xml[call_start_idx..].find("<EstimatedCall>");

                let (tag_type, offset) = match (next_recorded, next_estimated) {
                    (Some(r), Some(e)) => {
                        if r < e {
                            (1, r)
                        } else {
                            (2, e)
                        }
                    }
                    (Some(r), None) => (1, r),
                    (None, Some(e)) => (2, e),
                    (None, None) => break,
                };

                let abs_call_start = call_start_idx + offset;
                let end_tag = if tag_type == 1 {
                    "</RecordedCall>"
                } else {
                    "</EstimatedCall>"
                };
                let start_tag_len = if tag_type == 1 {
                    "<RecordedCall>".len()
                } else {
                    "</EstimatedCall>".len()
                };

                let call_sub = &journey_xml[abs_call_start..];
                let call_end = match call_sub.find(end_tag) {
                    Some(end) => end,
                    None => {
                        call_start_idx = abs_call_start + start_tag_len;
                        continue;
                    }
                };

                let call_xml = &call_sub[..call_end];
                call_start_idx = abs_call_start + call_end + end_tag.len();

                let stop_point_ref = if let Some(sp_start) = call_xml.find("<StopPointRef>") {
                    let sp_sub = &call_xml[sp_start + "<StopPointRef>".len()..];
                    if let Some(sp_end) = sp_sub.find("</StopPointRef>") {
                        Some(sp_sub[..sp_end].trim().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(stop_ref) = stop_point_ref {
                    if let Some(code) = extract_station_code(&stop_ref) {
                        let aimed_arrival_time = extract_time(call_xml, "<AimedArrivalTime>");
                        let expected_arrival_time = extract_time(call_xml, "<ExpectedArrivalTime>");
                        let aimed_departure_time = extract_time(call_xml, "<AimedDepartureTime>");
                        let expected_departure_time = extract_time(call_xml, "<ExpectedDepartureTime>");

                        let arrival_platform =
                            if let Some(ap_start) = call_xml.find("<ArrivalPlatformName>") {
                                let ap_sub = &call_xml[ap_start + "<ArrivalPlatformName>".len()..];
                                if let Some(ap_end) = ap_sub.find("</ArrivalPlatformName>") {
                                    Some(ap_sub[..ap_end].trim().to_string())
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                        let departure_platform = if let Some(dp_start) =
                            call_xml.find("<DeparturePlatformName>")
                        {
                            let dp_sub = &call_xml[dp_start + "<DeparturePlatformName>".len()..];
                            if let Some(dp_end) = dp_sub.find("</DeparturePlatformName>") {
                                Some(dp_sub[..dp_end].trim().to_string())
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if arrival_platform.is_some()
                            || departure_platform.is_some()
                            || aimed_arrival_time.is_some()
                            || expected_arrival_time.is_some()
                            || aimed_departure_time.is_some()
                            || expected_departure_time.is_some()
                        {
                            stop_map.insert(
                                code,
                                SncfStopTrack {
                                    arrival_platform,
                                    departure_platform,
                                    aimed_arrival_time,
                                    expected_arrival_time,
                                    aimed_departure_time,
                                    expected_departure_time,
                                },
                            );
                        }
                    }
                }
            }

            if !stop_map.is_empty() {
                for train_num in train_numbers {
                    let normalized_train = normalize_train_num(&train_num);
                    track_lookup
                        .entry(normalized_train)
                        .or_insert_with(HashMap::new)
                        .extend(stop_map.clone());
                }
            }
        }
    }

    SncfTrackData { track_lookup }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_station_code() {
        assert_eq!(
            extract_station_code("FR:ScheduledStopPoint::87276246"),
            Some("87276246".to_string())
        );
        assert_eq!(
            extract_station_code("StopPoint:OCECar TER-87586370"),
            Some("87586370".to_string())
        );
        assert_eq!(
            extract_station_code("StopArea:OCE87193508"),
            Some("87193508".to_string())
        );
        assert_eq!(extract_station_code("Short"), None);
    }

    #[test]
    fn test_parse_sncf_siri() {
        let xml = r#"
            <Siri>
              <EstimatedVehicleJourney>
                <TrainNumbers>
                  <TrainNumberRef>00833105</TrainNumberRef>
                </TrainNumbers>
                <RecordedCalls>
                  <RecordedCall>
                    <StopPointRef>FR:ScheduledStopPoint::87276246</StopPointRef>
                    <ArrivalPlatformName>1A</ArrivalPlatformName>
                    <DeparturePlatformName>1B</DeparturePlatformName>
                  </RecordedCall>
                </RecordedCalls>
                <EstimatedCalls>
                  <EstimatedCall>
                    <StopPointRef>FR:ScheduledStopPoint::87758318</StopPointRef>
                    <DeparturePlatformName>2</DeparturePlatformName>
                  </EstimatedCall>
                </EstimatedCalls>
              </EstimatedVehicleJourney>
            </Siri>
        "#;
        let data = parse_sncf_siri(xml);
        let stops = data.track_lookup.get("833105").unwrap();

        let stop1 = stops.get("87276246").unwrap();
        assert_eq!(stop1.arrival_platform.as_deref(), Some("1A"));
        assert_eq!(stop1.departure_platform.as_deref(), Some("1B"));

        let stop2 = stops.get("87758318").unwrap();
        assert_eq!(stop2.arrival_platform, None);
        assert_eq!(stop2.departure_platform.as_deref(), Some("2"));
    }
}
