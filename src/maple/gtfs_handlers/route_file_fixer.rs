use std::error::Error;
use std::fs;
use std::path::Path;

use csv::{ReaderBuilder, StringRecord, WriterBuilder};

pub fn fix_gtfs_route_colors<P: AsRef<Path>>(gtfs_folder: P) -> Result<(), Box<dyn Error>> {
    let routes_path = gtfs_folder.as_ref().join("routes.txt");
    let temp_path = gtfs_folder.as_ref().join("routes.tmp");

    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .from_path(&routes_path)?;

    let headers = rdr.headers()?.clone();

    let route_color_idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("route_color"));
    let route_text_color_idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("route_text_color"));

    // If neither color column exists, nothing to do.
    if route_color_idx.is_none() && route_text_color_idx.is_none() {
        return Ok(());
    }

    let mut wtr = WriterBuilder::new().flexible(true).from_path(&temp_path)?;

    // Write headers back unchanged
    wtr.write_record(&headers)?;

    for result in rdr.records() {
        let record: StringRecord = result?;

        // Convert record into a Vec<String> so we can mutate fields by index.
        let mut fields: Vec<String> = record.iter().map(|s| s.to_owned()).collect();

        if let Some(idx) = route_color_idx {
            if let Some(val) = fields.get_mut(idx) {
                if let Some(normalized) = normalize_color(val) {
                    *val = normalized;
                }
            }
        }

        if let Some(idx) = route_text_color_idx {
            if let Some(val) = fields.get_mut(idx) {
                if let Some(normalized) = normalize_color(val) {
                    *val = normalized;
                }
            }
        }

        wtr.write_record(&fields)?;
    }

    wtr.flush()?;
    fs::rename(temp_path, routes_path)?;

    Ok(())
}

/// Normalize a color string to 6-char RRGGBB (no '#').
///
/// Rules:
/// - Strip all non-hex chars (this removes '#').
/// - Uppercase A–F.
/// - If exactly 6 chars: keep as-is.
/// - If exactly 3 chars: expand like CSS "ABC" -> "AABBCC".
/// - If 1–5 chars: left-pad with '0' to reach length 6.
/// - If >6 chars: use the **last** 6 chars.
/// - If there are no hex chars at all: return None (caller keeps original).
fn normalize_color(input: &str) -> Option<String> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }

    // Keep only hex digits
    let mut hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.is_empty() {
        return None;
    }

    hex = hex.to_uppercase();
    let len = hex.len();

    match len {
        6 => Some(hex),
        3 => {
            // "ABC" -> "AABBCC"
            let mut out = String::with_capacity(6);
            for ch in hex.chars() {
                out.push(ch);
                out.push(ch);
            }
            Some(out)
        }
        n if n > 6 => {
            // Use last 6 characters
            let start = n - 6;
            Some(hex[start..].to_string())
        }
        n => {
            // 1–5 chars: left-pad with 0s
            let mut out = String::with_capacity(6);
            for _ in 0..(6 - n) {
                out.push('0');
            }
            out.push_str(&hex);
            Some(out)
        }
    }
}
