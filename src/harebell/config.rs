use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct HarebellConfig {
    // Width of lines in meters/pixels for calculating offsets?
    // Route merging rules
    pub merge_nyct_by_color: bool,
}

impl Default for HarebellConfig {
    fn default() -> Self {
        Self {
            merge_nyct_by_color: true,
        }
    }
}
