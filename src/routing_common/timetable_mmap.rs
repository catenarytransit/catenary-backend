use crate::routing_common::timetable::{FormatError, ZeroCopyTimetable};
use memmap2::{Mmap, MmapOptions};
use std::{fs::File, path::Path};

pub struct MappedTimetable {
    bytes: Mmap,
}

impl MappedTimetable {
    pub fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let file = File::open(path)?;
        let bytes = unsafe { MmapOptions::new().map(&file)? };
        ZeroCopyTimetable::new(&bytes)?;
        Ok(Self { bytes })
    }

    pub fn view(&self) -> Result<ZeroCopyTimetable<'_>, FormatError> {
        ZeroCopyTimetable::new(&self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opens_a_mapped_timetable() {
        let path = std::env::temp_dir().join(format!("gentian-mmap-{}", std::process::id()));
        let tt = crate::routing_common::timetable::TimetableOwned::empty(20_000, 10);
        std::fs::write(&path, tt.encode().unwrap()).unwrap();
        let mapped = MappedTimetable::open(&path).unwrap();
        assert_eq!(mapped.view().unwrap().header().day_count, 10);
        drop(mapped);
        std::fs::remove_file(path).unwrap();
    }
}
