use crate::routing_common::timetable::{FormatError, ZeroCopyTimetable};
use anyhow::{Context, Result};
use memmap2::{Mmap, MmapOptions};
use std::{fs::File, path::Path};

pub struct MappedTimetable {
    bytes: Mmap,
}

impl MappedTimetable {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("opening timetable {}", path.display()))?;
        let bytes = unsafe { MmapOptions::new().map(&file) }
            .with_context(|| format!("mapping timetable {}", path.display()))?;
        ZeroCopyTimetable::new(&bytes)
            .with_context(|| format!("validating timetable {}", path.display()))?;
        Ok(Self { bytes })
    }

    pub fn view(&self) -> std::result::Result<ZeroCopyTimetable<'_>, FormatError> {
        ZeroCopyTimetable::new(&self.bytes)
    }
}

/// Publish a timetable only after its streamed representation has been
/// flushed, synced and validated through the same mmap reader as Edelweiss.
/// A unique sibling avoids collisions between concurrent Gentian compilers.
pub fn publish(tt: &crate::routing_common::timetable::TimetableOwned, output: &Path) -> Result<u64> {
    use std::fs::{self, OpenOptions};
    use std::io::BufWriter;

    let dir = output.parent().filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let name = output.file_name().context("timetable output has no file name")?;
    let name = name.to_string_lossy();

    let mut last_error = None;
    for _ in 0..8 {
        let temporary = dir.join(format!(".{name}.{}.building", uuid::Uuid::new_v4()));
        let file = match OpenOptions::new().write(true).create_new(true).open(&temporary) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                last_error = Some(error);
                continue;
            }
            Err(error) => return Err(error).with_context(|| format!("creating {}", temporary.display())),
        };

        // Cleanup on every early return, including validation and rename failure.
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
        }
        let cleanup = Cleanup(temporary.clone());

        let mut writer = BufWriter::new(file);
        let bytes = tt.write_to(&mut writer)
            .with_context(|| format!("serializing {}", temporary.display()))?;
        let file = writer.into_inner()
            .map_err(|e| anyhow::anyhow!("flushing {}: {}", temporary.display(), e))?;
        file.sync_all().with_context(|| format!("syncing {}", temporary.display()))?;
        drop(file);

        let mapped = MappedTimetable::open(&temporary)?;
        drop(mapped);
        fs::rename(&temporary, output)
            .with_context(|| format!("publishing {}", output.display()))?;
        #[cfg(unix)]
        File::open(dir)?.sync_all().with_context(|| format!("syncing {}", dir.display()))?;
        drop(cleanup);
        return Ok(bytes);
    }
    Err(last_error.context("could not allocate a unique temporary timetable path")?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publishing_keeps_existing_file_on_serialization_failure() {
        let directory = std::env::temp_dir().join(format!(
            "gentian-publish-test-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("routing.bin");
        let old = crate::routing_common::timetable::TimetableOwned::empty(20_000, 10);
        let old_size = publish(&old, &path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), old_size);
        let old_bytes = std::fs::read(&path).unwrap();

        let invalid = crate::routing_common::timetable::TimetableOwned::empty(20_000, 513);
        assert!(publish(&invalid, &path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), old_bytes);

        let next = crate::routing_common::timetable::TimetableOwned::empty(20_010, 35);
        publish(&next, &path).unwrap();
        assert_eq!(MappedTimetable::open(&path).unwrap().view().unwrap().header().day_count, 35);
        let leftovers: Vec<_> = std::fs::read_dir(&directory).unwrap().collect();
        assert_eq!(leftovers.len(), 1, "scratch files must be removed");
        std::fs::remove_dir_all(directory).unwrap();
    }

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
