use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use catenary::routing_common::timetable::ZeroCopyTimetable;

/// Thread-safe hot-swappable container for the timetable
pub struct HotSwappableTimetable {
    /// Arc wrapping the memory map and lifetime-bounded parsed layout.
    /// Since `ZeroCopyTimetable` contains `&'a [u8]`, we use an unsafe
    /// `Arc<Mmap>` pattern natively, or we just map the raw bytes and
    /// provide an accessor.
    current_mmap: RwLock<Option<Arc<Mmap>>>,
}

impl HotSwappableTimetable {
    pub fn new() -> Self {
        Self {
            current_mmap: RwLock::new(None),
        }
    }

    /// Decompresses (if needed) and Memory-Maps the `.bin` timetable.
    /// For true zero copy, the file on disk should be raw binary (uncompressed).
    /// If it was compressed, it should be extracted to a cache directory first.
    pub async fn load_raw_binary<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let file = File::open(path)?;

        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // Validate that it fits the header expectations
        let _test_cast = ZeroCopyTimetable::new(&mmap)
            .ok_or_else(|| anyhow::anyhow!("Invalid timetable format or truncated file!"))?;

        // Swap it in atomically
        let mut write_guard = self.current_mmap.write().await;
        *write_guard = Some(Arc::new(mmap));

        Ok(())
    }

    /// Perform a safe read operation utilizing the zero-copy bindings
    pub async fn with_timetable<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(ZeroCopyTimetable<'_>) -> R,
    {
        let read_guard = self.current_mmap.read().await;
        if let Some(mmap_arc) = read_guard.as_ref() {
            // We know the mmap is valid and structurally sound via earlier checks
            let zc = ZeroCopyTimetable::new(mmap_arc).unwrap();
            Some(f(zc))
        } else {
            None
        }
    }
}
