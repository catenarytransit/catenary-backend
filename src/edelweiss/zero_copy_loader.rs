use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use catenary::routing_common::timetable::ZeroCopyTimetable;

/// A hot-swappable, checked Catenary V2 timetable mmap.
pub struct HotSwappableTimetable {
    current_mmap: RwLock<Option<Arc<Mmap>>>,
}
impl HotSwappableTimetable {
    pub fn new() -> Self { Self { current_mmap: RwLock::new(None) } }
    pub async fn load_raw_binary<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let f = File::open(path)?;
        // SAFETY: mapping a read-only binary snapshot; callers must replace
        // files atomically, never mutate a mapped inode in place.
        let map = unsafe { MmapOptions::new().map(&f)? };
        ZeroCopyTimetable::new(&map)?;
        *self.current_mmap.write().await = Some(Arc::new(map));
        Ok(())
    }
    pub async fn with_timetable<F,R>(&self, callback: F) -> Option<R>
    where F: FnOnce(ZeroCopyTimetable<'_>) -> R {
        let map = self.current_mmap.read().await.clone()?;
        let tt = ZeroCopyTimetable::new(&map).ok()?;
        Some(callback(tt))
    }
}
