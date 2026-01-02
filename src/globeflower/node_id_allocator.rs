// ===========================================================================
// Node ID Allocator for parallel cluster processing
// ===========================================================================
 use std::sync::Arc;
use std::sync::Mutex;

/// Thread-safe allocator that dispenses chunks of node IDs on demand.
/// Each cluster processing thread can request a chunk when it starts,
/// and request more if needed during processing.
pub struct NodeIdAllocator {
    next_id: Mutex<usize>,
    chunk_size: usize,
}

impl NodeIdAllocator {
    pub fn new(start: usize, chunk_size: usize) -> Self {
        Self {
            next_id: Mutex::new(start),
            chunk_size,
        }
    }

    /// Allocate a new chunk of node IDs. Returns the starting ID of the chunk.
    pub fn allocate_chunk(&self) -> usize {
        let mut guard = self.next_id.lock().unwrap();
        let start = *guard;
        *guard += self.chunk_size;
        start
    }

    /// Get the current next_id value (for updating external counter after parallel phase)
    pub fn current(&self) -> usize {
        *self.next_id.lock().unwrap()
    }
}

/// Wrapper for a cluster that can dynamically request additional ID chunks as needed.
/// This prevents clusters from failing when they exceed their initial chunk allocation,
/// and allows efficient use of ID space by starting with smaller chunks.
pub struct ChunkedNodeIdTracker {
    allocator: Arc<NodeIdAllocator>,
    current_chunk_start: usize,
    current_chunk_end: usize,
    next_id: usize,
}

impl ChunkedNodeIdTracker {
    /// Create a new tracker with an initial chunk from the allocator
    pub fn new(allocator: Arc<NodeIdAllocator>) -> Self {
        let chunk_start = allocator.allocate_chunk();
        let chunk_end = chunk_start + allocator.chunk_size;
        Self {
            allocator,
            current_chunk_start: chunk_start,
            current_chunk_end: chunk_end,
            next_id: chunk_start,
        }
    }

    /// Get the next node ID, requesting a new chunk if needed
    pub fn next(&mut self) -> usize {
        // Check if we've exhausted the current chunk
        if self.next_id >= self.current_chunk_end {
            // Request a new chunk
            let old_chunk = self.current_chunk_start;
            self.current_chunk_start = self.allocator.allocate_chunk();
            self.current_chunk_end = self.current_chunk_start + self.allocator.chunk_size;
            self.next_id = self.current_chunk_start;

            println!(
                "  [ChunkAlloc] Exhausted chunk {}-{}, allocated new chunk {}-{}",
                old_chunk,
                old_chunk + self.allocator.chunk_size - 1,
                self.current_chunk_start,
                self.current_chunk_end - 1
            );
        }

        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Get the current next_id value (for statistics)
    pub fn current(&self) -> usize {
        self.next_id
    }

    /// Get the initial chunk start (for statistics)
    pub fn chunk_start(&self) -> usize {
        self.current_chunk_start
    }
}