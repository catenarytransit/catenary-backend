use crate::graph::RenderGraph;
use crate::tile_gen::TileGenerator;
use anyhow::Result;
use log::info;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

pub struct Generator {
    output_dir: String,
}

impl Generator {
    pub fn new(output_dir: String) -> Self {
        Self { output_dir }
    }

    /// Generate all tiles for a zoom range and write them to disk.
    /// Uses parallel tile generation for improved performance.
    pub fn generate_all(&self, graph: &RenderGraph, min_z: u8, max_z: u8) -> Result<()> {
        info!("Starting generation for levels {}-{}...", min_z, max_z);
        fs::create_dir_all(&self.output_dir)?;

        // Use the new parallel API with progress tracking
        let progress = Arc::new(AtomicUsize::new(0));

        // Start progress monitoring thread
        let progress_clone = progress.clone();
        let min_z_copy = min_z;
        let max_z_copy = max_z;
        let monitor_handle = thread::spawn(move || {
            let mut last_count = 0;
            loop {
                thread::sleep(Duration::from_secs(5));
                let current = progress_clone.load(Ordering::Relaxed);
                if current > last_count {
                    info!(
                        "Progress: {} tiles generated (zoom {}-{})",
                        current, min_z_copy, max_z_copy
                    );
                    last_count = current;
                }
                // Check if we should exit (no progress for a while usually means done)
                // We'll break when the main thread joins us
                if current == last_count && last_count > 0 {
                    // Give it one more chance
                    thread::sleep(Duration::from_secs(2));
                    if progress_clone.load(Ordering::Relaxed) == current {
                        break;
                    }
                }
            }
        });

        // Generate all tiles for all zoom levels in parallel
        let (total, tiles) = TileGenerator::generate_tiles_for_zoom_range_with_progress(
            graph, min_z, max_z, &progress,
        );

        info!("Generated {} tiles, now writing to disk...", total);

        // Write tiles to disk (this is I/O bound, so we do it after generation)
        for tile_result in tiles {
            if let Err(e) = self.write_tile(
                tile_result.z,
                tile_result.x,
                tile_result.y,
                tile_result.tile,
            ) {
                eprintln!(
                    "Failed to write tile {}/{}/{}: {}",
                    tile_result.z, tile_result.x, tile_result.y, e
                );
            }
        }

        // Signal monitor thread to stop and wait for it
        drop(monitor_handle);

        info!("Tile generation complete!");
        Ok(())
    }

    /// Generate tiles for a single zoom level.
    /// Uses parallel tile generation for improved performance.
    pub fn generate_zoom_level(&self, graph: &RenderGraph, z: u8) -> Result<()> {
        info!("Generating tiles for zoom level {}...", z);

        let tiles = TileGenerator::generate_tiles_for_zoom_level(graph, z);

        println!("Generated {} tiles for zoom level {}", tiles.len(), z);

        for tile_result in tiles {
            if let Err(e) = self.write_tile(
                tile_result.z,
                tile_result.x,
                tile_result.y,
                tile_result.tile,
            ) {
                eprintln!(
                    "Failed to write tile {}/{}/{}: {}",
                    tile_result.z, tile_result.x, tile_result.y, e
                );
            }
        }

        Ok(())
    }

    fn write_tile(&self, z: u8, x: u32, y: u32, tile: mvt::Tile) -> Result<()> {
        let dir = format!("{}/{}/{}", self.output_dir, z, x);
        fs::create_dir_all(&dir)?;
        let path = format!("{}/{}.pbf", dir, y);
        let bytes = tile.to_bytes()?;
        if !bytes.is_empty() {
            fs::write(path, bytes)?;
        }
        Ok(())
    }
}
