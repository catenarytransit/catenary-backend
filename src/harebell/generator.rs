use crate::graph::RenderGraph;
use crate::tile_gen::TileGenerator;
use anyhow::Result;
use log::info;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

pub struct Generator {
    output_dir: String,
}

impl Generator {
    pub fn new(output_dir: String) -> Self {
        Self { output_dir }
    }

    pub fn generate_all(&self, graph: &RenderGraph, min_z: u8, max_z: u8) -> Result<()> {
        info!("Starting generation for levels {}-{}...", min_z, max_z);
        fs::create_dir_all(&self.output_dir)?;

        for z in min_z..=max_z {
            self.generate_zoom_level(graph, z)?;
        }
        Ok(())
    }

    fn generate_zoom_level(&self, graph: &RenderGraph, z: u8) -> Result<()> {
        info!("Analyzing tiles for zoom level {}...", z);

        // identify necessary tiles
        // We'll query valid tiles by checking the graph elements
        // This can be optimised by iterating the RTree

        // Since we don't have direct access to RTree elements iterator easily (private field?),
        // `graph.tree` is pub.
        // We can iterate over all envelopes in the tree.
        let mut tiles: HashSet<(u32, u32)> = HashSet::new();

        for item in graph.tree.iter() {
            let bounds = item.geom(); // AABB
            // Convert bounds to tile ranges
            let min_pt = bounds.lower();
            let max_pt = bounds.upper();

            let (min_tx, min_ty) = Self::latlon_to_tile(min_pt[1], min_pt[0], z);
            let (max_tx, max_ty) = Self::latlon_to_tile(max_pt[1], max_pt[0], z);

            for x in min_tx..=max_tx {
                for y in min_ty..=max_ty {
                    tiles.insert((x, y));
                }
            }
        }

        info!("Generating {} tiles for zoom level {}", tiles.len(), z);

        let tile_vec: Vec<(u32, u32)> = tiles.into_iter().collect();

        tile_vec.par_iter().for_each(|(x, y)| {
            let tile_data = TileGenerator::generate_tile(graph, z, *x, *y);
            // Write to disk
            if let Err(e) = self.write_tile(z, *x, *y, tile_data) {
                eprintln!("Failed to write tile {}/{}/{}: {}", z, x, y, e);
            }
        });

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

    fn latlon_to_tile(lat: f64, lon: f64, z: u8) -> (u32, u32) {
        let n = 2.0f64.powi(z as i32);
        let x = ((lon + 180.0) / 360.0 * n).floor() as u32;
        let lat_rad = lat.to_radians();
        let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0
            * n)
            .floor() as u32;
        (x, y)
    }
}
