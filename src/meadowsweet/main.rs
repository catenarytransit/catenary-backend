//! Meadowsweet: independent, bounded-memory OSM station railway pipeline.
//!
//! This first-stage executable accepts verified, map-matched physical-edge
//! traversals. The OSM importer and map matcher are separate future stages.
//! There is intentionally no dependency on Globeflower or local LOOM.
mod external_sort;
mod segments;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use external_sort::sort_fixed;
use segments::{SegmentWriter, read_segment};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Meadowsweet disk-backed railway station segment builder")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Split externally map-matched traversals at OSM station anchors.
    ///
    /// Input format v1 (BE): repeating records of start_station:i64,
    /// edge_count:u64, then edge_count records of edge_id:u64, forward:u8,
    /// arrival_station:i64 (i64::MIN for none).
    /// Stations must already be anchored to the physical railway graph.
    Segment {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output_dir: PathBuf,
    },
    /// Sort fixed 32-byte station-segment index records by their first field.
    /// Intermediate files remain on disk, not in process memory.
    SortSegmentIndex {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        scratch_dir: PathBuf,
        #[arg(long, default_value_t = 64)]
        memory_mib: usize,
    },
    /// Verify structural integrity of a Meadowsweet station segment index.
    Inspect {
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        path_file: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Segment { input, output_dir } => {
            fs::create_dir_all(&output_dir)?;
            let path_file = output_dir.join("paths.bin");
            let index_file = output_dir.join("segments.bin");
            let source = File::open(&input)?;
            let mut reader = BufReader::new(source);
            let mut writer = SegmentWriter::create(&path_file, &index_file)?;
            let mut traversals = 0u64;
            let mut segments = 0u64;
            loop {
                let mut first = [0u8; 1];
                if reader.read(&mut first)? == 0 {
                    break;
                }
                let mut header = [0u8; 16];
                header[0] = first[0];
                reader.read_exact(&mut header[1..]).context("truncated traversal header")?;
                let station = i64::from_be_bytes(header[0..8].try_into()?);
                let count = u64::from_be_bytes(header[8..16].try_into()?);
                if station == i64::MIN {
                    bail!("reserved station sentinel cannot be used as an origin");
                }
                segments += writer.append_records(station, &mut reader, count)?;
                traversals += 1;
            }
            writer.finish()?;
            println!("processed {traversals} traversals and {segments} station segments");
        }
        Command::SortSegmentIndex { input, output, scratch_dir, memory_mib } => {
            let memory = memory_mib.checked_mul(1024 * 1024)
                .context("memory limit overflow")?;
            let records = sort_fixed::<32>(&input, &output, &scratch_dir, memory, 16)?;
            println!("sorted {records} station segment index records");
        }
        Command::Inspect { index, path_file } => {
            let path_len = fs::metadata(path_file)?.len();
            let mut reader = BufReader::new(File::open(index)?);
            let mut segments = 0u64;
            while let Some(segment) = read_segment(&mut reader)? {
                let end = segment.path_offset
                    .checked_add(segment.edge_count.checked_mul(9).context("edge count overflow")?)
                    .context("path offset overflow")?;
                if end > path_len {
                    bail!("segment {} references beyond path store", segments);
                }
                if segment.edge_count == 0 {
                    bail!("zero-length station segment");
                }
                segments += 1;
            }
            println!("validated {segments} station segment records, {path_len} path bytes");
        }
    }
    Ok(())
}
