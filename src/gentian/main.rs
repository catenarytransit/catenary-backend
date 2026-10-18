//! Gentian V2 schema writer. GTFS-to-Nigiri graph construction is deferred.
//! This binary only writes an empty, valid V2 container with optional source
//! registrations; it does not build usable transit routes or stop indexes.
use std::path::PathBuf;
use clap::Parser;
use catenary::routing_common::timetable::{TimetableOwned, MAX_DAYS};

#[derive(Parser, Debug)]
#[command(author, version, about = "Write a Nigiri-shaped Catenary V2 timetable schema (no graph generation)")]
struct Args {
    #[arg(short, long)]
    output: PathBuf,
    /// First UTC traffic day in Unix days, including Nigiri's lookback offset.
    #[arg(long, default_value_t = 0)]
    first_utc_day: i32,
    #[arg(long, default_value_t = MAX_DAYS as u32)]
    day_count: u32,
    /// Register sources as CHATEAU,ONESTOP_FEED_ID,ATTEMPT_ID; repeatable.
    /// Source indices are assigned in the order given.
    #[arg(long = "source", value_name = "CHATEAU,FEED,ATTEMPT")]
    sources: Vec<String>,
}
fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut tt = TimetableOwned::empty(args.first_utc_day, args.day_count);
    for value in args.sources {
        let components: Vec<_> = value.splitn(3, ',').collect();
        if components.len() != 3 || components.iter().any(|x| x.is_empty()) {
            anyhow::bail!("invalid --source: expected CHATEAU,FEED,ATTEMPT");
        }
        tt.add_source(components[0], components[1], components[2])?;
    }
    let bytes = tt.encode()?;
    std::fs::write(&args.output, &bytes)?;
    println!(
        "Wrote V2 schema-only timetable: {} bytes, {} sources; no compiled routes or transports",
        bytes.len(), tt.sources.len()
    );
    Ok(())
}
