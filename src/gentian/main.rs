//! Gentian V2 schema writer and routing-region resolver.
//!
//! GTFS-to-RAPTOR graph compilation is not yet implemented. Region selection
//! registers source identities only; it does not create routes or transports.
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use catenary::routing_common::routing_regions::{RoutingRegionsConfig, resolve};
use catenary::routing_common::timetable::{MAX_DAYS, TimetableOwned};
use postgres::{Client, NoTls};

#[derive(Parser, Debug)]
#[command(author, version, about = "Resolve routing regions and write a V2 timetable schema (no route compilation)")]
struct Args {
    /// Output V2 binary. Optional when only listing selected chateaux.
    #[arg(short, long, required_unless_present = "list_chateaux")]
    output: Option<PathBuf>,

    /// First UTC traffic day in Unix days, including Nigiri's lookback offset.
    #[arg(long, default_value_t = 0)]
    first_utc_day: i32,

    #[arg(long, default_value_t = MAX_DAYS as u32)]
    day_count: u32,

    /// Register sources as CHATEAU,ONESTOP_FEED_ID,ATTEMPT_ID; repeatable.
    #[arg(long = "source", value_name = "CHATEAU,FEED,ATTEMPT", conflicts_with = "region")]
    sources: Vec<String>,

    /// Resolve this configured region through PostGIS, including whole chateaux.
    #[arg(long, value_name = "REGION_ID", conflicts_with = "sources")]
    region: Option<String>,

    /// TOML definition of routable regions.
    #[arg(long, default_value = "routing_regions/config.toml")]
    regions_config: PathBuf,

    /// PostgreSQL connection string. Defaults to DATABASE_URL.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Print resolved chateaux and active feed sources as JSON without writing a timetable.
    #[arg(long, requires = "region")]
    list_chateaux: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let selection = if let Some(id) = &args.region {
        let config = RoutingRegionsConfig::load(&args.regions_config)?;
        let url = args.database_url.as_deref()
            .context("--database-url or DATABASE_URL is required with --region")?;
        let mut client = Client::connect(url, NoTls)
            .context("connecting to Postgres to resolve routing region")?;
        Some(resolve(&config, &args.regions_config, id, &mut client)?)
    } else {
        None
    };

    if args.list_chateaux {
        let selection = selection.as_ref().context("--region is required")?;
        println!("{}", serde_json::to_string_pretty(selection)?);
        return Ok(());
    }

    let output = args.output.as_ref().context("--output is required")?;
    let mut tt = TimetableOwned::empty(args.first_utc_day, args.day_count);
    if let Some(selection) = &selection {
        ensure!(!selection.chateaux.is_empty(),
            "routing region {} intersects no chateaux", selection.id);
        ensure!(!selection.sources.is_empty(),
            "routing region {} has no completed production GTFS imports", selection.id);
        for source in &selection.sources {
            tt.add_source(&source.chateau, &source.onestop_feed_id, &source.attempt_id)?;
        }
    } else {
        for value in &args.sources {
            let components: Vec<_> = value.splitn(3, ',').collect();
            ensure!(components.len() == 3 && components.iter().all(|x| !x.is_empty()),
                "invalid --source: expected CHATEAU,FEED,ATTEMPT");
            tt.add_source(components[0], components[1], components[2])?;
        }
    }

    let bytes = tt.encode()?;
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    std::fs::write(output, &bytes)
        .with_context(|| format!("writing {}", output.display()))?;
    println!(
        "Wrote V2 schema-only timetable: {} bytes, {} sources; no compiled routes or transports",
        bytes.len(), tt.sources.len()
    );
    Ok(())
}
