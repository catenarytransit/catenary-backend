//! Gentian's regional timetable and OSR footpath compiler.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use catenary::routing_common::routing_regions::{RoutingRegionsConfig, resolve};
use catenary::routing_common::timetable::{MAX_DAYS, TimetableOwned};
use clap::Parser;
use chrono::Utc;
use postgres::{Client, NoTls};
mod compile;
mod external_assembly;
mod file_sizes;
mod footpaths;
mod transfer_rules;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Compile regional RAPTOR timetables with native OSR footpaths"
)]
struct Args {
    /// Output V4 seconds-precision binary. Optional when only listing selected chateaux.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// First UTC traffic day in Unix days. Defaults to yesterday (UTC).
    #[arg(long)]
    first_utc_day: Option<i32>,

    /// Number of UTC days to compile. Defaults to Nigiri's 365 days plus
    /// one lookback week.
    #[arg(long)]
    day_count: Option<u32>,

    /// Register sources as CHATEAU,ONESTOP_FEED_ID,ATTEMPT_ID; repeatable.
    #[arg(
        long = "source",
        value_name = "CHATEAU,FEED,ATTEMPT",
        conflicts_with = "region"
    )]
    sources: Vec<String>,

    /// Resolve this configured region through PostGIS, including whole chateaux.
    #[arg(long, value_name = "REGION_ID", conflicts_with = "sources")]
    region: Option<String>,

    /// TOML definition of routable regions.
    #[arg(long, default_value = "routing_regions.toml")]
    regions_config: PathBuf,

    /// PostgreSQL connection string. Defaults to DATABASE_URL.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, requires = "region", conflicts_with = "list_chateaux")]
    build: bool,

    /// Directory containing an extracted native OSR graph. Transfers are computed
    /// in-process against the exact location indexes being compiled.
    #[arg(long, requires = "build")]
    osr_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 900)]
    max_seconds: u32,

    #[arg(long, default_value_t = 1200.0)]
    candidate_meters: f64,

    /// Maximum in-memory schedule buffer for sorting and route assembly.
    /// This does not cap stop indexes, final timetable columns or OSR memory.
    #[arg(long, default_value_t = 64)]
    schedule_memory_mib: usize,

    /// Scratch filesystem for external schedule runs. Defaults to the output
    /// directory so /tmp tmpfs does not consume the compiler's memory budget.
    #[arg(long, value_name = "DIRECTORY", requires = "build")]
    scratch_dir: Option<PathBuf>,

    /// Print resolved chateaux and active feed sources as JSON without writing a timetable.
    #[arg(long, requires = "region")]
    list_chateaux: bool,

    /// Print the actual file size of every configured region timetable.
    /// Does not require a database connection.
    #[arg(long, conflicts_with_all = ["region", "sources", "build", "list_chateaux"])]
    list_timetable_sizes: bool,

    /// Override the configured timetable directory when listing file sizes.
    #[arg(long, requires = "list_timetable_sizes")]
    data_dir: Option<PathBuf>,
}

/// MOTIS/Nigiri defaults to TODAY with 365 service days. Its import interval
/// additionally starts one day earlier so previous-day overnight services
/// remain available at the beginning of the requested timetable.
const DEFAULT_SERVICE_DAYS: u32 = 365;
const LOOKBACK_DAYS: i32 = 7;

fn resolve_horizon(
    first_day_override: Option<i32>,
    day_count_override: Option<u32>,
    today_utc_day: i32,
) -> Result<(i32, u32)> {
    let first_utc_day = match first_day_override {
        Some(day) => day,
        None => today_utc_day
            .checked_sub(LOOKBACK_DAYS)
            .context("UTC day underflow computing timetable lookback")?,
    };
    let day_count = day_count_override.unwrap_or(DEFAULT_SERVICE_DAYS + LOOKBACK_DAYS as u32);
    ensure!(
        (1..=MAX_DAYS as u32).contains(&day_count),
        "day count must be between 1 and {MAX_DAYS}"
    );
    first_utc_day
        .checked_add(i32::try_from(day_count)?)
        .context("timetable horizon exceeds i32 UTC day range")?;
    Ok((first_utc_day, day_count))
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.list_timetable_sizes {
        let config = RoutingRegionsConfig::load(&args.regions_config)?;
        let dir = args.data_dir.as_deref().unwrap_or(&config.data_dir);
        return file_sizes::print_configured(&config, dir);
    }
    let selection = if let Some(id) = &args.region {
        let config = RoutingRegionsConfig::load(&args.regions_config)?;
        let url = args
            .database_url
            .as_deref()
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

    let utc_day_today = i32::try_from(Utc::now().timestamp().div_euclid(86_400))
        .context("current UTC day exceeds i32 range")?;
    let (first_utc_day, day_count) =
        resolve_horizon(args.first_utc_day, args.day_count, utc_day_today)?;
    let start = chrono::DateTime::<Utc>::from_timestamp(i64::from(first_utc_day) * 86_400, 0)
        .context("invalid start of timetable horizon")?;
    let last = chrono::DateTime::<Utc>::from_timestamp(
        (i64::from(first_utc_day) + i64::from(day_count) - 1) * 86_400,
        0,
    )
    .context("invalid end of timetable horizon")?;
    eprintln!(
        "Gentian timetable horizon: {} through {} ({} UTC days)",
        start.date_naive(),
        last.date_naive(),
        day_count
    );

    if args.build {
        let selection = selection.as_ref().context("--region is required")?;
        let url = args
            .database_url
            .as_deref()
            .context("DATABASE_URL is required")?;
        let mut client = Client::connect(url, NoTls)?;
        let output = args
            .output
            .as_ref()
            .context("--output is required with --build")?;
        let osr_dir = args
            .osr_dir
            .as_deref()
            .context("--osr-dir is required for native OSR transfer compilation")?;
        compile::build(
            &mut client,
            &selection.sources,
            first_utc_day,
            day_count,
            osr_dir,
            args.max_seconds,
            args.candidate_meters,
            output,
            args.scratch_dir.as_deref(),
            args.schedule_memory_mib,
        )?;
        return file_sizes::print_published(&selection.id, output);
    }
    let output = args.output.as_ref().context("--output is required")?;
    let mut tt = TimetableOwned::empty(first_utc_day, day_count);
    if let Some(selection) = &selection {
        ensure!(
            !selection.chateaux.is_empty(),
            "routing region {} intersects no chateaux",
            selection.id
        );
        ensure!(
            !selection.sources.is_empty(),
            "routing region {} has no completed production GTFS imports",
            selection.id
        );
        for source in &selection.sources {
            tt.add_source(&source.chateau, &source.onestop_feed_id, &source.attempt_id)?;
        }
    } else {
        for value in &args.sources {
            let components: Vec<_> = value.splitn(3, ',').collect();
            ensure!(
                components.len() == 3 && components.iter().all(|x| !x.is_empty()),
                "invalid --source: expected CHATEAU,FEED,ATTEMPT"
            );
            tt.add_source(components[0], components[1], components[2])?;
        }
    }

    let bytes = catenary::routing_common::timetable_mmap::publish(&tt, output)?;
    println!(
        "Wrote V4 seconds-precision schema-only timetable: {} bytes, {} sources; no compiled routes or transports",
        bytes,
        tt.sources.len()
    );
    if let Some(selection) = &selection {
        file_sizes::print_published(&selection.id, output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_horizon_is_today_with_one_lookback_day_and_365_service_days() {
        // 2026-10-22 is UTC day 20748.
        assert_eq!(resolve_horizon(None, None, 20748).unwrap(), (20747, 366));
    }

    #[test]
    fn explicit_horizon_overrides_are_independent() {
        assert_eq!(resolve_horizon(Some(0), None, 20748).unwrap(), (0, 366));
        assert_eq!(resolve_horizon(None, Some(35), 20748).unwrap(), (20747, 35));
        assert_eq!(
            resolve_horizon(Some(20745), Some(35), 20748).unwrap(),
            (20745, 35)
        );
    }

    #[test]
    fn invalid_horizons_are_rejected() {
        assert!(resolve_horizon(None, Some(0), 20748).is_err());
        assert!(resolve_horizon(None, Some(MAX_DAYS as u32 + 1), 20748).is_err());
        assert!(resolve_horizon(None, None, i32::MIN).is_err());
        assert!(resolve_horizon(Some(i32::MAX), Some(2), 20748).is_err());
    }

    #[test]
    fn date_flags_are_optional() {
        let args = Args::try_parse_from(["gentian", "--output", "timetable.bin"]).unwrap();
        assert!(args.first_utc_day.is_none());
        assert!(args.day_count.is_none());
    }
}
