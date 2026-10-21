//! External sort for expanded GTFS schedules.
//!
//! The disk key is (source, public route, stop sequence, departure, trip ID,
//! relative stop events, traffic days). All records for a route pattern are
//! adjacent after the merge. Keeping a bounded prefix of one pattern is safe:
//! independent RAPTOR routes must each be non-overtaking, but need not contain
//! every compatible trip of the pattern.
//!
//! Runs and merge intermediates live beside the output by default, *not* in
//! /tmp (which is commonly tmpfs on build hosts).
use super::compile::{Event, Schedule};
use anyhow::{Context, Result, bail, ensure};
use catenary::routing_common::timetable::{LocationIdx, TrafficDays};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

const FAN_IN: usize = 16;
const IO_BUFFER: usize = 128 * 1024;

/// Approximate the retained heap footprint, including Vec capacities and
/// per-record allocator metadata. The limit is a bound on records retained by
/// external sorting/assembly, not on timetable columns or OSR's graph.
pub(super) fn charge(s: &Schedule) -> usize {
    size_of::<Schedule>()
        .saturating_add(s.trip_id.capacity())
        .saturating_add(s.route_id.capacity())
        .saturating_add(s.locations.capacity().saturating_mul(size_of::<LocationIdx>()))
        .saturating_add(s.events.capacity().saturating_mul(size_of::<Event>()))
        .saturating_add(128)
}

fn compare(a: &Schedule, b: &Schedule) -> Ordering {
    a.source.cmp(&b.source)
        .then_with(|| a.route_id.cmp(&b.route_id))
        .then_with(|| a.locations.iter().map(|x| x.0).cmp(b.locations.iter().map(|x| x.0)))
        .then_with(|| a.first_second.cmp(&b.first_second))
        .then_with(|| a.trip_id.cmp(&b.trip_id))
        // Stable, deterministic ordering for repeated trips with different
        // timezone-dependent relative events on distinct service days.
        .then_with(|| a.events.iter().map(|e| (e.arrival, e.departure))
            .cmp(b.events.iter().map(|e| (e.arrival, e.departure))))
        .then_with(|| a.days.words.cmp(&b.days.words))
}

pub(super) fn same_pattern(a: &Schedule, b: &Schedule) -> bool {
    a.source == b.source && a.route_id == b.route_id && a.locations == b.locations
}

struct Scratch {
    dir: PathBuf,
    serial: u64,
}

impl Scratch {
    fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).with_context(|| format!("creating scratch root {}", root.display()))?;
        let dir = root.join(format!("gentian-sort-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).with_context(|| format!("creating {}", dir.display()))?;
        Ok(Self { dir, serial: 0 })
    }

    fn next(&mut self) -> PathBuf {
        let index = self.serial;
        self.serial += 1;
        self.dir.join(format!("{index:016x}.run"))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.dir) {
            eprintln!("Gentian: unable to remove scratch directory {}: {error}", self.dir.display());
        }
    }
}

pub(super) struct ScheduleRuns {
    scratch: Scratch,
    records: Vec<Schedule>,
    bytes: usize,
    limit: usize,
    runs: Vec<PathBuf>,
    count: u64,
}

impl ScheduleRuns {
    pub fn new(scratch_root: &Path, memory_mib: usize) -> Result<Self> {
        ensure!((1..=4096).contains(&memory_mib), "schedule buffer must be 1..=4096 MiB");
        let limit = memory_mib.checked_mul(1024 * 1024).context("schedule buffer overflow")?;
        Ok(Self {
            scratch: Scratch::new(scratch_root)?, records: Vec::new(),
            bytes: 0, limit, runs: Vec::new(), count: 0,
        })
    }

    pub fn push(&mut self, record: Schedule) -> Result<()> {
        let size = charge(&record);
        ensure!(size <= self.limit,
            "single schedule requires {size} bytes, exceeding sort buffer {} bytes",
            self.limit);
        if self.bytes > self.limit - size { self.spill()?; }
        self.bytes += size;
        self.count += 1;
        self.records.push(record);
        Ok(())
    }

    pub fn is_empty(&self) -> bool { self.count == 0 }

    fn spill(&mut self) -> Result<()> {
        if self.records.is_empty() { return Ok(()); }
        self.records.sort_by(compare);
        let path = self.scratch.next();
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        let mut out = BufWriter::with_capacity(IO_BUFFER, file);
        for record in self.records.drain(..) {
            write_record(&mut out, &record)?;
        }
        out.flush()?;
        self.bytes = 0;
        self.runs.push(path);
        Ok(())
    }

    /// Exhaust all sorted runs in bounded-memory batches. The callback is
    /// invoked for a single stop pattern and never with more than `limit`
    /// bytes of schedule payloads. Large patterns are split into multiple
    /// RAPTOR routes, all of which are independently non-overtaking.
    pub fn consume(mut self, mut on_batch: impl FnMut(Vec<Schedule>) -> Result<()>) -> Result<()> {
        self.spill()?;
        let mut pass = 0;
        while self.runs.len() > 1 {
            let previous = std::mem::take(&mut self.runs);
            for files in previous.chunks(FAN_IN) {
                let target = self.scratch.next();
                merge_files(files, &target, self.limit)?;
                self.runs.push(target);
            }
            for file in previous { fs::remove_file(file)?; }
            pass += 1;
            eprintln!("Gentian external sort: merge pass {pass}, {} runs", self.runs.len());
        }
        let Some(path) = self.runs.first() else { return Ok(()) };
        let mut input = BufReader::with_capacity(IO_BUFFER, File::open(path)?);
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut processed = 0u64;
        while let Some(schedule) = read_record(&mut input, self.limit)? {
            let size = charge(&schedule);
            ensure!(size <= self.limit, "schedule over memory cap during merge");
            let boundary = batch.first().is_some_and(|a| !same_pattern(a, &schedule))
                || batch_bytes > self.limit - size;
            if boundary {
                on_batch(std::mem::take(&mut batch))?;
                batch_bytes = 0;
            }
            batch_bytes += size;
            batch.push(schedule);
            processed += 1;
        }
        ensure!(processed == self.count, "external sort lost or duplicated schedules");
        if !batch.is_empty() { on_batch(batch)?; }
        Ok(())
    }
}

struct Entry {
    run: usize,
    schedule: Schedule,
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.schedule, &other.schedule).then_with(|| self.run.cmp(&other.run))
    }
}

fn merge_files(paths: &[PathBuf], target: &Path, limit: usize) -> Result<()> {
    let mut inputs = paths.iter().map(|p| {
        File::open(p).map(|f| BufReader::with_capacity(IO_BUFFER, f))
    }).collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<Entry>>::new();
    for (run, input) in inputs.iter_mut().enumerate() {
        if let Some(schedule) = read_record(input, limit)? {
            heap.push(Reverse(Entry { run, schedule }));
        }
    }
    let file = OpenOptions::new().write(true).create_new(true).open(target)?;
    let mut output = BufWriter::with_capacity(IO_BUFFER, file);
    while let Some(Reverse(Entry { run, schedule })) = heap.pop() {
        write_record(&mut output, &schedule)?;
        if let Some(next) = read_record(&mut inputs[run], limit)? {
            heap.push(Reverse(Entry { run, schedule: next }));
        }
    }
    output.flush()?;
    Ok(())
}

fn write_record(w: &mut impl Write, s: &Schedule) -> Result<()> {
    ensure!(s.locations.len() == s.events.len(), "schedule stop count mismatch");
    let len = 84usize
        .checked_add(s.route_id.len()).and_then(|v| v.checked_add(s.trip_id.len()))
        .and_then(|v| v.checked_add(s.events.len().checked_mul(20)?))
        .context("schedule record exceeds addressable memory")?;
    let len = u32::try_from(len).context("schedule record exceeds 4 GiB")?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&s.source.to_le_bytes())?;
    w.write_all(&s.route_type.to_le_bytes())?;
    w.write_all(&s.first_second.to_le_bytes())?;
    w.write_all(&u32::try_from(s.route_id.len())?.to_le_bytes())?;
    w.write_all(&u32::try_from(s.trip_id.len())?.to_le_bytes())?;
    w.write_all(&u32::try_from(s.events.len())?.to_le_bytes())?;
    for word in s.days.words { w.write_all(&word.to_le_bytes())?; }
    w.write_all(s.route_id.as_bytes())?;
    w.write_all(s.trip_id.as_bytes())?;
    for (location, event) in s.locations.iter().zip(&s.events) {
        ensure!(*location == event.stop, "schedule stop sequence mismatch");
        w.write_all(&location.0.to_le_bytes())?;
        w.write_all(&event.arrival.to_le_bytes())?;
        w.write_all(&event.departure.to_le_bytes())?;
    }
    Ok(())
}

fn read_record(r: &mut impl Read, limit: usize) -> Result<Option<Schedule>> {
    let mut size = [0u8; 4];
    if r.read(&mut size[..1])? == 0 { return Ok(None); }
    r.read_exact(&mut size[1..])?;
    let payload = u32::from_le_bytes(size) as usize;
    ensure!(payload >= 84 && payload <= limit, "invalid or oversized sort-run record");
    let mut reader = r.take(payload as u64);
    fn u16le(r: &mut impl Read) -> Result<u16> { let mut b = [0; 2]; r.read_exact(&mut b)?; Ok(u16::from_le_bytes(b)) }
    fn i16le(r: &mut impl Read) -> Result<i16> { let mut b = [0; 2]; r.read_exact(&mut b)?; Ok(i16::from_le_bytes(b)) }
    fn u32le(r: &mut impl Read) -> Result<u32> { let mut b = [0; 4]; r.read_exact(&mut b)?; Ok(u32::from_le_bytes(b)) }
    fn u64le(r: &mut impl Read) -> Result<u64> { let mut b = [0; 8]; r.read_exact(&mut b)?; Ok(u64::from_le_bytes(b)) }
    fn i64le(r: &mut impl Read) -> Result<i64> { let mut b = [0; 8]; r.read_exact(&mut b)?; Ok(i64::from_le_bytes(b)) }
    let source = u16le(&mut reader)?;
    let route_type = i16le(&mut reader)?;
    let first_second = u32le(&mut reader)?;
    let route_len = u32le(&mut reader)? as usize;
    let trip_len = u32le(&mut reader)? as usize;
    let stops = u32le(&mut reader)? as usize;
    let expected = 84usize.checked_add(route_len)
        .and_then(|n| n.checked_add(trip_len))
        .and_then(|n| n.checked_add(stops.checked_mul(20)?))
        .context("invalid sort-run record lengths")?;
    ensure!(expected == payload, "malformed sort-run record length");
    let mut words = [0u64; 8];
    for word in &mut words { *word = u64le(&mut reader)?; }
    let mut route = vec![0u8; route_len];
    reader.read_exact(&mut route)?;
    let route_id = String::from_utf8(route).context("invalid route ID in run")?;
    let mut trip = vec![0u8; trip_len];
    reader.read_exact(&mut trip)?;
    let trip_id = String::from_utf8(trip).context("invalid trip ID in run")?;
    let mut locations = Vec::with_capacity(stops);
    let mut events = Vec::with_capacity(stops);
    for _ in 0..stops {
        let stop = LocationIdx(u32le(&mut reader)?);
        locations.push(stop);
        events.push(Event { stop, arrival: i64le(&mut reader)?, departure: i64le(&mut reader)? });
    }
    ensure!(reader.limit() == 0, "trailing bytes in schedule record");
    Ok(Some(Schedule {
        trip_id, source, route_id, route_type, locations, events,
        first_second, days: TrafficDays { words },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(id: &str, second: u32, stops: &[u32]) -> Schedule {
        Schedule {
            trip_id: id.into(), source: 0, route_id: "R".into(), route_type: 3,
            locations: stops.iter().copied().map(LocationIdx).collect(),
            events: stops.iter().enumerate().map(|(i, &stop)| Event {
                stop: LocationIdx(stop), arrival: (i as i64) * 120,
                departure: (i as i64) * 120,
            }).collect(),
            first_second: second, days: TrafficDays::default(),
        }
    }

    #[test]
    fn external_merge_is_exact_and_stable_for_multiple_patterns() {
        let root = std::env::temp_dir().join(format!("gentian-sort-test-{}", uuid::Uuid::new_v4()));
        let mut sorter = ScheduleRuns::new(&root, 1).unwrap();
        let mut original = Vec::new();
        for i in (0..160).rev() {
            let schedule = sample(&format!("t{i:04}"), i * 60, &[0, 1, 2]);
            original.push(schedule.clone());
            sorter.push(schedule).unwrap();
        }
        let extra = sample("second-pattern", 0, &[0, 3, 2]);
        original.push(extra.clone());
        sorter.push(extra).unwrap();
        original.sort_by(compare);
        let mut actual = Vec::new();
        sorter.consume(|batch| { actual.extend(batch); Ok(()) }).unwrap();
        assert_eq!(actual.len(), original.len());
        for (a, b) in actual.iter().zip(&original) {
            assert_eq!(compare(a, b), Ordering::Equal);
            assert_eq!(a.days, b.days);
        }
        assert!(fs::read_dir(&root).unwrap().next().is_none());
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn huge_single_record_is_rejected_without_overcommitting() {
        let root = std::env::temp_dir().join(format!("gentian-sort-test-{}", uuid::Uuid::new_v4()));
        let mut sorter = ScheduleRuns::new(&root, 1).unwrap();
        let huge = sample(&"x".repeat(1_100_000), 0, &[0, 1]);
        assert!(sorter.push(huge).is_err());
        drop(sorter);
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn truncated_record_fails_closed() {
        let record = sample("x", 15, &[0, 1]);
        let mut bytes = Vec::new();
        write_record(&mut bytes, &record).unwrap();
        bytes.pop();
        assert!(read_record(&mut bytes.as_slice(), 1024 * 1024).is_err());
    }
}
