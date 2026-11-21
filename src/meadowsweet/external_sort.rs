//! Disk-based sorting of fixed-width, big-endian records.
//!
//! Records compare lexicographically; callers must put the sort key first in
//! big-endian form. No input-size-dependent collection is retained in memory.
use anyhow::{Context, Result, bail};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub fn read_record<const N: usize>(reader: &mut impl Read) -> Result<Option<[u8; N]>> {
    let mut result = [0u8; N];
    if N == 0 {
        bail!("zero-width external-sort record");
    }
    if reader.read(&mut result[..1])? == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut result[1..])
        .context("truncated fixed-width record")?;
    Ok(Some(result))
}

pub fn write_record<const N: usize>(writer: &mut impl Write, record: &[u8; N]) -> Result<()> {
    writer.write_all(record)?;
    Ok(())
}

#[derive(Eq, PartialEq)]
struct Head<const N: usize> {
    bytes: [u8; N],
    run: usize,
}
impl<const N: usize> Ord for Head<N> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes).then(self.run.cmp(&other.run))
    }
}
impl<const N: usize> PartialOrd for Head<N> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Sort a file of N-byte records using bounded runs and a bounded merge fan-in.
/// The temporary directory must be unique to this invocation.
pub fn sort_fixed<const N: usize>(
    input: &Path,
    output: &Path,
    temp_dir: &Path,
    memory_bytes: usize,
    fan_in: usize,
) -> Result<u64> {
    if N == 0 || memory_bytes < N || fan_in < 2 {
        bail!("invalid external sort configuration");
    }
    fs::create_dir_all(temp_dir)?;
    let mut source = BufReader::new(File::open(input)?);
    let capacity = memory_bytes / N;
    let mut runs = Vec::<PathBuf>::new();
    let mut count = 0u64;

    loop {
        let mut batch = Vec::<[u8; N]>::with_capacity(capacity);
        for _ in 0..capacity {
            match read_record(&mut source)? {
                Some(value) => {
                    count += 1;
                    batch.push(value);
                }
                None => break,
            }
        }
        if batch.is_empty() {
            break;
        }
        batch.sort_unstable();
        let path = temp_dir.join(format!("initial_{:08}.bin", runs.len()));
        let mut writer = BufWriter::new(File::create(&path)?);
        for record in &batch {
            write_record(&mut writer, record)?;
        }
        writer.flush()?;
        runs.push(path);
    }
    if runs.is_empty() {
        File::create(output)?;
        return Ok(0);
    }

    let mut pass = 0usize;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, subset) in runs.chunks(fan_in).enumerate() {
            let path = temp_dir.join(format!("pass_{pass:04}_{group:08}.bin"));
            merge_runs::<N>(subset, &path)?;
            next.push(path);
        }
        for file in runs {
            fs::remove_file(file)?;
        }
        runs = next;
        pass += 1;
    }
    fs::rename(&runs[0], output).with_context(|| {
        format!("move externally sorted output to {}", output.display())
    })?;
    Ok(count)
}

fn merge_runs<const N: usize>(paths: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers: Vec<_> = paths
        .iter()
        .map(|p| File::open(p).map(BufReader::new))
        .collect::<std::io::Result<_>>()?;
    let mut heap = BinaryHeap::<Reverse<Head<N>>>::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(bytes) = read_record(reader)? {
            heap.push(Reverse(Head { bytes, run }));
        }
    }
    let mut writer = BufWriter::new(File::create(output)?);
    while let Some(Reverse(Head { bytes, run })) = heap.pop() {
        write_record(&mut writer, &bytes)?;
        if let Some(bytes) = read_record(&mut readers[run])? {
            heap.push(Reverse(Head { bytes, run }));
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sort_multiple_runs_and_preserve_duplicates() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-sort-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir_all(&dir)?;
        let input = dir.join("input");
        let output = dir.join("output");
        {
            let mut file = File::create(&input)?;
            for value in [7u64, 2, 5, 2, 9, 1, 8, 3, 4, 6] {
                file.write_all(&value.to_be_bytes())?;
            }
        }
        assert_eq!(sort_fixed::<8>(&input, &output, &dir.join("scratch"), 16, 2)?, 10);
        let mut reader = BufReader::new(File::open(output)?);
        let mut actual = Vec::new();
        while let Some(v) = read_record::<8>(&mut reader)? {
            actual.push(u64::from_be_bytes(v));
        }
        assert_eq!(actual, [1, 2, 2, 3, 4, 5, 6, 7, 8, 9]);
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn reject_truncated_records() -> Result<()> {
        assert!(read_record::<8>(&mut &b"abc"[..]).is_err());
        Ok(())
    }
}
