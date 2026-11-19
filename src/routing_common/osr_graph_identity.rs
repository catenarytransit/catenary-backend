//! OSR content identity: compute once on publication, read a tiny manifest at runtime.
//! The legacy SHA-256 is retained so previously compiled Gentian timetables remain usable.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path};

pub const IDENTITY_FILE: &str = "graph-identity.json";
const PREFIX: &[u8] = b"catenary-osr-raw-way-v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphIdentity {
    pub schema_version: u32,
    pub blake3: String,
    pub legacy_sha256: String,
}

fn checked_bytes(s: &str) -> Result<[u8; 32]> {
    ensure!(s.len() == 64, "OSR identity must contain 64 hex characters");
    Ok(hex::decode(s)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid OSR identity length"))?)
}

pub fn read_published(dir: &Path) -> Result<Option<GraphIdentity>> {
    let bytes = match fs::read(dir.join(IDENTITY_FILE)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let identity: GraphIdentity = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", dir.join(IDENTITY_FILE).display()))?;
    ensure!(
        identity.schema_version == 1,
        "unsupported OSR graph identity schema"
    );
    checked_bytes(&identity.blake3)?;
    checked_bytes(&identity.legacy_sha256)?;
    Ok(Some(identity))
}

pub fn published_fingerprints(dir: &Path) -> Result<Option<([u8; 32], [u8; 32])>> {
    read_published(dir)?
        .map(|identity| {
            Ok((
                checked_bytes(&identity.blake3)?,
                checked_bytes(&identity.legacy_sha256)?,
            ))
        })
        .transpose()
}

/// Offline-only. Includes all .bin filenames, lengths and contents.
pub fn compute(dir: &Path) -> Result<GraphIdentity> {
    let mut files = fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "bin"));
    files.sort();
    ensure!(
        files
            .iter()
            .any(|p| p.file_name().is_some_and(|n| n == "routing.bin")),
        "OSR graph lacks routing.bin: {}",
        dir.display()
    );
    let mut fast = blake3::Hasher::new();
    let mut legacy = Sha256::new();
    fast.update(PREFIX);
    legacy.update(PREFIX);
    let mut buffer = vec![0u8; 1024 * 1024];
    for path in files {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("non-UTF8 OSR filename")?;
        let mut f = fs::File::open(&path)?;
        let before = f.metadata()?;
        let name_len = (name.len() as u64).to_le_bytes();
        let file_len = before.len().to_le_bytes();
        for part in [&name_len[..], name.as_bytes(), &file_len[..]] {
            fast.update(part);
            legacy.update(part);
        }
        loop {
            let n = f.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            fast.update(&buffer[..n]);
            legacy.update(&buffer[..n]);
        }
        let after = f.metadata()?;
        ensure!(
            before.len() == after.len() && before.modified()? == after.modified()?,
            "OSR graph changed while fingerprinting {}",
            path.display()
        );
    }
    Ok(GraphIdentity {
        schema_version: 1,
        blake3: fast.finalize().to_hex().to_string(),
        legacy_sha256: hex::encode(legacy.finalize()),
    })
}

/// Used by Gentian. No graph scan when a published identity is present.
pub fn fingerprint(dir: &Path) -> Result<[u8; 32]> {
    if let Some((fast, _)) = published_fingerprints(dir)? {
        return Ok(fast);
    }
    checked_bytes(&compute(dir)?.blake3)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_digest_matches_original_sha256_encoding() {
        let root = std::env::temp_dir().join(format!("osr-legacy-id-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("routing.bin"), b"abcd").unwrap();
        let identity = compute(&root).unwrap();
        let mut hash = Sha256::new();
        hash.update(PREFIX);
        hash.update((11u64).to_le_bytes());
        hash.update(b"routing.bin");
        hash.update((4u64).to_le_bytes());
        hash.update(b"abcd");
        assert_eq!(identity.legacy_sha256, hex::encode(hash.finalize()));
        assert_eq!(identity.legacy_sha256.len(), 64);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn published_identity_is_read_without_scanning_binaries() {
        let root = std::env::temp_dir().join(format!("osr-id-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("routing.bin"), b"abcd").unwrap();
        let id = compute(&root).unwrap();
        fs::write(root.join(IDENTITY_FILE), serde_json::to_vec(&id).unwrap()).unwrap();
        let saved = fingerprint(&root).unwrap();
        fs::remove_file(root.join("routing.bin")).unwrap();
        assert_eq!(fingerprint(&root).unwrap(), saved);
        fs::remove_dir_all(root).unwrap();
    }
}
