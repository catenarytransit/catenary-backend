//! Content identity for graph-local native stop/way indexes. Compute once per
//! graph load, never per route. Include every binary artifact (also platforms).
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path};

pub fn fingerprint(dir: &Path) -> Result<[u8; 32]> {
    let mut files = fs::read_dir(dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.retain(|path| path.extension().is_some_and(|ext| ext == "bin"));
    files.sort();
    ensure!(
        files
            .iter()
            .any(|p| p.file_name().is_some_and(|n| n == "routing.bin")),
        "OSR graph lacks routing.bin: {}",
        dir.display()
    );
    let mut hash = Sha256::new();
    hash.update(b"catenary-osr-raw-way-v1\0");
    let mut buffer = vec![0u8; 1024 * 1024];
    for path in files {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("non-UTF8 OSR filename")?;
        hash.update((name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        let mut file = fs::File::open(&path)?;
        let before = file.metadata()?;
        hash.update(before.len().to_le_bytes());
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let after = file.metadata()?;
        ensure!(
            before.len() == after.len() && before.modified()? == after.modified()?,
            "OSR graph changed while fingerprinting {}",
            path.display()
        );
    }
    Ok(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn graph_identity_checks_contents_and_names_but_not_directory() {
        let root = std::env::temp_dir().join(format!("osr-identity-{}", std::process::id()));
        fs::create_dir_all(root.join("a")).unwrap();
        fs::create_dir_all(root.join("b")).unwrap();
        for part in ["a", "b"] {
            fs::write(root.join(part).join("routing.bin"), b"abcd").unwrap();
        }
        let first = fingerprint(&root.join("a")).unwrap();
        assert_eq!(first, fingerprint(&root.join("b")).unwrap());
        fs::write(root.join("b/routing.bin"), b"abce").unwrap();
        assert_ne!(first, fingerprint(&root.join("b")).unwrap());
        fs::write(root.join("a/platforms.bin"), b"abcd").unwrap();
        assert_ne!(first, fingerprint(&root.join("a")).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}
