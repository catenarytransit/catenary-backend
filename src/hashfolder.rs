//! Computes a folder hash using SipHash-2-4 with a fixed zero key.

use async_recursion::async_recursion;
use futures::stream::StreamExt;
use path_clean::PathClean;
use siphasher::sip::SipHasher24; // Import SipHasher24
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher}; // Keep Hash and Hasher traits
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReadDirStream;

const BUFFER_SIZE: usize = 8192;

// --- Fixed Zero Key for SipHash ---
// WARNING: Using a fixed public key offers limited security benefits compared
// to a secret key and is significantly less secure than cryptographic hashes.
const SIPHASH_ZERO_KEY: [u8; 16] = [0u8; 16]; // 16-byte zero key

#[derive(Error, Debug)]
pub enum FolderHashError {
    #[error("I/O error accessing path '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Failed to strip prefix '{prefix}' from path '{path}'")]
    StripPrefix { prefix: PathBuf, path: PathBuf },
    #[error("Error joining concurrent task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Invalid path: Not UTF-8")]
    InvalidPath(PathBuf),
}

macro_rules! io_err {
    // Same helper macro
    ($path:expr, $err:expr) => {
        FolderHashError::Io {
            path: $path.to_path_buf(),
            source: $err,
        }
    };
}

type FileHashMap = BTreeMap<PathBuf, u64>;

/// Computes the SipHash-2-4 hash using the fixed zero key.
async fn hash_file_content_sip_zero(file_path: &Path) -> Result<u64, FolderHashError> {
    let mut file = File::open(file_path)
        .await
        .map_err(|e| io_err!(file_path, e))?;
    // Use the fixed zero key
    let mut hasher = SipHasher24::new_with_key(&SIPHASH_ZERO_KEY);
    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let n = file
            .read(&mut buffer)
            .await
            .map_err(|e| io_err!(file_path, e))?;
        if n == 0 {
            break;
        }
        // Use the standard Hasher trait method
        hasher.write(&buffer[..n]);
    }
    Ok(hasher.finish())
}

/// Recursively walks directory, computes SipHash (zero key) for non-PDF files.
#[async_recursion]
async fn collect_file_hashes_sip_zero(
    base_path: &Path,
    current_path: &Path,
    file_hashes: &mut FileHashMap,
) -> Result<(), FolderHashError> {
    let mut entries = ReadDirStream::new(
        fs::read_dir(current_path)
            .await
            .map_err(|e| io_err!(current_path, e))?,
    );

    while let Some(entry_result) = entries.next().await {
        let entry = entry_result.map_err(|e| io_err!(current_path, e))?;
        let path = entry.path();
        let metadata = entry.metadata().await.map_err(|e| io_err!(&path, e))?;

        if metadata.is_dir() {
            // Recurse
            collect_file_hashes_sip_zero(base_path, &path, file_hashes).await?;
        } else if metadata.is_file() {
            if path.extension().map_or(false, |ext| ext == "pdf") {
                continue;
            }

            let relative_path = path
                .strip_prefix(base_path)
                .map_err(|_| FolderHashError::StripPrefix {
                    prefix: base_path.to_path_buf(),
                    path: path.clone(),
                })?
                .to_path_buf()
                .components()
                .map(|comp| {
                    comp.as_os_str()
                        .to_str()
                        .ok_or_else(|| FolderHashError::InvalidPath(path.clone()))
                })
                .collect::<Result<PathBuf, _>>()?;

            let cleaned_relative_path = relative_path.clean();

            // Compute SipHash using the fixed zero key
            let hash = hash_file_content_sip_zero(&path).await?;
            file_hashes.insert(cleaned_relative_path, hash);
        }
    }
    Ok(())
}

/// Computes a deterministic SipHash-2-4 hash using a fixed zero key.
/// (Public interface)
///
/// Calculates the SipHash (zero key) for each non-PDF file, stores these
/// in a `BTreeMap<PathBuf, u64>`, then calculates the SipHash (zero key)
/// of the entire map structure directly using the `Hash` trait.
///
/// **Warning:** See warnings at the top of the file. Reproducible and faster
/// than SHA-256, but not cryptographically secure, especially with a zero key.
pub async fn hash_folder_sip_zero(folder_path: &Path) -> Result<u64, FolderHashError> {
    let mut file_hashes: FileHashMap = BTreeMap::new();

    // 1. Collect file hashes using SipHash with fixed zero key
    collect_file_hashes_sip_zero(folder_path, folder_path, &mut file_hashes).await?;

    // 2. Hash the BTreeMap *directly* using SipHash with the same fixed zero key
    let mut final_hasher = SipHasher24::new_with_key(&SIPHASH_ZERO_KEY);
    file_hashes.hash(&mut final_hasher);
    let final_hash = final_hasher.finish();

    Ok(final_hash)
}
