use anyhow::{Context, Result, bail};
use bytes::Bytes;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

pub const FEED_ID: &str = "f-u0-switzerland";

const ARCHIVE_URL: &str = "https://gh-proxy.org/https://github.com/catenarytransit/pfaedled-gtfs-actions/releases/download/latest/ch_pfaedle_shapes_only.zip";
const ARCHIVE_FILE_NAME: &str = "f-u0-switzerland.pfaedle.zip";
const REQUIRED_FILES: [&str; 2] = ["trips.txt", "shapes.txt"];

/// A concurrently running download of the Pfaedle overlay.
///
/// Dropping this value cancels unfinished network work. This is important because
/// Maple may discover that the source GTFS does not need ingestion after the
/// auxiliary request has already started.
pub struct PendingArchiveDownload {
    task: Option<JoinHandle<Result<Bytes>>>,
}

impl PendingArchiveDownload {
    pub fn start(client: reqwest::Client) -> Self {
        let task = tokio::spawn(async move { download(&client).await });
        Self { task: Some(task) }
    }

    pub async fn finish(mut self) -> Result<Bytes> {
        let task = self
            .task
            .take()
            .expect("pending Switzerland Pfaedle download lost its task");

        task.await
            .context("joining Switzerland Pfaedle download task")?
    }
}

impl Drop for PendingArchiveDownload {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn download(client: &reqwest::Client) -> Result<Bytes> {
    let response = client
        .get(ARCHIVE_URL)
        .timeout(Duration::from_secs(10000))
        .send()
        .await
        .context("requesting Switzerland Pfaedle overlay")?
        .error_for_status()
        .context("Switzerland Pfaedle overlay returned an unsuccessful status")?;

    response
        .bytes()
        .await
        .context("reading Switzerland Pfaedle overlay response body")
}

pub async fn store_archive(gtfs_temp_storage: &str, archive: &Bytes) -> Result<()> {
    let destination = archive_path(gtfs_temp_storage);
    let temporary = destination.with_file_name(format!("{ARCHIVE_FILE_NAME}.tmp"));

    if let Err(error) = tokio::fs::write(&temporary, archive).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| {
            format!(
                "writing temporary Switzerland Pfaedle archive {}",
                temporary.display()
            )
        });
    }

    if let Err(error) = tokio::fs::rename(&temporary, &destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| {
            format!(
                "installing Switzerland Pfaedle archive {}",
                destination.display()
            )
        });
    }

    Ok(())
}

/// Replace exactly the Pfaedle-generated GTFS tables in an already flattened
/// Switzerland feed. The archive is never extracted wholesale: only the two
/// expected root entries are streamed to staging files and then renamed over
/// the source tables.
pub fn apply_overlay(gtfs_temp_storage: &str, gtfs_directory: &Path) -> Result<()> {
    let archive_path = archive_path(gtfs_temp_storage);
    let archive_file = File::open(&archive_path).with_context(|| {
        format!(
            "opening Switzerland Pfaedle archive {}",
            archive_path.display()
        )
    })?;
    let mut archive = zip::ZipArchive::new(archive_file)
        .context("parsing Switzerland Pfaedle overlay as a ZIP archive")?;

    let staged_paths =
        REQUIRED_FILES.map(|file_name| gtfs_directory.join(format!(".{file_name}.pfaedle.tmp")));

    for staged_path in &staged_paths {
        let _ = fs::remove_file(staged_path);
    }

    let stage_result: Result<()> = (|| {
        for (&file_name, staged_path) in REQUIRED_FILES.iter().zip(staged_paths.iter()) {
            let mut source = archive.by_name(file_name).with_context(|| {
                format!("Switzerland Pfaedle archive is missing required file {file_name}")
            })?;

            if source.is_dir() {
                bail!("Switzerland Pfaedle ZIP entry {file_name} is a directory");
            }

            let mut destination = File::create(staged_path).with_context(|| {
                format!("creating staged overlay file {}", staged_path.display())
            })?;

            io::copy(&mut source, &mut destination).with_context(|| {
                format!("extracting {file_name} from Switzerland Pfaedle archive")
            })?;
            destination.flush().with_context(|| {
                format!("flushing staged overlay file {}", staged_path.display())
            })?;
        }

        Ok(())
    })();

    if let Err(error) = stage_result {
        for staged_path in &staged_paths {
            let _ = fs::remove_file(staged_path);
        }
        return Err(error);
    }

    for (&file_name, staged_path) in REQUIRED_FILES.iter().zip(staged_paths.iter()) {
        let destination = gtfs_directory.join(file_name);
        fs::rename(staged_path, &destination).with_context(|| {
            format!(
                "replacing {} with Switzerland Pfaedle output",
                destination.display()
            )
        })?;
    }

    Ok(())
}

fn archive_path(gtfs_temp_storage: &str) -> PathBuf {
    Path::new(gtfs_temp_storage).join(ARCHIVE_FILE_NAME)
}
