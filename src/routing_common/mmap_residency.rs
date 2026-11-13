//! One-time startup advice for owned file mappings. Never advise heap ranges.
use memmap2::Mmap;
use serde::Deserialize;
use std::io;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StartupMmapAdvice {
    #[default]
    None,
    Cold,
    Pageout,
}

impl StartupMmapAdvice {
    /// Successful bytes advised, not bytes reclaimed. Contents stay valid.
    pub fn advise(self, mapping: &Mmap) -> io::Result<usize> {
        if self == Self::None || mapping.is_empty() {
            return Ok(0);
        }
        #[cfg(target_os = "linux")]
        {
            let advice = match self {
                Self::Cold => libc::MADV_COLD,
                Self::Pageout => libc::MADV_PAGEOUT,
                Self::None => unreachable!(),
            };
            // SAFETY: Mmap owns the page-aligned range and remains alive for
            // the call. COLD/PAGEOUT preserve bytes, unlike DONTNEED on COW.
            if unsafe { libc::madvise(mapping.as_ptr().cast_mut().cast(), mapping.len(), advice) }
                == -1
            {
                return Err(io::Error::last_os_error());
            }
            Ok(mapping.len())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "startup mmap advice requires Linux 5.4+",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Write;

    #[test]
    fn file_contents_survive_startup_advice() {
        let path = std::env::temp_dir().join(format!("edelweiss-residency-{}", std::process::id()));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let data: Vec<_> = (0..131_072).map(|i| (i % 251) as u8).collect();
        file.write_all(&data).unwrap();
        file.sync_all().unwrap();
        let mapping = unsafe { Mmap::map(&file) }.unwrap();
        assert_eq!(StartupMmapAdvice::None.advise(&mapping).unwrap(), 0);
        for advice in [StartupMmapAdvice::Cold, StartupMmapAdvice::Pageout] {
            assert_eq!(&mapping[..], data.as_slice());
            let result = advice.advise(&mapping);
            #[cfg(target_os = "linux")]
            assert_eq!(result.unwrap(), data.len());
            #[cfg(not(target_os = "linux"))]
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
            assert_eq!(&mapping[..], data.as_slice());
        }
        assert_eq!(fs::read(&path).unwrap(), data);
        drop(mapping);
        drop(file);
        fs::remove_file(path).unwrap();
    }
}
