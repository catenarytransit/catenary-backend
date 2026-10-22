//! Compact terminal progress reporting for Globeflower's database loader.
//! We deliberately avoid a dependency and never emit carriage returns into CI logs.

use log::info;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

const BAR_WIDTH: usize = 32;
const REDRAW_INTERVAL: Duration = Duration::from_millis(150);

/// A finite progress bar, driven by *completed* database batches.
/// Interactive terminals get an in-place bar; redirected output gets
/// occasional newline-delimited log records instead.
pub(crate) struct LoadProgress {
    stage: &'static str,
    label: &'static str,
    total: usize,
    done: usize,
    started: Instant,
    last_draw: Instant,
    last_log_decile: usize,
    interactive: bool,
    finished: bool,
}

impl LoadProgress {
    pub(crate) fn new(label: &'static str, total: usize) -> Self {
        Self::for_stage("planner", label, total)
    }

    pub(crate) fn for_stage(stage: &'static str, label: &'static str, total: usize) -> Self {
        let now = Instant::now();
        let mut progress = Self {
            stage,
            label,
            total,
            done: 0,
            started: now,
            last_draw: now,
            last_log_decile: 0,
            interactive: io::stderr().is_terminal(),
            finished: false,
        };
        if progress.interactive {
            progress.draw();
        } else {
            info!("[progress] {}: 0/{} (0%)", label, total);
        }
        progress
    }

    /// Call only after a batch has been fully fetched and processed.
    pub(crate) fn advance(&mut self, delta: usize) {
        self.done = self.done.saturating_add(delta).min(self.total);
        if self.interactive {
            if self.last_draw.elapsed() >= REDRAW_INTERVAL || self.done == self.total {
                self.draw();
                self.last_draw = Instant::now();
            }
        } else {
            // Log no more than ten progress updates per stage, even for
            // thousands of individual batches or non-interactive CI runs.
            let decile = fraction(self.done, self.total, 10);
            if decile > self.last_log_decile {
                self.last_log_decile = decile;
                info!(
                    "[progress] {}: {}/{} ({}%)",
                    self.label,
                    self.done,
                    self.total,
                    fraction(self.done, self.total, 100)
                );
            }
        }
    }

    pub(crate) fn finish(mut self) {
        // Normal completion is explicit; errors should not show 100%.
        if self.interactive {
            self.draw();
            let _ = writeln!(io::stderr().lock());
        } else {
            info!(
                "[progress] {} complete: {}/{} in {:.1}s",
                self.label,
                self.done,
                self.total,
                self.started.elapsed().as_secs_f64()
            );
        }
        self.finished = true;
    }

    fn draw(&self) {
        let rendered = format_bar(
            self.stage,
            self.label,
            self.done,
            self.total,
            self.started.elapsed(),
        );
        let mut output = io::stderr().lock();
        let _ = write!(output, "\r\x1b[2K{rendered}");
        let _ = output.flush();
    }
}

impl Drop for LoadProgress {
    fn drop(&mut self) {
        if self.interactive && !self.finished {
            // If a SQL query fails, end the incomplete bar on its own line
            // before the error is printed by main.
            let _ = writeln!(io::stderr().lock());
        }
    }
}

fn fraction(done: usize, total: usize, scale: usize) -> usize {
    if total == 0 {
        return scale;
    }
    ((done.min(total) as u128 * scale as u128) / total as u128) as usize
}

fn format_bar(stage: &str, label: &str, done: usize, total: usize, elapsed: Duration) -> String {
    let filled = fraction(done, total, BAR_WIDTH);
    let bar = format!("{}{}", "#".repeat(filled), "-".repeat(BAR_WIDTH - filled));
    let percent = fraction(done, total, 100);
    let eta = if done == 0 || elapsed < Duration::from_secs(1) || done >= total {
        "--:--".to_owned()
    } else {
        let rate = done as f64 / elapsed.as_secs_f64();
        let remaining = ((total - done) as f64 / rate).ceil() as u64;
        if remaining >= 3600 {
            format!(
                "{:02}:{:02}:{:02}",
                remaining / 3600,
                (remaining / 60) % 60,
                remaining % 60
            )
        } else {
            format!("{:02}:{:02}", remaining / 60, remaining % 60)
        }
    };
    format!(
        "[{stage}] {label} [{bar}] {done}/{total} ({percent}%) elapsed {:.1}s ETA {eta}",
        elapsed.as_secs_f64()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fraction_is_bounded_and_safe_for_empty_totals() {
        assert_eq!(fraction(0, 0, 100), 100);
        assert_eq!(fraction(0, 4, 100), 0);
        assert_eq!(fraction(1, 4, 100), 25);
        assert_eq!(fraction(2, 4, 100), 50);
        assert_eq!(fraction(5, 4, 100), 100);
        assert_eq!(fraction(usize::MAX, usize::MAX, 32), 32);
    }

    #[test]
    fn display_includes_counts_bar_and_percentage() {
        let text = format_bar(
            "planner",
            "Loading direction patterns",
            50,
            200,
            Duration::from_secs(10),
        );
        assert!(text.contains("Loading direction patterns"));
        assert!(text.contains("50/200 (25%)"));
        assert!(text.contains("########------------------------"));
        assert!(text.contains("ETA 00:30"));
    }

    #[test]
    fn stage_is_accurate() {
        assert!(
            format_bar("gtfs2graph", "Stops", 2, 4, Duration::from_secs(2))
                .starts_with("[gtfs2graph] Stops ")
        );
    }

    #[test]
    fn unknown_eta_until_there_is_a_reliable_rate() {
        assert!(
            format_bar("planner", "Loading", 0, 100, Duration::from_secs(5)).contains("ETA --:--")
        );
        assert!(
            format_bar("planner", "Loading", 100, 100, Duration::from_secs(5)).contains("100%")
        );
        assert!(format_bar("planner", "Loading", 0, 0, Duration::ZERO).contains("0/0 (100%)"));
    }
}
