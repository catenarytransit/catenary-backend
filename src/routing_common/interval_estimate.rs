//! Nigiri's interval_estimator search-window rules, adapted to Unix seconds.
//! Source: motis-project/nigiri/include/nigiri/routing/interval_estimate.h
//! Unlike Nigiri's timetable-level clamps, Edelweiss applies an explicit
//! request-relative maximum window (and the enumerator clamps to real service days).
//! Keep time interval boundaries inclusive for the compressed event enumerator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Interval {
    pub from: i64,
    pub to: i64,
}

impl Interval {
    pub fn size(self) -> i64 {
        self.to.saturating_sub(self.from)
    }
}

pub(crate) struct IntervalEstimator {
    reference: i64,
    max_window_seconds: u32,
    extend_earlier: bool,
    extend_later: bool,
    forward: bool,
    min_connection_count: usize,
}

impl IntervalEstimator {
    pub fn new(
        reference: i64,
        max_window_seconds: u32,
        min_connection_count: usize,
        arrive_by: bool,
    ) -> Self {
        Self {
            reference,
            max_window_seconds,
            extend_earlier: arrive_by,
            extend_later: !arrive_by,
            forward: !arrive_by,
            min_connection_count,
        }
    }

    fn bounds(&self) -> Interval {
        let max = i64::from(self.max_window_seconds);
        Interval {
            from: if self.extend_earlier {
                self.reference.saturating_sub(max)
            } else {
                self.reference
            },
            to: if self.extend_later {
                self.reference.saturating_add(max)
            } else {
                self.reference
            },
        }
    }

    fn clamp(&self, interval: Interval) -> Interval {
        let bounds = self.bounds();
        Interval {
            from: interval.from.clamp(bounds.from, bounds.to),
            to: interval.to.clamp(bounds.from, bounds.to),
        }
    }

    fn can_extend_earlier(&self, itv: Interval) -> bool {
        self.extend_earlier && itv.from > self.bounds().from
    }

    fn can_extend_later(&self, itv: Interval) -> bool {
        self.extend_later && itv.to < self.bounds().to
    }

    fn can_extend_bad_dir(&self, itv: Interval) -> bool {
        if self.forward {
            self.can_extend_later(itv)
        } else {
            self.can_extend_earlier(itv)
        }
    }

    /// Port of nigiri::routing::interval_estimator::initial.
    pub fn initial(&self, itv: Interval) -> Interval {
        if self.min_connection_count == 0
            || (!self.extend_earlier && !self.extend_later)
        {
            return itv;
        }
        let mut next = itv;
        let ext = 3_600_i64.saturating_mul(self.min_connection_count as i64);
        if self.can_extend_bad_dir(itv) {
            if self.forward {
                if self.can_extend_earlier(itv) {
                    next.from = next.from.saturating_sub(3_600);
                } else {
                    next.to = next.to.saturating_add(3_600);
                }
                next.to = next.to.saturating_add(ext);
            } else {
                if self.can_extend_later(itv) {
                    next.to = next.to.saturating_add(3_600);
                } else {
                    next.from = next.from.saturating_sub(3_600);
                }
                next.from = next.from.saturating_sub(ext);
            }
        } else if self.forward {
            if self.extend_earlier {
                next.from = next.from.saturating_sub(3_600_i64.saturating_add(ext));
            }
        } else if self.extend_later {
            next.to = next.to.saturating_add(3_600_i64.saturating_add(ext));
        }
        self.clamp(next)
    }

    /// Port of nigiri::routing::interval_estimator::extension:
    /// expand the existing interval by its width times the remaining result count.
    pub fn extension(&self, itv: Interval, missing_connections: usize) -> Interval {
        if missing_connections == 0
            || (!self.extend_earlier && !self.extend_later)
        {
            return itv;
        }
        let ext = itv.size().saturating_mul(missing_connections as i64);
        let mut next = itv;
        if self.can_extend_earlier(itv) && self.can_extend_later(itv) {
            next.from = next.from.saturating_sub(ext / 2);
            next.to = next.to.saturating_add(ext / 2);
        } else {
            if self.extend_earlier {
                next.from = next.from.saturating_sub(ext);
            }
            if self.extend_later {
                next.to = next.to.saturating_add(ext);
            }
        }
        self.clamp(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nigiri_forward_initial_and_extension() {
        let e = IntervalEstimator::new(1_000_000, 48 * 3_600, 5, false);
        let initial = e.initial(Interval { from: 1_000_000, to: 1_000_000 });
        assert_eq!(initial.size(), 6 * 3_600);
        assert_eq!(initial.from, 1_000_000);
        let extended = e.extension(initial, 2);
        assert_eq!(extended.to, 1_000_000 + 18 * 3_600);
        assert_eq!(e.extension(extended, 0), extended);
    }

    #[test]
    fn nigiri_reverse_initial_and_clamped_extension() {
        let e = IntervalEstimator::new(1_000_000, 12 * 3_600, 5, true);
        let initial = e.initial(Interval { from: 1_000_000, to: 1_000_000 });
        assert_eq!(initial.from, 1_000_000 - 6 * 3_600);
        assert_eq!(initial.to, 1_000_000);
        let extended = e.extension(initial, 2);
        assert_eq!(extended.from, 1_000_000 - 12 * 3_600);
        assert_eq!(e.extension(extended, 1), extended);
    }

    #[test]
    fn small_max_window_clamps_initial() {
        let e = IntervalEstimator::new(0, 900, 5, false);
        assert_eq!(e.initial(Interval { from: 0, to: 0 }).to, 900);
    }
}
