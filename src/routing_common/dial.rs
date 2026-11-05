//! Direct translation of Nigiri's common/dial.h at
//! 82e55cedfc184ea3a33082619ba937d172e9c575 (MIT; third_party/nigiri-LICENSE).
//! Bucket indices use the caller's duration unit; the timetable adapter uses seconds.

pub(crate) struct Dial<T, F> {
    get_bucket: F,
    current_bucket: usize,
    size: usize,
    buckets: Vec<Vec<T>>,
}

impl<T, F: Fn(&T) -> usize> Dial<T, F> {
    pub(crate) fn new(max_bucket: usize, get_bucket: F) -> Self {
        Self {
            get_bucket,
            current_bucket: 0,
            size: 0,
            buckets: (0..=max_bucket).map(|_| Vec::new()).collect(),
        }
    }

    pub(crate) fn push(&mut self, element: T) {
        let distance = (self.get_bucket)(&element);
        assert!(distance < self.buckets.len());
        self.buckets[distance].push(element);
        self.current_bucket = self.current_bucket.min(distance);
        self.size += 1;
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        if self.size == 0 {
            return None;
        }
        while self.buckets[self.current_bucket].is_empty() {
            self.current_bucket += 1;
        }
        let element = self.buckets[self.current_bucket].pop();
        self.size -= 1;
        element
    }

    pub(crate) fn n_buckets(&self) -> usize {
        self.buckets.len()
    }
}

#[derive(Default)]
pub(crate) struct DijkstraStats {
    pub settled: u64,
    pub edges: u64,
}

/// Nigiri routing/dijkstra.h's static graph traversal, translated in order.
/// RTT edges are absent from Catenary's static query model. Duration arithmetic
/// is widened before comparison, as C++ integer promotion does for u16 labels.
pub(crate) fn dijkstra<F: Fn(&(u32, usize)) -> usize>(
    offsets: &[usize],
    edges: &[(u32, u32)],
    queue: &mut Dial<(u32, usize), F>,
    distances: &mut [u32],
    max_distance: u32,
) -> DijkstraStats {
    let mut stats = DijkstraStats::default();
    while let Some((distance, stop)) = queue.pop() {
        if distances[stop] < distance {
            continue;
        }
        stats.settled += 1;
        for &(target, duration) in &edges[offsets[stop]..offsets[stop + 1]] {
            stats.edges += 1;
            let target = target as usize;
            let new_distance = u64::from(distance) + u64::from(duration);
            if new_distance < u64::from(distances[target])
                && new_distance < queue.n_buckets() as u64
                && new_distance < u64::from(max_distance)
            {
                distances[target] = new_distance as u32;
                queue.push((new_distance as u32, target));
            }
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_distances_are_lifo_and_a_smaller_key_rewinds_the_cursor() {
        let mut queue = Dial::new(100, |label: &(usize, char)| label.0);
        queue.push((90, 'a'));
        queue.push((10, 'b'));
        queue.push((10, 'c'));
        assert_eq!(queue.pop(), Some((10, 'c')));
        assert_eq!(queue.pop(), Some((10, 'b')));
        queue.push((0, 'd'));
        assert_eq!(queue.pop(), Some((0, 'd')));
        assert_eq!(queue.pop(), Some((90, 'a')));
        assert_eq!(queue.pop(), None);
        assert_eq!(queue.n_buckets(), 101);
    }
}
