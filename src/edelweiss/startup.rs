/// Run independent startup loaders concurrently and wait for both outputs.
/// Keep Result values intact so neither error bypasses the other loader's join.
pub(super) fn load_concurrently<A, B: Send>(
    first: impl FnOnce() -> A,
    second: impl FnOnce() -> B + Send,
) -> (A, B) {
    std::thread::scope(|scope| {
        let second = scope.spawn(second);
        let first = first();
        let second = second
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        (first, second)
    })
}

#[cfg(test)]
mod tests {
    use super::load_concurrently;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn loaders_overlap_and_both_outputs_are_returned() {
        let (first_started, wait_for_first) = mpsc::channel();
        let (second_started, wait_for_second) = mpsc::channel();
        // A sequential implementation times out instead of hanging the test.
        let outputs = load_concurrently(
            || {
                first_started.send(()).unwrap();
                wait_for_second
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                "timetable"
            },
            move || {
                second_started.send(()).unwrap();
                wait_for_first.recv_timeout(Duration::from_secs(5)).unwrap();
                "OSR"
            },
        );
        assert_eq!(outputs, ("timetable", "OSR"));
    }

    #[test]
    fn errors_preserve_both_results_and_wait_for_the_other_loader() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for fail_first in [true, false] {
            let finished = AtomicBool::new(false);
            let (first, second) = load_concurrently(
                || if fail_first { Err("timetable") } else { Ok(()) },
                || {
                    finished.store(true, Ordering::SeqCst);
                    if fail_first { Ok(()) } else { Err("OSR") }
                },
            );
            assert!(finished.load(Ordering::SeqCst));
            assert_eq!(first, if fail_first { Err("timetable") } else { Ok(()) });
            assert_eq!(second, if fail_first { Ok(()) } else { Err("OSR") });
        }
    }

    #[test]
    fn loader_panics_are_propagated() {
        for panic_first in [true, false] {
            let result = std::panic::catch_unwind(|| {
                load_concurrently(
                    || assert!(!panic_first, "timetable loader panicked"),
                    || assert!(panic_first, "OSR loader panicked"),
                )
            });
            assert!(result.is_err());
        }
    }
}
