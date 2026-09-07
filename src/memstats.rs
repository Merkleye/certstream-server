//! Allocator-level heap statistics.
//!
//! RSS on its own cannot separate "the process is holding this data" from
//! "jemalloc has not handed these pages back to the OS yet". `allocated`
//! answers the first question, `resident` the second, and the gap between
//! them is what tells an operator whether to shrink a cache or to retune the
//! allocator. Both are exported so that question is answerable from
//! `/metrics` instead of from a heap profiler attached after the fact.

/// Live heap figures from the allocator, in bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct HeapUsage {
    /// Bytes the application currently holds.
    pub allocated: u64,
    /// Bytes in physically resident pages mapped by the allocator. Tracks
    /// process RSS closely, minus stacks and non-allocator mappings.
    pub resident: u64,
}

#[cfg(not(target_env = "msvc"))]
pub fn record() -> Option<HeapUsage> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // jemalloc caches its counters and only refreshes them when the epoch is
    // advanced. Skipping this makes every read return the values from process
    // start, which looks like a perfectly flat heap.
    if let Err(e) = epoch::advance() {
        tracing::debug!(error = %e, "jemalloc epoch advance failed");
        return None;
    }

    let mut usage = HeapUsage::default();

    if let Ok(v) = stats::allocated::read() {
        usage.allocated = v as u64;
        metrics::gauge!("certstream_jemalloc_allocated_bytes").set(v as f64);
    }
    if let Ok(v) = stats::resident::read() {
        usage.resident = v as u64;
        metrics::gauge!("certstream_jemalloc_resident_bytes").set(v as f64);
    }
    // active - allocated is per-object rounding waste; mapped - resident and
    // retained together describe address space jemalloc holds but does not
    // pay for in physical memory.
    if let Ok(v) = stats::active::read() {
        metrics::gauge!("certstream_jemalloc_active_bytes").set(v as f64);
    }
    if let Ok(v) = stats::mapped::read() {
        metrics::gauge!("certstream_jemalloc_mapped_bytes").set(v as f64);
    }
    if let Ok(v) = stats::retained::read() {
        metrics::gauge!("certstream_jemalloc_retained_bytes").set(v as f64);
    }
    if let Ok(v) = stats::metadata::read() {
        metrics::gauge!("certstream_jemalloc_metadata_bytes").set(v as f64);
    }

    Some(usage)
}

#[cfg(target_env = "msvc")]
pub fn record() -> Option<HeapUsage> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_heap_usage_is_zeroed() {
        let usage = HeapUsage::default();
        assert_eq!(usage.allocated, 0);
        assert_eq!(usage.resident, 0);
    }

    #[test]
    fn heap_usage_is_copy_and_debuggable() {
        let usage = HeapUsage {
            allocated: 42,
            resident: 100,
        };
        let copied = usage;
        assert_eq!(copied.allocated, 42);
        assert_eq!(copied.resident, 100);
        assert!(format!("{usage:?}").contains("42"));
    }

    // record() calls into jemalloc's mallctl interface directly (epoch,
    // stats::*), independent of whether jemalloc is the process's
    // #[global_allocator] -- that's only wired up in the `main` binary, not
    // this lib target, so `cargo test --lib` exercises the same jemalloc
    // control-plane calls production does, just against a process whose
    // actual Rust allocations go through the system allocator instead.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn record_reads_jemalloc_stats() {
        let usage = record().expect("jemalloc stats should be readable");
        // jemalloc's own bookkeeping allocates something to track itself, so
        // this should never be a bare zero in practice; the real assertion
        // is that record() didn't return None (an epoch/stats read error).
        let _ = usage.allocated;
        let _ = usage.resident;
    }

    #[cfg(target_env = "msvc")]
    #[test]
    fn record_is_none_on_msvc() {
        assert!(record().is_none());
    }
}
