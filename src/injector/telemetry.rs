//! Real wall-clock performance telemetry and package accounting.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Real metrics tracked during live anticipatory context injection.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InjectorTelemetrySnapshot {
    pub task_duration_ms: u64,
    pub total_retrieval_wait_ms: u64,
    pub packages_consumed: usize,
    pub packages_ignored: usize,
    pub tool_calls: usize,
    pub failed_compile_test_iterations: usize,
}

/// Thread-safe accumulator for real injector performance metrics.
#[derive(Debug, Default)]
pub struct InjectorTelemetry {
    task_start_ms: AtomicU64,
    task_completion_ms: AtomicU64,
    retrieval_wait_ms: AtomicU64,
    packages_consumed: AtomicUsize,
    packages_ignored: AtomicUsize,
    tool_calls: AtomicUsize,
    failed_compile_test_iterations: AtomicUsize,
}

impl InjectorTelemetry {
    pub fn new() -> Arc<Self> {
        let telem = Arc::new(Self::default());
        telem
            .task_start_ms
            .store(crate::types::now_ms(), Ordering::Relaxed);
        telem
    }

    pub fn record_retrieval_wait(&self, duration_ms: u64) {
        self.retrieval_wait_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub fn record_package_consumed(&self) {
        self.packages_consumed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_package_ignored(&self) {
        self.packages_ignored.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tool_call(&self) {
        self.tool_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed_iteration(&self) {
        self.failed_compile_test_iterations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn complete_task(&self) {
        self.task_completion_ms
            .store(crate::types::now_ms(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> InjectorTelemetrySnapshot {
        let start = self.task_start_ms.load(Ordering::Relaxed);
        let end = self.task_completion_ms.load(Ordering::Relaxed);
        let task_duration_ms = if end >= start && end > 0 {
            end - start
        } else if start > 0 {
            crate::types::now_ms().saturating_sub(start)
        } else {
            0
        };

        InjectorTelemetrySnapshot {
            task_duration_ms,
            total_retrieval_wait_ms: self.retrieval_wait_ms.load(Ordering::Relaxed),
            packages_consumed: self.packages_consumed.load(Ordering::Relaxed),
            packages_ignored: self.packages_ignored.load(Ordering::Relaxed),
            tool_calls: self.tool_calls.load(Ordering::Relaxed),
            failed_compile_test_iterations: self
                .failed_compile_test_iterations
                .load(Ordering::Relaxed),
        }
    }
}
