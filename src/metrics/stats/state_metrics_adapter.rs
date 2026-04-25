// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bridge from state-engine metrics to global metrics manager.

use crate::metrics::core::{Counter, Gauge, Histogram};
use crate::metrics::manager::MetricsManager;
use crate::metrics::registry::{MetricHandle, MetricsRegistry};
use crate::runtime::streaming::state::StateMetricsCollector;

const SUBSYSTEM: &str = "fs_state";

/// State engine collector implementation backed by the global registry.
pub struct StateMetricsBridge {
    _handle_memory: MetricHandle,
    _handle_spill: MetricHandle,
    _handle_compact: MetricHandle,
    _handle_io_errors: MetricHandle,

    memory_usage: Gauge,
    spill_duration_ms: Histogram,
    compaction_duration_ms: Histogram,
    io_errors: Counter,

    job_id: String,
}

impl StateMetricsBridge {
    /// Register state metrics for `job_id`.
    pub fn try_new(job_id: &str) -> Option<Self> {
        let m = MetricsManager::try_global()?;
        Some(Self::new_with_registry(job_id, m.as_ref()))
    }

    pub fn new_with_registry(job_id: &str, registry: &MetricsRegistry) -> Self {
        let labels: &[(&str, &str)] = &[("job_id", job_id)];
        let key = sanitise(job_id);

        let (memory_usage, _handle_memory) = registry
            .register_gauge(
                &format!("{SUBSYSTEM}_memory_bytes_{key}"),
                "Current in-memory state store usage in bytes (non-additive snapshot)",
                labels,
            )
            .unwrap_or_else(|e| {
                tracing::warn!(job_id, error = %e, "register memory_bytes gauge failed");
                fallback_gauge()
            });

        let (spill_duration_ms, _handle_spill) = registry
            .register_histogram(
                &format!("{SUBSYSTEM}_spill_duration_ms_{key}"),
                "State spill-to-disk latency in milliseconds",
                labels,
                None,
            )
            .unwrap_or_else(|e| {
                tracing::warn!(job_id, error = %e, "register spill_duration histogram failed");
                fallback_histogram()
            });

        let (compaction_duration_ms, _handle_compact) = registry
            .register_histogram(
                &format!("{SUBSYSTEM}_compaction_duration_ms_{key}"),
                "State compaction latency in milliseconds",
                labels,
                None,
            )
            .unwrap_or_else(|e| {
                tracing::warn!(job_id, error = %e, "register compaction_duration histogram failed");
                fallback_histogram()
            });

        let (io_errors, _handle_io_errors) = registry
            .register_counter(
                &format!("{SUBSYSTEM}_io_errors_total_{key}"),
                "Total number of state I/O errors (monotonically increasing)",
                labels,
            )
            .unwrap_or_else(|e| {
                tracing::warn!(job_id, error = %e, "register io_errors counter failed");
                fallback_counter()
            });

        Self {
            _handle_memory,
            _handle_spill,
            _handle_compact,
            _handle_io_errors,
            memory_usage,
            spill_duration_ms,
            compaction_duration_ms,
            io_errors,
            job_id: job_id.to_string(),
        }
    }
}

impl Drop for StateMetricsBridge {
    fn drop(&mut self) {
        if let Some(m) = MetricsManager::try_global() {
            m.unregister_handle(&self._handle_memory);
            m.unregister_handle(&self._handle_spill);
            m.unregister_handle(&self._handle_compact);
            m.unregister_handle(&self._handle_io_errors);
        }
        tracing::debug!(job_id = %self.job_id, "StateMetricsBridge dropped; metrics unregistered");
    }
}

impl StateMetricsCollector for StateMetricsBridge {
    fn record_memory_usage(&self, _operator_id: u32, bytes: u64) {
        self.memory_usage.record(bytes as f64);
    }

    fn record_spill_duration(&self, _operator_id: u32, duration_ms: u128) {
        self.spill_duration_ms.record(duration_ms as f64);
    }

    fn record_compaction_duration(&self, _operator_id: u32, _is_major: bool, duration_ms: u128) {
        self.compaction_duration_ms.record(duration_ms as f64);
    }

    fn inc_io_errors(&self, _operator_id: u32) {
        self.io_errors.inc();
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn sanitise(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn fallback_counter() -> (Counter, MetricHandle) {
    use crate::metrics::core::MetricsBackend;
    use crate::metrics::do_nothing::DoNothingBackend;
    let c = DoNothingBackend.create_counter("", "", &[]);
    (
        c,
        MetricHandle {
            name: std::sync::Arc::from("__do_nothing__"),
        },
    )
}

fn fallback_gauge() -> (Gauge, MetricHandle) {
    use crate::metrics::core::MetricsBackend;
    use crate::metrics::do_nothing::DoNothingBackend;
    let g = DoNothingBackend.create_gauge("", "", &[]);
    (
        g,
        MetricHandle {
            name: std::sync::Arc::from("__do_nothing__"),
        },
    )
}

fn fallback_histogram() -> (Histogram, MetricHandle) {
    use crate::metrics::core::MetricsBackend;
    use crate::metrics::do_nothing::DoNothingBackend;
    let h = DoNothingBackend.create_histogram("", "", &[], None);
    (
        h,
        MetricHandle {
            name: std::sync::Arc::from("__do_nothing__"),
        },
    )
}
