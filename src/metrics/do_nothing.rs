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

//! Do-nothing metrics backend (default).
//!
//! [`DoNothingBackend`] is selected when `metrics.backend` is omitted from
//! `config.yaml` or set to `do_nothing`.  All operations are zero-cost no-ops.

use std::sync::Arc;

use crate::metrics::core::{
    Counter, Gauge, Histogram, Labels, MetricsBackend, UpDownCounter,
};
use crate::metrics::core::iface;

// ── Internal do-nothing implementations ──────────────────────────────────────

struct DoNothingCounterBackend;
impl iface::Counter for DoNothingCounterBackend {
    #[inline]
    fn add(&self, _: u64) {}
}

struct DoNothingUpDownCounterBackend;
impl iface::UpDownCounter for DoNothingUpDownCounterBackend {
    #[inline]
    fn add(&self, _: i64) {}
}

struct DoNothingGaugeBackend;
impl iface::Gauge for DoNothingGaugeBackend {
    #[inline]
    fn record(&self, _: f64) {}
}

struct DoNothingHistogramBackend;
impl iface::Histogram for DoNothingHistogramBackend {
    #[inline]
    fn record(&self, _: f64) {}
}

// ── DoNothingBackend ──────────────────────────────────────────────────────────

/// A [`MetricsBackend`] that discards all metric data with zero overhead.
#[derive(Debug, Clone, Default)]
pub struct DoNothingBackend;

impl MetricsBackend for DoNothingBackend {
    fn create_counter(&self, _: &str, _: &str, _: Labels<'_>) -> Counter {
        Counter(Arc::new(DoNothingCounterBackend))
    }

    fn create_up_down_counter(&self, _: &str, _: &str, _: Labels<'_>) -> UpDownCounter {
        UpDownCounter(Arc::new(DoNothingUpDownCounterBackend))
    }

    fn create_gauge(&self, _: &str, _: &str, _: Labels<'_>) -> Gauge {
        Gauge(Arc::new(DoNothingGaugeBackend))
    }

    fn create_histogram(&self, _: &str, _: &str, _: Labels<'_>, _: Option<&[f64]>) -> Histogram {
        Histogram(Arc::new(DoNothingHistogramBackend))
    }

    fn remove(&self, _: &str) {}

    fn gather(&self) -> String {
        String::new()
    }
}
