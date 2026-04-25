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

//! Core metric types aligned with OpenTelemetry instruments.

use std::sync::Arc;

// ── Label type alias ─────────────────────────────────────────────────────────

/// A slice of `(label_name, label_value)` pairs attached to a metric.
pub type Labels<'a> = &'a [(&'a str, &'a str)];

// ── Internal backend interfaces (crate-private) ───────────────────────────────

pub(crate) mod iface {
    pub trait Counter: Send + Sync + 'static {
        fn add(&self, value: u64);
    }

    pub trait UpDownCounter: Send + Sync + 'static {
        fn add(&self, value: i64);
    }

    pub trait Gauge: Send + Sync + 'static {
        fn record(&self, value: f64);
    }

    pub trait Histogram: Send + Sync + 'static {
        fn record(&self, value: f64);
    }
}

// ── Public handle types ───────────────────────────────────────────────────────

/// Monotonically increasing counter.
#[derive(Clone)]
pub struct Counter(pub(crate) Arc<dyn iface::Counter>);

impl Counter {
    /// Add value.
    #[inline]
    pub fn add(&self, value: u64) {
        self.0.add(value);
    }

    /// Increment by 1.
    #[inline]
    pub fn inc(&self) {
        self.0.add(1);
    }
}

/// Additive counter that can go up and down.
#[derive(Clone)]
pub struct UpDownCounter(pub(crate) Arc<dyn iface::UpDownCounter>);

impl UpDownCounter {
    /// Add a signed delta (positive to increment, negative to decrement).
    #[inline]
    pub fn add(&self, value: i64) {
        self.0.add(value);
    }

    /// Increment by 1.
    #[inline]
    pub fn inc(&self) {
        self.0.add(1);
    }

    /// Decrement by 1.
    #[inline]
    pub fn dec(&self) {
        self.0.add(-1);
    }
}

/// Gauge that records the current value.
#[derive(Clone)]
pub struct Gauge(pub(crate) Arc<dyn iface::Gauge>);

impl Gauge {
    /// Record the current value of the measurement.
    #[inline]
    pub fn record(&self, value: f64) {
        self.0.record(value);
    }
}

/// Histogram for value distributions.
#[derive(Clone)]
pub struct Histogram(pub(crate) Arc<dyn iface::Histogram>);

impl Histogram {
    /// Record one observation.
    #[inline]
    pub fn record(&self, value: f64) {
        self.0.record(value);
    }
}

// ── Backend factory trait ─────────────────────────────────────────────────────

/// Backend factory for metric handles.
pub trait MetricsBackend: Send + Sync + 'static {
    /// Create (or look up) a [`Counter`].
    fn create_counter(&self, name: &str, help: &str, labels: Labels<'_>) -> Counter;

    /// Create (or look up) an [`UpDownCounter`].
    fn create_up_down_counter(&self, name: &str, help: &str, labels: Labels<'_>) -> UpDownCounter;

    /// Create (or look up) a [`Gauge`].
    fn create_gauge(&self, name: &str, help: &str, labels: Labels<'_>) -> Gauge;

    /// Create (or look up) a [`Histogram`].
    ///
    /// `buckets` are the upper-bound values for each histogram bucket.
    /// Pass `None` to use the backend's default bucket boundaries.
    fn create_histogram(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
        buckets: Option<&[f64]>,
    ) -> Histogram;

    /// Remove the metric identified by `name` from the backend so it no
    /// longer appears in exports.
    fn remove(&self, name: &str);

    /// Collect the current metrics snapshot (e.g. Prometheus text format).
    fn gather(&self) -> String;
}
