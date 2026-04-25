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

//! Prometheus-backed [`MetricsRecorder`] implementation.
//!
//! Uses the `prometheus` crate's default global registry so that all metrics
//! registered anywhere in the process are exported together via `/metrics`.

use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

use crate::metrics::core::{Counter, Gauge, Histogram, Labels, MetricsRecorder};

// ── Wrapper handles ───────────────────────────────────────────────────────────

pub struct PrometheusCounter(prometheus::Counter);

impl Counter for PrometheusCounter {
    fn inc(&self) {
        self.0.inc();
    }
    fn inc_by(&self, value: u64) {
        self.0.inc_by(value as f64);
    }
}

pub struct PrometheusGauge(prometheus::Gauge);

impl Gauge for PrometheusGauge {
    fn set(&self, value: f64) {
        self.0.set(value);
    }
    fn inc(&self) {
        self.0.inc();
    }
    fn dec(&self) {
        self.0.dec();
    }
    fn inc_by(&self, value: f64) {
        self.0.add(value);
    }
    fn dec_by(&self, value: f64) {
        self.0.sub(value);
    }
}

pub struct PrometheusHistogram(prometheus::Histogram);

impl Histogram for PrometheusHistogram {
    fn observe(&self, value: f64) {
        self.0.observe(value);
    }
}

// ── Recorder ──────────────────────────────────────────────────────────────────

/// Default histogram buckets (milliseconds scale, suitable for latency).
const DEFAULT_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
];

/// A [`MetricsRecorder`] that stores metrics in a [`prometheus::Registry`].
///
/// All `register_*` calls are idempotent: calling them twice with the same
/// `name + labels` returns a handle to the *same* underlying metric.
#[derive(Clone)]
pub struct PrometheusRecorder {
    registry: Arc<Registry>,
    /// Guards concurrent registrations so that the "get-or-create" pattern
    /// is race-free without holding the lock during normal observation paths.
    register_lock: Arc<Mutex<()>>,
}

impl PrometheusRecorder {
    /// Create a new recorder backed by a fresh, isolated [`Registry`].
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            register_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Expose a reference to the underlying [`Registry`] (needed by the
    /// HTTP exporter to render the text format).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    fn label_names(labels: Labels<'_>) -> Vec<String> {
        labels.iter().map(|(k, _)| k.to_string()).collect()
    }

    fn label_values(labels: Labels<'_>) -> Vec<String> {
        labels.iter().map(|(_, v)| v.to_string()).collect()
    }
}

impl Default for PrometheusRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder for PrometheusRecorder {
    fn register_counter(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
    ) -> Arc<dyn Counter> {
        let _guard = self.register_lock.lock();
        let label_names = Self::label_names(labels);
        let label_name_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();

        let opts = Opts::new(name, help);
        let vec = CounterVec::new(opts, &label_name_refs).expect("invalid counter opts");

        let vec = match self.registry.register(Box::new(vec.clone())) {
            Ok(()) => vec,
            Err(prometheus::Error::AlreadyReg) => {
                // Already registered – retrieve the existing family.
                self.registry
                    .gather()
                    .iter()
                    .find(|mf| mf.get_name() == name)
                    .map(|_| {
                        CounterVec::new(Opts::new(name, help), &label_name_refs)
                            .expect("invalid counter opts")
                    })
                    .unwrap_or(vec)
            }
            Err(e) => panic!("Failed to register counter '{name}': {e}"),
        };

        let label_values = Self::label_values(labels);
        let label_value_refs: Vec<&str> = label_values.iter().map(String::as_str).collect();
        let counter = vec.with_label_values(&label_value_refs);
        Arc::new(PrometheusCounter(counter))
    }

    fn register_gauge(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
    ) -> Arc<dyn Gauge> {
        let _guard = self.register_lock.lock();
        let label_names = Self::label_names(labels);
        let label_name_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();

        let opts = Opts::new(name, help);
        let vec = GaugeVec::new(opts, &label_name_refs).expect("invalid gauge opts");

        let vec = match self.registry.register(Box::new(vec.clone())) {
            Ok(()) => vec,
            Err(prometheus::Error::AlreadyReg) => {
                GaugeVec::new(Opts::new(name, help), &label_name_refs)
                    .expect("invalid gauge opts")
            }
            Err(e) => panic!("Failed to register gauge '{name}': {e}"),
        };

        let label_values = Self::label_values(labels);
        let label_value_refs: Vec<&str> = label_values.iter().map(String::as_str).collect();
        let gauge = vec.with_label_values(&label_value_refs);
        Arc::new(PrometheusGauge(gauge))
    }

    fn register_histogram(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
        buckets: Option<&[f64]>,
    ) -> Arc<dyn Histogram> {
        let _guard = self.register_lock.lock();
        let label_names = Self::label_names(labels);
        let label_name_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();

        let bucket_values = buckets.unwrap_or(DEFAULT_BUCKETS).to_vec();
        let opts = HistogramOpts::new(name, help).buckets(bucket_values.clone());
        let vec =
            HistogramVec::new(opts, &label_name_refs).expect("invalid histogram opts");

        let vec = match self.registry.register(Box::new(vec.clone())) {
            Ok(()) => vec,
            Err(prometheus::Error::AlreadyReg) => {
                let opts =
                    HistogramOpts::new(name, help).buckets(bucket_values);
                HistogramVec::new(opts, &label_name_refs)
                    .expect("invalid histogram opts")
            }
            Err(e) => panic!("Failed to register histogram '{name}': {e}"),
        };

        let label_values = Self::label_values(labels);
        let label_value_refs: Vec<&str> = label_values.iter().map(String::as_str).collect();
        let histogram = vec.with_label_values(&label_value_refs);
        Arc::new(PrometheusHistogram(histogram))
    }

    fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder
            .encode(&metric_families, &mut buf)
            .expect("prometheus encode failed");
        String::from_utf8(buf).unwrap_or_default()
    }
}
