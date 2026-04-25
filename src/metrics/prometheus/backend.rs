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

//! Prometheus backend implementation.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
    core::Collector,
};

use crate::metrics::core::{
    Counter, Gauge, Histogram, Labels, MetricsBackend, UpDownCounter,
};
use crate::metrics::core::iface;

// ── Default histogram bucket boundaries ──────────────────────────────────────

/// Default histogram buckets.
const DEFAULT_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
];

// ── Internal Prometheus backend types ────────────────────────────────────────

struct PrometheusCounterBackend(prometheus::Counter);
impl iface::Counter for PrometheusCounterBackend {
    fn add(&self, v: u64) {
        self.0.inc_by(v as f64);
    }
}

/// UpDownCounter maps to Prometheus gauge.
struct PrometheusUpDownCounterBackend(prometheus::Gauge);
impl iface::UpDownCounter for PrometheusUpDownCounterBackend {
    fn add(&self, v: i64) {
        self.0.add(v as f64);
    }
}

struct PrometheusGaugeBackend(prometheus::Gauge);
impl iface::Gauge for PrometheusGaugeBackend {
    fn record(&self, v: f64) {
        self.0.set(v);
    }
}

struct PrometheusHistogramBackend(prometheus::Histogram);
impl iface::Histogram for PrometheusHistogramBackend {
    fn record(&self, v: f64) {
        self.0.observe(v);
    }
}

// ── Stored collector entries (needed for unregister) ──────────────────────────

enum CollectorEntry {
    Counter(CounterVec),
    UpDownCounter(GaugeVec),
    Gauge(GaugeVec),
    Histogram(HistogramVec),
}

// ── PrometheusBackend ─────────────────────────────────────────────────────────

/// Metrics backend backed by a private Prometheus registry.
#[derive(Clone)]
pub struct PrometheusBackend {
    inner: Arc<PrometheusBackendInner>,
}

struct PrometheusBackendInner {
    registry: Registry,
    collectors: Mutex<HashMap<String, CollectorEntry>>,
}

impl PrometheusBackend {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PrometheusBackendInner {
                registry: Registry::new(),
                collectors: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn label_names(labels: Labels<'_>) -> Vec<String> {
        labels.iter().map(|(k, _)| k.to_string()).collect()
    }

    fn label_values(labels: Labels<'_>) -> Vec<String> {
        labels.iter().map(|(_, v)| v.to_string()).collect()
    }

    fn register<C: Collector + Clone + 'static>(&self, collector: C) {
        self.inner
            .registry
            .register(Box::new(collector))
            .expect("failed to register collector with Prometheus registry");
    }
}

impl Default for PrometheusBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsBackend for PrometheusBackend {
    // ── Counter ───────────────────────────────────────────────────────────────

    fn create_counter(&self, name: &str, help: &str, labels: Labels<'_>) -> Counter {
        let mut c = self.inner.collectors.lock();
        if let Some(CollectorEntry::Counter(v)) = c.get(name) {
            let vals = Self::label_values(labels);
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            return Counter(Arc::new(PrometheusCounterBackend(v.with_label_values(&refs))));
        }
        let names = Self::label_names(labels);
        let nrefs: Vec<&str> = names.iter().map(String::as_str).collect();
        let vec = CounterVec::new(Opts::new(name, help), &nrefs).expect("invalid counter opts");
        self.register(vec.clone());
        let vals = Self::label_values(labels);
        let vrefs: Vec<&str> = vals.iter().map(String::as_str).collect();
        let h = vec.with_label_values(&vrefs);
        c.insert(name.to_string(), CollectorEntry::Counter(vec));
        Counter(Arc::new(PrometheusCounterBackend(h)))
    }

    // ── UpDownCounter ─────────────────────────────────────────────────────────

    fn create_up_down_counter(&self, name: &str, help: &str, labels: Labels<'_>) -> UpDownCounter {
        let mut c = self.inner.collectors.lock();
        if let Some(CollectorEntry::UpDownCounter(v)) = c.get(name) {
            let vals = Self::label_values(labels);
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            return UpDownCounter(Arc::new(PrometheusUpDownCounterBackend(
                v.with_label_values(&refs),
            )));
        }
        let names = Self::label_names(labels);
        let nrefs: Vec<&str> = names.iter().map(String::as_str).collect();
        let vec =
            GaugeVec::new(Opts::new(name, help), &nrefs).expect("invalid up_down_counter opts");
        self.register(vec.clone());
        let vals = Self::label_values(labels);
        let vrefs: Vec<&str> = vals.iter().map(String::as_str).collect();
        let h = vec.with_label_values(&vrefs);
        c.insert(name.to_string(), CollectorEntry::UpDownCounter(vec));
        UpDownCounter(Arc::new(PrometheusUpDownCounterBackend(h)))
    }

    // ── Gauge ─────────────────────────────────────────────────────────────────

    fn create_gauge(&self, name: &str, help: &str, labels: Labels<'_>) -> Gauge {
        let mut c = self.inner.collectors.lock();
        if let Some(CollectorEntry::Gauge(v)) = c.get(name) {
            let vals = Self::label_values(labels);
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            return Gauge(Arc::new(PrometheusGaugeBackend(v.with_label_values(&refs))));
        }
        let names = Self::label_names(labels);
        let nrefs: Vec<&str> = names.iter().map(String::as_str).collect();
        let vec = GaugeVec::new(Opts::new(name, help), &nrefs).expect("invalid gauge opts");
        self.register(vec.clone());
        let vals = Self::label_values(labels);
        let vrefs: Vec<&str> = vals.iter().map(String::as_str).collect();
        let h = vec.with_label_values(&vrefs);
        c.insert(name.to_string(), CollectorEntry::Gauge(vec));
        Gauge(Arc::new(PrometheusGaugeBackend(h)))
    }

    // ── Histogram ─────────────────────────────────────────────────────────────

    fn create_histogram(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
        buckets: Option<&[f64]>,
    ) -> Histogram {
        let mut c = self.inner.collectors.lock();
        if let Some(CollectorEntry::Histogram(v)) = c.get(name) {
            let vals = Self::label_values(labels);
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            return Histogram(Arc::new(PrometheusHistogramBackend(
                v.with_label_values(&refs),
            )));
        }
        let names = Self::label_names(labels);
        let nrefs: Vec<&str> = names.iter().map(String::as_str).collect();
        let bkts = buckets.unwrap_or(DEFAULT_BUCKETS).to_vec();
        let opts = HistogramOpts::new(name, help).buckets(bkts);
        let vec = HistogramVec::new(opts, &nrefs).expect("invalid histogram opts");
        self.register(vec.clone());
        let vals = Self::label_values(labels);
        let vrefs: Vec<&str> = vals.iter().map(String::as_str).collect();
        let h = vec.with_label_values(&vrefs);
        c.insert(name.to_string(), CollectorEntry::Histogram(vec));
        Histogram(Arc::new(PrometheusHistogramBackend(h)))
    }

    // ── Remove ────────────────────────────────────────────────────────────────

    fn remove(&self, name: &str) {
        let mut c = self.inner.collectors.lock();
        if let Some(entry) = c.remove(name) {
            let boxed: Box<dyn Collector> = match entry {
                CollectorEntry::Counter(v) => Box::new(v),
                CollectorEntry::UpDownCounter(v) => Box::new(v),
                CollectorEntry::Gauge(v) => Box::new(v),
                CollectorEntry::Histogram(v) => Box::new(v),
            };
            if let Err(e) = self.inner.registry.unregister(boxed) {
                tracing::warn!(
                    metric = name, error = %e,
                    "Failed to unregister metric from Prometheus registry"
                );
            }
        } else {
            tracing::warn!(metric = name, "PrometheusBackend::remove: metric not found");
        }
    }

    // ── Gather ────────────────────────────────────────────────────────────────

    fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let families = self.inner.registry.gather();
        let mut buf = Vec::with_capacity(4096);
        encoder
            .encode(&families, &mut buf)
            .expect("Prometheus text encode failed");
        String::from_utf8(buf).unwrap_or_default()
    }
}
