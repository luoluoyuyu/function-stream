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

//! Global metrics registry.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use anyhow::{Result, anyhow, bail};
use parking_lot::RwLock;
use tracing::warn;

use crate::metrics::core::{Counter, Gauge, Histogram, Labels, MetricsBackend, UpDownCounter};

// ── MetricHandle ──────────────────────────────────────────────────────────────

/// Opaque handle returned when a metric is registered.
#[derive(Clone)]
pub struct MetricHandle {
    pub(crate) name: Arc<str>,
}

impl MetricHandle {
    fn new(name: &str) -> Self {
        Self {
            name: Arc::from(name),
        }
    }

    /// The metric name this handle was issued for.
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ── Internal bookkeeping ───────────────────────────────────────────────────────

#[derive(Clone)]
enum MetricEntry {
    Counter(Counter),
    UpDownCounter(UpDownCounter),
    Gauge(Gauge),
    Histogram(Histogram),
}

impl MetricEntry {
    fn kind_str(&self) -> &'static str {
        match self {
            MetricEntry::Counter(_) => "counter",
            MetricEntry::UpDownCounter(_) => "up_down_counter",
            MetricEntry::Gauge(_) => "gauge",
            MetricEntry::Histogram(_) => "histogram",
        }
    }
}

// ── MetricsRegistry ───────────────────────────────────────────────────────────

static GLOBAL_REGISTRY: OnceLock<Arc<MetricsRegistry>> = OnceLock::new();

/// The central metrics registry.
pub struct MetricsRegistry {
    backend: Arc<dyn MetricsBackend>,
    entries: RwLock<HashMap<String, MetricEntry>>,
}

impl MetricsRegistry {
    // ── Construction ──────────────────────────────────────────────────────────

    pub fn new(backend: Arc<dyn MetricsBackend>) -> Self {
        Self {
            backend,
            entries: RwLock::new(HashMap::new()),
        }
    }

    // ── Global singleton ──────────────────────────────────────────────────────

    /// Initialise the process-wide registry (call once at startup).
    pub fn init(backend: Arc<dyn MetricsBackend>) -> Result<()> {
        GLOBAL_REGISTRY
            .set(Arc::new(Self::new(backend)))
            .map_err(|_| anyhow!("MetricsRegistry already initialised"))
    }

    /// Return the global registry.  Panics if not yet initialised.
    pub fn global() -> Arc<Self> {
        GLOBAL_REGISTRY
            .get()
            .expect("MetricsRegistry not initialised — call init() at startup")
            .clone()
    }

    /// Return `Some` if initialised, `None` otherwise (safe for test contexts).
    pub fn try_global() -> Option<Arc<Self>> {
        GLOBAL_REGISTRY.get().cloned()
    }

    // ── Register ──────────────────────────────────────────────────────────────

    /// Register a [`Counter`] (monotonically increasing).
    pub fn register_counter(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
    ) -> Result<(Counter, MetricHandle)> {
        let mut map = self.entries.write();
        if let Some(e) = map.get(name) {
            return match e.clone() {
                MetricEntry::Counter(c) => Ok((c, MetricHandle::new(name))),
                other => bail!(
                    "Metric '{name}' already registered as {}, cannot re-register as counter",
                    other.kind_str()
                ),
            };
        }
        let v = self.backend.create_counter(name, help, labels);
        map.insert(name.to_string(), MetricEntry::Counter(v.clone()));
        Ok((v, MetricHandle::new(name)))
    }

    /// Register an [`UpDownCounter`] (additive, non-monotonic).
    pub fn register_up_down_counter(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
    ) -> Result<(UpDownCounter, MetricHandle)> {
        let mut map = self.entries.write();
        if let Some(e) = map.get(name) {
            return match e.clone() {
                MetricEntry::UpDownCounter(c) => Ok((c, MetricHandle::new(name))),
                other => bail!(
                    "Metric '{name}' already registered as {}, cannot re-register as up_down_counter",
                    other.kind_str()
                ),
            };
        }
        let v = self.backend.create_up_down_counter(name, help, labels);
        map.insert(name.to_string(), MetricEntry::UpDownCounter(v.clone()));
        Ok((v, MetricHandle::new(name)))
    }

    /// Register a [`Gauge`] (non-additive snapshot).
    pub fn register_gauge(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
    ) -> Result<(Gauge, MetricHandle)> {
        let mut map = self.entries.write();
        if let Some(e) = map.get(name) {
            return match e.clone() {
                MetricEntry::Gauge(g) => Ok((g, MetricHandle::new(name))),
                other => bail!(
                    "Metric '{name}' already registered as {}, cannot re-register as gauge",
                    other.kind_str()
                ),
            };
        }
        let v = self.backend.create_gauge(name, help, labels);
        map.insert(name.to_string(), MetricEntry::Gauge(v.clone()));
        Ok((v, MetricHandle::new(name)))
    }

    /// Register a [`Histogram`].
    ///
    /// `buckets` are the upper-bound values for each histogram bucket.
    /// Pass `None` to use the backend's default bucket boundaries.
    pub fn register_histogram(
        &self,
        name: &str,
        help: &str,
        labels: Labels<'_>,
        buckets: Option<&[f64]>,
    ) -> Result<(Histogram, MetricHandle)> {
        let mut map = self.entries.write();
        if let Some(e) = map.get(name) {
            return match e.clone() {
                MetricEntry::Histogram(h) => Ok((h, MetricHandle::new(name))),
                other => bail!(
                    "Metric '{name}' already registered as {}, cannot re-register as histogram",
                    other.kind_str()
                ),
            };
        }
        let v = self.backend.create_histogram(name, help, labels, buckets);
        map.insert(name.to_string(), MetricEntry::Histogram(v.clone()));
        Ok((v, MetricHandle::new(name)))
    }

    // ── Unregister ────────────────────────────────────────────────────────────

    /// Remove the metric identified by `name`.
    /// Returns `true` if found and removed, `false` if not registered.
    pub fn unregister(&self, name: &str) -> bool {
        let mut map = self.entries.write();
        if map.remove(name).is_some() {
            self.backend.remove(name);
            true
        } else {
            warn!(metric = name, "unregister: metric not found");
            false
        }
    }

    /// Remove the metric referenced by `handle`.
    pub fn unregister_handle(&self, handle: &MetricHandle) -> bool {
        self.unregister(&handle.name)
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    /// Collect the current metrics snapshot from the backend.
    pub fn gather(&self) -> String {
        self.backend.gather()
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
    pub fn contains(&self, name: &str) -> bool {
        self.entries.read().contains_key(name)
    }
}
