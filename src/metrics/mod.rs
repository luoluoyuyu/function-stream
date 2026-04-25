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

//! Metrics subsystem with a process-wide [`MetricsManager`].
//!
pub mod core;
pub mod do_nothing;
pub mod manager;
pub mod stats;
pub mod prometheus;
pub mod registry;

// ── Public re-exports ─────────────────────────────────────────────────────────

#[allow(unused_imports)]
pub use core::{Counter, Gauge, Histogram, MetricsBackend, UpDownCounter};
#[allow(unused_imports)]
pub use do_nothing::DoNothingBackend;
pub use manager::MetricsManager;
#[allow(unused_imports)]
pub use prometheus::PrometheusBackend;
#[allow(unused_imports)]
pub use registry::{MetricHandle, MetricsRegistry};
#[allow(unused_imports)]
pub use stats::state_metrics_adapter::StateMetricsBridge;

use anyhow::{Context, Result};

use crate::config::metrics_config::MetricsConfig;

/// One-shot bootstrap: [`MetricsManager::init`] with `config.metrics`.
pub fn init(config: &MetricsConfig) -> Result<()> {
    MetricsManager::init(config).context("MetricsManager::init")
}

/// Called from `ComponentRegistry` during `bootstrap_system`.
pub fn initialize_metrics(config: &crate::config::GlobalConfig) -> Result<()> {
    init(&config.metrics).context("Metrics subsystem initialisation failed")
}
