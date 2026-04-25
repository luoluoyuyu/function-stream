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

use serde::{Deserialize, Serialize};

/// Which metrics backend to activate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetricsBackend {
    /// Discard all metrics (default, zero overhead).
    #[default]
    DoNothing,
    /// Export metrics in Prometheus text format via an HTTP endpoint.
    Prometheus,
}

/// Configuration for the Prometheus metrics exporter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Address the `/metrics` HTTP endpoint will bind to.
    pub bind_address: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9090".to_string(),
        }
    }
}

/// Top-level metrics configuration, embedded in [`GlobalConfig`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsConfig {
    /// The backend to use for recording metrics.
    #[serde(default)]
    pub backend: MetricsBackend,

    /// Prometheus-specific settings. Only used when `backend == Prometheus`.
    #[serde(default)]
    pub prometheus: PrometheusConfig,
}
