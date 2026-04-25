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

//! Process-wide metrics manager.

use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow};
use tracing::info;

use crate::config::metrics_config::MetricsBackend as ConfigMetricsBackend;
use crate::config::metrics_config::MetricsConfig;
use crate::metrics::core::MetricsBackend;
use crate::metrics::do_nothing::DoNothingBackend;
use crate::metrics::prometheus::PrometheusBackend;
use crate::metrics::registry::MetricsRegistry;

static GLOBAL: OnceLock<Arc<MetricsManager>> = OnceLock::new();

/// Process-wide metrics manager, initialised once.
pub struct MetricsManager {
    registry: Arc<MetricsRegistry>,
    config_backend: ConfigMetricsBackend,
}

impl MetricsManager {
    /// Initialise backend, registry, and optional Prometheus exporter.
    pub fn init(config: &MetricsConfig) -> Result<()> {
        if GLOBAL.get().is_some() {
            return Err(anyhow!("MetricsManager already initialised"));
        }

        let backend: Arc<dyn MetricsBackend> = match &config.backend {
            ConfigMetricsBackend::DoNothing => {
                info!("Metrics: backend=do_nothing (no collection, zero overhead)");
                Arc::new(DoNothingBackend::default()) as Arc<dyn MetricsBackend>
            }
            ConfigMetricsBackend::Prometheus => {
                info!(address = %config.prometheus.bind_address, "Metrics: backend=prometheus");
                Arc::new(PrometheusBackend::new()) as Arc<dyn MetricsBackend>
            }
        };

        MetricsRegistry::init(Arc::clone(&backend)).context("MetricsRegistry::init failed")?;

        let registry = MetricsRegistry::global();
        let manager = Arc::new(MetricsManager {
            registry,
            config_backend: config.backend.clone(),
        });

        GLOBAL
            .set(Arc::clone(&manager))
            .map_err(|_| anyhow!("Failed to set global MetricsManager"))?;

        if matches!(&config.backend, ConfigMetricsBackend::Prometheus) {
            let bind = config.prometheus.bind_address.clone();
            let m = manager.clone();
            tokio::spawn(async move {
                let shutdown = std::future::pending::<()>();
                if let Err(e) = crate::metrics::prometheus::serve_metrics(&bind, m, shutdown).await
                {
                    tracing::error!(error = %e, "Prometheus /metrics exporter failed");
                }
            });
        }

        Ok(())
    }

    /// Returns the process-wide manager. Panics if not initialised.
    pub fn global() -> Arc<MetricsManager> {
        GLOBAL
            .get()
            .expect("MetricsManager not initialised — call initialize_metrics at startup")
            .clone()
    }

    /// Returns `Some` after init, otherwise `None`.
    pub fn try_global() -> Option<Arc<MetricsManager>> {
        GLOBAL.get().cloned()
    }

    /// Whether configured as `do_nothing`.
    pub fn is_do_nothing(&self) -> bool {
        matches!(self.config_backend, ConfigMetricsBackend::DoNothing)
    }

    /// Whether configured as `prometheus`.
    pub fn is_prometheus(&self) -> bool {
        matches!(self.config_backend, ConfigMetricsBackend::Prometheus)
    }

    /// Selected backend from config.
    pub fn config_backend(&self) -> &ConfigMetricsBackend {
        &self.config_backend
    }

    /// Inner registry.
    pub fn registry(&self) -> &Arc<MetricsRegistry> {
        &self.registry
    }
}

impl Deref for MetricsManager {
    type Target = MetricsRegistry;

    fn deref(&self) -> &Self::Target {
        self.registry.as_ref()
    }
}
