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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::common::{DataFusionError, Result};

use super::connector_provider::{SinkProvider, SourceProvider};
use super::kafka_connector::KafkaConnector;
use super::passthrough_sink_connector::PassthroughSinkConnector;
use super::s3_connector::S3Connector;
use crate::sql::common::constants::connector_type;

pub struct ConnectorRegistry {
    sources: HashMap<String, Arc<dyn SourceProvider>>,
    sinks: HashMap<String, Arc<dyn SinkProvider>>,
}

impl ConnectorRegistry {
    fn new() -> Self {
        let mut registry = Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        };

        let kafka = Arc::new(KafkaConnector);
        registry.register_source(kafka.clone());
        registry.register_sink(kafka);

        registry.register_sink(Arc::new(S3Connector));
        registry.register_sink(Arc::new(PassthroughSinkConnector {
            name: connector_type::FILESYSTEM,
        }));
        registry.register_sink(Arc::new(PassthroughSinkConnector {
            name: connector_type::DELTA,
        }));
        registry.register_sink(Arc::new(PassthroughSinkConnector {
            name: connector_type::ICEBERG,
        }));
        registry.register_sink(Arc::new(PassthroughSinkConnector { name: "lancedb" }));

        registry
    }

    pub fn register_source(&mut self, provider: Arc<dyn SourceProvider>) {
        self.sources.insert(provider.name().to_ascii_lowercase(), provider);
    }

    pub fn register_sink(&mut self, provider: Arc<dyn SinkProvider>) {
        self.sinks.insert(provider.name().to_ascii_lowercase(), provider);
    }

    pub fn get_source(&self, connector_name: &str) -> Result<Arc<dyn SourceProvider>> {
        self.sources
            .get(&connector_name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Connector '{}' is not registered or does not support being used as a SOURCE",
                    connector_name
                ))
            })
    }

    pub fn get_sink(&self, connector_name: &str) -> Result<Arc<dyn SinkProvider>> {
        self.sinks
            .get(&connector_name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Connector '{}' is not registered or does not support being used as a SINK",
                    connector_name
                ))
            })
    }
}

pub static REGISTRY: LazyLock<ConnectorRegistry> = LazyLock::new(ConnectorRegistry::new);
