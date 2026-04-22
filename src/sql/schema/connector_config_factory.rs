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

use datafusion::arrow::datatypes::Schema;
use datafusion::common::Result;

use super::connector_config::ConnectorConfig;
use super::connector_registry::REGISTRY;
use super::kafka_operator_config::build_kafka_proto_config_from_string_map;
use super::sink_runtime_config::SinkRuntimeConfig;
use super::table_role::TableRole;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::{BadData, Format};

pub fn build_connector_config(
    connector_name: &str,
    role: TableRole,
    options: &mut ConnectorOptions,
    format: &Option<Format>,
    bad_data: BadData,
) -> Result<ConnectorConfig> {
    let runtime_opts_map = options.snapshot_for_catalog().into_iter().collect();
    let runtime_props = SinkRuntimeConfig::from_options_map(&runtime_opts_map)?.to_runtime_properties();
    match role {
        TableRole::Ingestion | TableRole::Reference => {
            REGISTRY
                .get_source(connector_name)?
                .build_source_config(options, format, bad_data)
        }
        TableRole::Egress => REGISTRY
            .get_sink(connector_name)?
            .build_sink_config(options, format, &runtime_props),
    }
}

pub fn build_connector_config_from_options(
    connector_name: &str,
    role: TableRole,
    options: &mut ConnectorOptions,
    format: &Option<Format>,
    bad_data: BadData,
) -> Result<ConnectorConfig> {
    build_connector_config(connector_name, role, options, format, bad_data)
}

pub fn build_connector_config_from_catalog(
    connector_name: &str,
    role: TableRole,
    opts: HashMap<String, String>,
    physical_schema: &Schema,
) -> Result<ConnectorConfig> {
    if connector_name.eq_ignore_ascii_case("kafka") {
        return match (role, build_kafka_proto_config_from_string_map(opts, physical_schema)?) {
            (TableRole::Ingestion | TableRole::Reference, protocol::function_stream_graph::connector_op::Config::KafkaSource(cfg)) => {
                Ok(ConnectorConfig::KafkaSource(cfg))
            }
            (TableRole::Egress, protocol::function_stream_graph::connector_op::Config::KafkaSink(cfg)) => {
                Ok(ConnectorConfig::KafkaSink(cfg))
            }
            (TableRole::Ingestion | TableRole::Reference, _) => {
                datafusion::common::plan_err!("catalog kafka source role requires kafka_source config")
            }
            (TableRole::Egress, _) => {
                datafusion::common::plan_err!("catalog kafka sink role requires kafka_sink config")
            }
        };
    }

    let mut options = ConnectorOptions::from_flat_string_map(opts)?;
    let format = Format::from_opts(&mut options)?;
    let bad_data = BadData::from_opts(&mut options)?;
    build_connector_config(connector_name, role, &mut options, &format, bad_data)
}
