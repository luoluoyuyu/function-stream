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
use datafusion::common::{Result, plan_err};

use super::connector_config::ConnectorConfig;
use super::kafka_operator_config::{
    build_kafka_proto_config, build_kafka_proto_config_from_string_map,
};
use super::sink_config_codec::build_sink_connector_config;
use super::table_role::TableRole;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::constants::connector_type;
use crate::sql::common::formats::{BadData, Format};

pub fn build_connector_config_from_options(
    connector_name: &str,
    role: TableRole,
    options: &mut ConnectorOptions,
    format: &Option<Format>,
    bad_data: BadData,
) -> Result<ConnectorConfig> {
    if connector_name.eq_ignore_ascii_case(connector_type::KAFKA) {
        let proto_cfg = build_kafka_proto_config(options, role, format, bad_data)?;
        return match (role, proto_cfg) {
            (
                TableRole::Ingestion | TableRole::Reference,
                protocol::function_stream_graph::connector_op::Config::KafkaSource(cfg),
            ) => Ok(ConnectorConfig::KafkaSource(cfg)),
            (
                TableRole::Egress,
                protocol::function_stream_graph::connector_op::Config::KafkaSink(cfg),
            ) => Ok(ConnectorConfig::KafkaSink(cfg)),
            (TableRole::Ingestion | TableRole::Reference, _) => {
                plan_err!("kafka source role requires kafka_source config")
            }
            (TableRole::Egress, _) => plan_err!("kafka sink role requires kafka_sink config"),
        };
    }

    match role {
        TableRole::Egress => {
            let extra_opts = options.drain_remaining_string_values()?;
            build_sink_connector_config(connector_name, role, extra_opts)
        }
        TableRole::Ingestion | TableRole::Reference => plan_err!(
            "connector '{connector_name}' source/lookup config is not implemented yet; sink config cannot be reused"
        ),
    }
}

pub fn build_connector_config_from_catalog(
    connector_name: &str,
    role: TableRole,
    opts: HashMap<String, String>,
    physical_schema: &Schema,
) -> Result<ConnectorConfig> {
    if connector_name.eq_ignore_ascii_case(connector_type::KAFKA) {
        let proto_cfg = build_kafka_proto_config_from_string_map(opts, physical_schema)?;
        return match (role, proto_cfg) {
            (
                TableRole::Ingestion | TableRole::Reference,
                protocol::function_stream_graph::connector_op::Config::KafkaSource(cfg),
            ) => Ok(ConnectorConfig::KafkaSource(cfg)),
            (
                TableRole::Egress,
                protocol::function_stream_graph::connector_op::Config::KafkaSink(cfg),
            ) => Ok(ConnectorConfig::KafkaSink(cfg)),
            (TableRole::Ingestion | TableRole::Reference, _) => {
                plan_err!("catalog kafka source role requires kafka_source config")
            }
            (TableRole::Egress, _) => {
                plan_err!("catalog kafka sink role requires kafka_sink config")
            }
        };
    }

    match role {
        TableRole::Egress => build_sink_connector_config(connector_name, role, opts),
        TableRole::Ingestion | TableRole::Reference => plan_err!(
            "connector '{connector_name}' source/lookup catalog config is not implemented yet; sink config cannot be reused"
        ),
    }
}
