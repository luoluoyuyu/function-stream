// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use datafusion::common::{DataFusionError, Result, plan_err};
use protocol::function_stream_graph::connector_op::Config as ProtoConfig;

use super::connector_config::ConnectorConfig;
use super::connector_provider::{SinkProvider, SourceProvider, require_option};
use super::kafka_operator_config::build_kafka_proto_config;
use super::sink_runtime_config::SinkRuntimeProperties;
use super::table_role::TableRole;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::{BadData, Format};
use crate::sql::common::with_option_keys as opt;

pub struct KafkaConnector;

impl KafkaConnector {
    const NAME: &'static str = "kafka";

    fn validate_common(&self, options: &mut ConnectorOptions) -> Result<()> {
        let servers = require_option(options, opt::KAFKA_BOOTSTRAP_SERVERS, Self::NAME).or_else(
            |_| require_option(options, opt::KAFKA_BOOTSTRAP_SERVERS_LEGACY, Self::NAME),
        )?;
        if servers.trim().is_empty() {
            return plan_err!("'bootstrap.servers' cannot be empty");
        }
        options.insert_str(opt::KAFKA_BOOTSTRAP_SERVERS, servers)?;

        let topic = require_option(options, opt::KAFKA_TOPIC, Self::NAME)?;
        if topic.trim().is_empty() {
            return plan_err!("'topic' cannot be empty");
        }
        options.insert_str(opt::KAFKA_TOPIC, topic)?;
        Ok(())
    }
}

impl SourceProvider for KafkaConnector {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn build_source_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        bad_data: BadData,
    ) -> Result<ConnectorConfig> {
        self.validate_common(options)?;
        match build_kafka_proto_config(options, TableRole::Ingestion, format, bad_data)? {
            ProtoConfig::KafkaSource(cfg) => Ok(ConnectorConfig::KafkaSource(cfg)),
            _ => Err(DataFusionError::Plan(
                "Kafka source role requires kafka_source config".to_string(),
            )),
        }
    }
}

impl SinkProvider for KafkaConnector {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        format: &Option<Format>,
        _runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig> {
        self.validate_common(options)?;
        match build_kafka_proto_config(options, TableRole::Egress, format, BadData::Fail {})? {
            ProtoConfig::KafkaSink(cfg) => Ok(ConnectorConfig::KafkaSink(cfg)),
            _ => Err(DataFusionError::Plan(
                "Kafka sink role requires kafka_sink config".to_string(),
            )),
        }
    }
}
