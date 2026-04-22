// Licensed under the Apache License, Version 2.0

use datafusion::common::Result;

use super::connector_config::ConnectorConfig;
use super::connector_provider::SinkProvider;
use super::sink_config_codec::build_sink_connector_config;
use super::sink_runtime_config::SinkRuntimeProperties;
use super::table_role::TableRole;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::Format;

pub struct PassthroughSinkConnector {
    pub name: &'static str,
}

impl SinkProvider for PassthroughSinkConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        _format: &Option<Format>,
        runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig> {
        let mut extra_opts = options.drain_remaining_string_values()?;
        // SinkBuilder 已提前抽走 runtime 选项；透传前回填，确保 typed config 中 runtime_properties 完整。
        for (k, v) in runtime_props {
            extra_opts.entry(k.clone()).or_insert_with(|| v.clone());
        }
        build_sink_connector_config(self.name, TableRole::Egress, extra_opts)
    }
}
