// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use datafusion::common::{Result, plan_err};

use super::connector_registry::REGISTRY;
use super::sink_runtime_config::{SinkRuntimeConfig, SinkRuntimeProperties};
use super::source_table::SourceTable;
use super::table_builder_factory::TableBuildBase;
use super::table_role::TableRole;
use super::temporal_pipeline_config::TemporalPipelineConfig;
use crate::sql::common::connector_options::ConnectorOptions;

pub struct SinkBuilder<'a> {
    identifier: &'a str,
    connector_name: &'a str,
    options: &'a mut ConnectorOptions,
    base: TableBuildBase,
    runtime_props: SinkRuntimeProperties,
}

impl<'a> SinkBuilder<'a> {
    pub fn new(
        identifier: &'a str,
        connector_name: &'a str,
        options: &'a mut ConnectorOptions,
        base: TableBuildBase,
    ) -> Result<Self> {
        let runtime_props = SinkRuntimeConfig::extract_from_options(options)?.to_runtime_properties();
        Ok(Self {
            identifier,
            connector_name,
            options,
            base,
            runtime_props,
        })
    }

    pub fn build(self) -> Result<SourceTable> {
        let provider = REGISTRY.get_sink(self.connector_name)?;
        let connector_config =
            provider.build_sink_config(self.options, &self.base.connection_format, &self.runtime_props)?;

        if !self.options.is_empty() {
            let unknown_keys: Vec<String> = self.options.keys().cloned().collect();
            return plan_err!(
                "Unknown options for SINK connector '{}': {:?}",
                self.connector_name,
                unknown_keys
            );
        }

        Ok(SourceTable {
            adapter_type: self.connector_name.to_string(),
            table_identifier: self.identifier.to_string(),
            role: TableRole::Egress,
            connector_config,
            temporal_config: TemporalPipelineConfig::default(),
            connection_format: self.base.connection_format,
            catalog_with_options: self.base.catalog_with_options,
            schema_specs: self.base.schema_specs,
            key_constraints: self.base.key_constraints,
            payload_format: self.base.payload_format,
            description: self.base.description,
            partition_exprs: Arc::new(None),
            lookup_cache_max_bytes: self.base.lookup_cache_max_bytes,
            lookup_cache_ttl: self.base.lookup_cache_ttl,
            inferred_fields: None,
            registry_id: None,
        })
    }
}
