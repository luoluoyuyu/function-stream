// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use datafusion::common::{Result, plan_err};

use super::connector_registry::REGISTRY;
use super::source_table::SourceTable;
use super::table_builder_factory::TableBuildBase;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::BadData;

pub struct SourceBuilder<'a> {
    identifier: &'a str,
    connector_name: &'a str,
    options: &'a mut ConnectorOptions,
    base: TableBuildBase,
    bad_data: BadData,
}

impl<'a> SourceBuilder<'a> {
    pub fn new(
        identifier: &'a str,
        connector_name: &'a str,
        options: &'a mut ConnectorOptions,
        base: TableBuildBase,
        bad_data: BadData,
    ) -> Self {
        Self {
            identifier,
            connector_name,
            options,
            base,
            bad_data,
        }
    }

    pub fn build(self) -> Result<SourceTable> {
        let provider = REGISTRY.get_source(self.connector_name)?;
        let connector_config =
            provider.build_source_config(self.options, &self.base.connection_format, self.bad_data)?;

        if !self.options.is_empty() {
            let unknown_keys: Vec<String> = self.options.keys().cloned().collect();
            return plan_err!(
                "Unknown options for SOURCE connector '{}': {:?}",
                self.connector_name,
                unknown_keys
            );
        }

        Ok(SourceTable {
            adapter_type: self.connector_name.to_string(),
            table_identifier: self.identifier.to_string(),
            role: self.base.role,
            connector_config,
            temporal_config: self.base.temporal_config,
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
