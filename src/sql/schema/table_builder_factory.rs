// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;
use std::time::Duration;

use datafusion::common::{Result, plan_err};

use super::data_encoding_format::DataEncodingFormat;
use super::sink_builder::SinkBuilder;
use super::source_builder::SourceBuilder;
use super::source_table::SourceTable;
use super::table_role::TableRole;
use super::temporal_pipeline_config::TemporalPipelineConfig;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::formats::{BadData, Format};
use crate::sql::common::with_option_keys as opt;

pub struct TableBuilderFactory;

pub struct TableBuildBase {
    pub role: TableRole,
    pub schema_specs: Vec<super::column_descriptor::ColumnDescriptor>,
    pub key_constraints: Vec<String>,
    pub payload_format: Option<DataEncodingFormat>,
    pub connection_format: Option<Format>,
    pub description: String,
    pub catalog_with_options: BTreeMap<String, String>,
    pub temporal_config: TemporalPipelineConfig,
    pub lookup_cache_max_bytes: Option<u64>,
    pub lookup_cache_ttl: Option<Duration>,
}

impl TableBuilderFactory {
    pub fn deduce_role(options: &mut ConnectorOptions) -> Result<TableRole> {
        match options.pull_opt_str(opt::TYPE)?.as_deref() {
            None | Some("source") => Ok(TableRole::Ingestion),
            Some("sink") => Ok(TableRole::Egress),
            Some("lookup") => Ok(TableRole::Reference),
            Some(other) => plan_err!("Invalid connection type '{}'", other),
        }
    }

    pub fn create_builder_with_role<'a>(
        identifier: &'a str,
        connector_name: &'a str,
        options: &'a mut ConnectorOptions,
        base: TableBuildBase,
        bad_data: BadData,
    ) -> Result<BuilderVariant<'a>> {
        match base.role {
            TableRole::Ingestion | TableRole::Reference => Ok(BuilderVariant::Source(
                SourceBuilder::new(identifier, connector_name, options, base, bad_data),
            )),
            TableRole::Egress => Ok(BuilderVariant::Sink(SinkBuilder::new(
                identifier,
                connector_name,
                options,
                base,
            )?)),
        }
    }
}

pub enum BuilderVariant<'a> {
    Source(SourceBuilder<'a>),
    Sink(SinkBuilder<'a>),
}

impl<'a> BuilderVariant<'a> {
    pub fn build(self) -> Result<SourceTable> {
        match self {
            BuilderVariant::Source(builder) => builder.build(),
            BuilderVariant::Sink(builder) => builder.build(),
        }
    }
}
