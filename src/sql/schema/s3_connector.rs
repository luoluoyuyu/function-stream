// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use datafusion::common::{DataFusionError, Result, plan_err};
use protocol::function_stream_graph::{ParquetCompressionProto, S3SinkConfig, SinkFormatProto};

use super::connector_config::ConnectorConfig;
use super::connector_provider::{SinkProvider, require_option};
use super::sink_runtime_config::SinkRuntimeProperties;
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::constants::connection_format_value;
use crate::sql::common::formats::Format;
use crate::sql::common::with_option_keys as opt;

pub struct S3Connector;

impl SinkProvider for S3Connector {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn build_sink_config(
        &self,
        options: &mut ConnectorOptions,
        _format: &Option<Format>,
        runtime_props: &SinkRuntimeProperties,
    ) -> Result<ConnectorConfig> {
        let path = options
            .pull_opt_str(opt::PATH)?
            .or(options.pull_opt_str(opt::SINK_PATH)?)
            .ok_or_else(|| {
                DataFusionError::Plan("S3 Connector requires 'path' or 'sink.path'".to_string())
            })?;
        let bucket = require_option(options, opt::S3_BUCKET, self.name())?;
        let region = options
            .pull_opt_str(opt::S3_REGION)?
            .unwrap_or_else(|| "us-east-1".to_string());

        let format_str = require_option(options, opt::FORMAT, self.name())?;
        let sink_format = match format_str.to_ascii_lowercase().as_str() {
            connection_format_value::CSV => SinkFormatProto::SinkFormatCsv,
            connection_format_value::JSON
            | connection_format_value::JSONL
            | connection_format_value::NDJSON => SinkFormatProto::SinkFormatJsonl,
            connection_format_value::PARQUET => SinkFormatProto::SinkFormatParquet,
            other => return plan_err!("S3 connector does not support format '{}'", other),
        };

        let endpoint = options.pull_opt_str(opt::S3_ENDPOINT)?;
        let access_key_id = options.pull_opt_str(opt::S3_ACCESS_KEY_ID)?;
        let secret_access_key = options.pull_opt_str(opt::S3_SECRET_ACCESS_KEY)?;
        let session_token = options.pull_opt_str(opt::S3_SESSION_TOKEN)?;
        let parquet_compression = parse_optional_parquet_compression(options)?;
        let extra_properties = options.drain_remaining_string_values()?;

        Ok(ConnectorConfig::S3Sink(S3SinkConfig {
            path,
            format: sink_format as i32,
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            parquet_compression,
            extra_properties,
            runtime_properties: runtime_props.clone(),
        }))
    }
}

fn parse_optional_parquet_compression(options: &mut ConnectorOptions) -> Result<Option<i32>> {
    let Some(value) = options.pull_opt_str(opt::PARQUET_COMPRESSION)? else {
        return Ok(None);
    };
    let parsed = match value.to_ascii_lowercase().as_str() {
        "uncompressed" => ParquetCompressionProto::ParquetCompressionUncompressed,
        "snappy" => ParquetCompressionProto::ParquetCompressionSnappy,
        "gzip" => ParquetCompressionProto::ParquetCompressionGzip,
        "zstd" => ParquetCompressionProto::ParquetCompressionZstd,
        "lz4" => ParquetCompressionProto::ParquetCompressionLz4,
        "lz4_raw" => ParquetCompressionProto::ParquetCompressionLz4Raw,
        other => return plan_err!("unsupported parquet.compression '{other}'"),
    };
    Ok(Some(parsed as i32))
}
