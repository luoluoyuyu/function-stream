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

use datafusion::common::{Result, plan_err};
use protocol::function_stream_graph::{
    DeltaSinkConfig, FilesystemSinkConfig, IcebergSinkConfig, LanceDbSinkConfig,
    ParquetCompressionProto, S3SinkConfig, SinkFormatProto,
};

use super::connector_config::ConnectorConfig;
use super::sink_runtime_config::SinkRuntimeConfig;
use super::table_role::TableRole;
use crate::sql::common::constants::{
    connection_format_value, connector_type, parquet_compression_value,
};
use crate::sql::common::with_option_keys as opt;

pub fn build_sink_connector_config(
    connector_name: &str,
    role: TableRole,
    extra_opts: HashMap<String, String>,
) -> Result<ConnectorConfig> {
    if role != TableRole::Egress {
        return plan_err!("connector '{connector_name}' only supports typed config as sink");
    }
    let runtime_props = SinkRuntimeConfig::from_options_map(&extra_opts)?.to_runtime_properties();
    match connector_name.to_ascii_lowercase().as_str() {
        connector_type::FILESYSTEM => Ok(ConnectorConfig::FilesystemSink(FilesystemSinkConfig {
            path: required_path(&extra_opts)?,
            format: parse_sink_format_for_connector(required_format(&extra_opts)?, connector_name)?
                as i32,
            parquet_compression: parse_optional_compression(&extra_opts)?,
            extra_properties: extra_opts.clone(),
            runtime_properties: runtime_props.clone(),
        })),
        connector_type::DELTA => Ok(ConnectorConfig::DeltaSink(DeltaSinkConfig {
            path: required_path(&extra_opts)?,
            format: parse_sink_format_for_connector(required_format(&extra_opts)?, connector_name)?
                as i32,
            parquet_compression: parse_optional_compression(&extra_opts)?,
            extra_properties: extra_opts.clone(),
            runtime_properties: runtime_props.clone(),
        })),
        connector_type::ICEBERG => Ok(ConnectorConfig::IcebergSink(IcebergSinkConfig {
            path: required_path(&extra_opts)?,
            format: parse_sink_format_for_connector(required_format(&extra_opts)?, connector_name)?
                as i32,
            parquet_compression: parse_optional_compression(&extra_opts)?,
            extra_properties: extra_opts.clone(),
            runtime_properties: runtime_props.clone(),
        })),
        connector_type::S3 => Ok(ConnectorConfig::S3Sink(S3SinkConfig {
            path: required_path(&extra_opts)?,
            format: parse_sink_format_for_connector(required_format(&extra_opts)?, connector_name)?
                as i32,
            bucket: required(&extra_opts, opt::S3_BUCKET)?,
            region: extra_opts
                .get(opt::S3_REGION)
                .cloned()
                .unwrap_or_else(|| "us-east-1".to_string()),
            endpoint: extra_opts.get(opt::S3_ENDPOINT).cloned(),
            access_key_id: extra_opts.get(opt::S3_ACCESS_KEY_ID).cloned(),
            secret_access_key: extra_opts.get(opt::S3_SECRET_ACCESS_KEY).cloned(),
            session_token: extra_opts.get(opt::S3_SESSION_TOKEN).cloned(),
            parquet_compression: parse_optional_compression(&extra_opts)?,
            extra_properties: extra_opts.clone(),
            runtime_properties: runtime_props.clone(),
        })),
        "lancedb" => Ok(ConnectorConfig::LanceDbSink(LanceDbSinkConfig {
            path: required_path(&extra_opts)?,
            format: parse_sink_format_for_connector(required_format(&extra_opts)?, connector_name)?
                as i32,
            s3_bucket: extra_opts.get(opt::S3_BUCKET).cloned(),
            s3_region: extra_opts.get(opt::S3_REGION).cloned(),
            s3_endpoint: extra_opts.get(opt::S3_ENDPOINT).cloned(),
            s3_access_key_id: extra_opts.get(opt::S3_ACCESS_KEY_ID).cloned(),
            s3_secret_access_key: extra_opts.get(opt::S3_SECRET_ACCESS_KEY).cloned(),
            s3_session_token: extra_opts.get(opt::S3_SESSION_TOKEN).cloned(),
            extra_properties: extra_opts.clone(),
            runtime_properties: runtime_props.clone(),
        })),
        _ => plan_err!("connector '{connector_name}' does not support typed sink config"),
    }
}

fn required(opts: &HashMap<String, String>, key: &str) -> Result<String> {
    opts.get(key).cloned().ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(format!("missing required WITH option '{key}'"))
    })
}

fn required_path(opts: &HashMap<String, String>) -> Result<String> {
    opts.get(opt::PATH)
        .cloned()
        .or_else(|| opts.get(opt::SINK_PATH).cloned())
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "missing required WITH option 'path' (or 'sink.path')".to_string(),
            )
        })
}

fn required_format(opts: &HashMap<String, String>) -> Result<String> {
    opts.get(opt::FORMAT).cloned().ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(
            "missing required WITH option 'format'".to_string(),
        )
    })
}

fn parse_sink_format_for_connector(value: String, connector: &str) -> Result<SinkFormatProto> {
    let fmt = match value.to_ascii_lowercase().as_str() {
        connection_format_value::CSV => SinkFormatProto::SinkFormatCsv,
        f if f == connection_format_value::JSONL
            || f == connection_format_value::NDJSON
            || f == connection_format_value::JSON =>
        {
            SinkFormatProto::SinkFormatJsonl
        }
        connection_format_value::AVRO => SinkFormatProto::SinkFormatAvro,
        connection_format_value::PARQUET => SinkFormatProto::SinkFormatParquet,
        f if f == connection_format_value::ORC => SinkFormatProto::SinkFormatOrc,
        connection_format_value::LANCE => SinkFormatProto::SinkFormatLance,
        other => return plan_err!("unsupported sink format '{other}'"),
    };
    let ok = match connector.to_ascii_lowercase().as_str() {
        connector_type::FILESYSTEM | connector_type::DELTA => matches!(
            fmt,
            SinkFormatProto::SinkFormatCsv
                | SinkFormatProto::SinkFormatJsonl
                | SinkFormatProto::SinkFormatAvro
                | SinkFormatProto::SinkFormatParquet
                | SinkFormatProto::SinkFormatOrc
        ),
        connector_type::ICEBERG | connector_type::S3 => {
            matches!(
                fmt,
                SinkFormatProto::SinkFormatCsv | SinkFormatProto::SinkFormatParquet
            )
        }
        "lancedb" => matches!(fmt, SinkFormatProto::SinkFormatLance),
        _ => false,
    };
    if !ok {
        return plan_err!("format '{value}' is not supported by connector '{connector}'");
    }
    Ok(fmt)
}

fn parse_optional_compression(opts: &HashMap<String, String>) -> Result<Option<i32>> {
    let Some(v) = opts.get(opt::PARQUET_COMPRESSION) else {
        return Ok(None);
    };
    let parsed = match v.to_ascii_lowercase().as_str() {
        parquet_compression_value::UNCOMPRESSED => {
            ParquetCompressionProto::ParquetCompressionUncompressed
        }
        parquet_compression_value::SNAPPY => ParquetCompressionProto::ParquetCompressionSnappy,
        parquet_compression_value::GZIP => ParquetCompressionProto::ParquetCompressionGzip,
        parquet_compression_value::ZSTD => ParquetCompressionProto::ParquetCompressionZstd,
        parquet_compression_value::LZ4 => ParquetCompressionProto::ParquetCompressionLz4,
        parquet_compression_value::LZ4_RAW => ParquetCompressionProto::ParquetCompressionLz4Raw,
        other => return plan_err!("unsupported parquet.compression '{other}'"),
    };
    Ok(Some(parsed as i32))
}
