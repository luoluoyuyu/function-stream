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
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use prost::Message;
use protocol::function_stream_graph::ConnectorOp;
use protocol::function_stream_graph::connector_op::Config;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::connector::sink_props_codec::{
    apply_common_sink_fields, normalized_props,
};
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::factory::operator_constructor::OperatorConstructor;
use crate::runtime::streaming::operators::sink::lancedb::LanceDbSinkOperator;
use crate::sql::common::constants::connection_format_value;
use crate::sql::common::with_option_keys as opt;

pub struct LanceDbSinkDispatcher;

impl OperatorConstructor for LanceDbSinkDispatcher {
    fn with_config(&self, payload: &[u8], _registry: Arc<Registry>) -> Result<ConstructedOperator> {
        let op = ConnectorOp::decode(payload).context("failed to decode connector op")?;
        let props = match op.config {
            Some(Config::LancedbSink(cfg)) => lancedb_props(cfg),
            _ => bail!("lanceDB connector expects LanceDbSinkConfig"),
        };
        let props = normalized_props(props);

        let format = props
            .get(opt::FORMAT)
            .map(String::as_str)
            .unwrap_or(connection_format_value::LANCE)
            .to_ascii_lowercase();
        if format != connection_format_value::LANCE {
            bail!("lanceDB requires format='lance', got '{format}'");
        }

        let dataset_uri = resolve_lance_uri(&props)?;
        let sink = LanceDbSinkOperator::new(op.name, dataset_uri);
        Ok(ConstructedOperator::Operator(Box::new(sink)))
    }
}

fn resolve_lance_uri(props: &HashMap<String, String>) -> Result<String> {
    let path = props
        .get(opt::PATH)
        .cloned()
        .or_else(|| props.get(opt::SINK_PATH).cloned())
        .unwrap_or_else(|| ".".to_string());

    // If path already contains a fully-qualified URI scheme, use it as-is.
    if path.contains("://") {
        return Ok(path);
    }

    if let Some(bucket) = props.get(opt::S3_BUCKET) {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            Ok(format!("s3://{bucket}"))
        } else {
            Ok(format!("s3://{bucket}/{trimmed}"))
        }
    } else {
        Ok(path)
    }
}

fn lancedb_props(
    cfg: protocol::function_stream_graph::LanceDbSinkConfig,
) -> HashMap<String, String> {
    let mut props = cfg.extra_properties;
    props.extend(cfg.runtime_properties);
    apply_common_sink_fields(&mut props, cfg.path, cfg.format, None);
    if let Some(v) = cfg.s3_bucket {
        props.insert(opt::S3_BUCKET.to_string(), v);
    }
    if let Some(v) = cfg.s3_region {
        props.insert(opt::S3_REGION.to_string(), v);
    }
    if let Some(v) = cfg.s3_endpoint {
        props.insert(opt::S3_ENDPOINT.to_string(), v);
    }
    if let Some(v) = cfg.s3_access_key_id {
        props.insert(opt::S3_ACCESS_KEY_ID.to_string(), v);
    }
    if let Some(v) = cfg.s3_secret_access_key {
        props.insert(opt::S3_SECRET_ACCESS_KEY.to_string(), v);
    }
    if let Some(v) = cfg.s3_session_token {
        props.insert(opt::S3_SESSION_TOKEN.to_string(), v);
    }
    props
}
