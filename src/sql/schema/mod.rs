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

pub mod catalog_ddl;
pub mod column_descriptor;
pub mod connection_type;
pub mod connector_config;
pub mod connector_config_factory;
pub mod connector_provider;
pub mod connector_registry;
pub mod data_encoding_format;
pub mod kafka_connector;
pub mod kafka_operator_config;
pub mod passthrough_sink_connector;
pub mod schema_context;
pub mod schema_provider;
pub mod sink_builder;
pub mod sink_config_codec;
pub mod sink_runtime_config;
pub mod s3_connector;
pub mod source_table;
pub mod source_builder;
pub mod table;
pub mod table_builder_factory;
pub mod table_execution_unit;
pub mod table_role;
pub mod temporal_pipeline_config;
pub mod utils;

pub use catalog_ddl::{
    catalog_table_row_detail, schema_columns_one_line, show_create_catalog_table,
};
pub use column_descriptor::ColumnDescriptor;
pub use connection_type::ConnectionType;
pub use source_table::SourceTable;

/// Back-compat alias for [`SourceTable`].
pub type ConnectorTable = SourceTable;
pub use schema_provider::{ObjectName, StreamPlanningContext, StreamSchemaProvider, StreamTable};
pub use table::Table;
