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

use std::sync::Arc;

use anyhow::Context;

// State backend sources live under `src/wasm_runtime/src/state_backend/`; compiled here for `crate::storage::state_backend`.
#[path = "../wasm_runtime/src/state_backend/mod.rs"]
pub mod state_backend;

// Stream catalog + task storage sources under `src/catalog_storage/src/{stream_catalog,task}/`;
// compiled here so `crate::storage::stream_catalog` / `crate::storage::task` keep resolving.
#[path = "../catalog_storage/src/stream_catalog/mod.rs"]
pub mod stream_catalog;

#[path = "../catalog_storage/src/task/mod.rs"]
pub mod task;

/// Install the process-global [`stream_catalog::CatalogManager`] from configuration.
/// In-memory when `config.stream_catalog.persist` is `false`, otherwise a durable
/// [`stream_catalog::RocksDbMetaStore`] (default path: `{data_dir}/catalog.db`).
pub fn initialize_stream_catalog(config: &crate::config::GlobalConfig) -> anyhow::Result<()> {
    use stream_catalog::{CatalogManager, InMemoryMetaStore, MetaStore, RocksDbMetaStore};

    let store: Arc<dyn MetaStore> = if !config.stream_catalog.persist {
        Arc::new(InMemoryMetaStore::new())
    } else {
        let path = config
            .stream_catalog
            .db_path
            .as_ref()
            .map(|p| crate::config::resolve_path(p))
            .unwrap_or_else(|| crate::config::get_data_dir().join("catalog.db"));

        std::fs::create_dir_all(&path).with_context(|| {
            format!(
                "Failed to create stream catalog RocksDB directory {}",
                path.display()
            )
        })?;

        Arc::new(RocksDbMetaStore::open(&path).with_context(|| {
            format!(
                "Failed to open stream catalog RocksDB at {}",
                path.display()
            )
        })?)
    };

    CatalogManager::init_global(store).context("Stream catalog (CatalogManager) global init failed")
}
