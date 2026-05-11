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

// Runtime module

pub use function_stream_runtime_common::{common, memory};

// Implementation sources live under `src/streaming_runtime/src/{streaming,util}/` and are
// compiled here so `crate::sql` / `crate::runtime::memory` paths keep resolving.
#[path = "../streaming_runtime/src/streaming/mod.rs"]
pub mod streaming;

#[path = "../streaming_runtime/src/util/mod.rs"]
pub mod util;

// WASM runtime sources live under `src/wasm_runtime/src/wasm/`; compiled here for `crate::` paths.
#[path = "../wasm_runtime/src/wasm/mod.rs"]
pub mod wasm;

pub use wasm::input;
pub use wasm::output;
pub use wasm::processor;
