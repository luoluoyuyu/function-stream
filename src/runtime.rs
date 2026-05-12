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

// In-tree runtime: streaming engine, util helpers, and WASM task runtime.
// Paths are relative to `src/` (this file lives at `src/runtime.rs`).

pub use function_stream_runtime_common::{common, memory};

#[path = "streaming_runtime/src/streaming/mod.rs"]
pub mod streaming;

#[path = "streaming_runtime/src/util/mod.rs"]
pub mod util;

#[path = "wasm_runtime/src/wasm/mod.rs"]
pub mod wasm;

pub use wasm::{input, output, processor};
