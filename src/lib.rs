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

// Library crate for function-stream

#![allow(dead_code)]

pub use function_stream_config as config;
#[path = "coordinator/src/legacy/mod.rs"]
pub mod coordinator;
pub use function_stream_logger as logging;
pub mod runtime;
#[path = "servicer/src/legacy/mod.rs"]
pub mod server;
pub use function_stream_streaming_planner as sql;
pub mod storage;
