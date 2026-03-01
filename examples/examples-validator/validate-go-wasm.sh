#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
GO_EXAMPLE_DIR="$ROOT_DIR/examples/go-processor"
VALIDATOR_CARGO="$ROOT_DIR/examples/examples-validator/Cargo.toml"
GO_SDK_DIR="$ROOT_DIR/go-sdk"

FS_HOST="${FS_HOST:-127.0.0.1}"
FS_PORT="${FS_PORT:-8080}"
BROKERS="${BROKERS:-127.0.0.1:9092}"
INPUT_TOPIC="${INPUT_TOPIC:-input-topic}"
OUTPUT_TOPIC="${OUTPUT_TOPIC:-output-topic}"
GROUP_ID="${GROUP_ID:-industrial-verifier-v1}"
MSG_COUNT="${MSG_COUNT:-2000}"
AUTO_START_SERVER="${AUTO_START_SERVER:-1}"
AUTO_START_KAFKA="${AUTO_START_KAFKA:-1}"

if [ -d "$ROOT_DIR/.tools/tinygo/bin" ]; then
  export PATH="$ROOT_DIR/.tools/tinygo/bin:$PATH"
fi

TMP_DIR="$(mktemp -d)"
SERVER_PID=""
KAFKA_PID=""
KAFKA_HOME="${KAFKA_HOME:-$ROOT_DIR/.tools/kafka}"
BROKER_ENDPOINT="$(printf "%s" "$BROKERS" | cut -d',' -f1)"
BROKER_HOST="$(printf "%s" "$BROKER_ENDPOINT" | cut -d':' -f1)"
BROKER_PORT="$(printf "%s" "$BROKER_ENDPOINT" | cut -d':' -f2)"
if [ -z "$BROKER_PORT" ]; then
  BROKER_PORT="9092"
fi

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$KAFKA_PID" ] && kill -0 "$KAFKA_PID" >/dev/null 2>&1; then
    kill "$KAFKA_PID" >/dev/null 2>&1 || true
    wait "$KAFKA_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

check_port() {
  (echo >"/dev/tcp/$FS_HOST/$FS_PORT") >/dev/null 2>&1
}

check_broker_port() {
  (echo >"/dev/tcp/$BROKER_HOST/$BROKER_PORT") >/dev/null 2>&1
}

wait_for_server() {
  for _ in $(seq 1 120); do
    if check_port; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_broker() {
  for _ in $(seq 1 120); do
    if check_broker_port; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_local_kafka() {
  if [ "$BROKER_HOST" != "127.0.0.1" ] && [ "$BROKER_HOST" != "localhost" ]; then
    echo "broker host must be localhost or 127.0.0.1 for auto-start"
    exit 1
  fi
  if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
    mkdir -p "$ROOT_DIR/.tools"
    curl -L -o "$TMP_DIR/kafka.tgz" "https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz"
    mkdir -p "$KAFKA_HOME"
    tar -xzf "$TMP_DIR/kafka.tgz" --strip-components=1 -C "$KAFKA_HOME"
  fi
  local controller_port
  controller_port="$((BROKER_PORT + 1))"
  local kafka_cfg="$TMP_DIR/kafka.properties"
  cat >"$kafka_cfg" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:${controller_port}
listeners=PLAINTEXT://127.0.0.1:${BROKER_PORT},CONTROLLER://127.0.0.1:${controller_port}
advertised.listeners=PLAINTEXT://${BROKER_HOST}:${BROKER_PORT}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=${TMP_DIR}/kafka-logs
num.partitions=1
auto.create.topics.enable=true
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
  local cluster_id
  cluster_id="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$cluster_id" -c "$kafka_cfg" >/dev/null
  "$KAFKA_HOME/bin/kafka-server-start.sh" "$kafka_cfg" >"$TMP_DIR/kafka.log" 2>&1 &
  KAFKA_PID="$!"
  if ! wait_for_broker; then
    echo "failed to start kafka"
    echo "kafka log: $TMP_DIR/kafka.log"
    exit 1
  fi
}

prepare_config() {
  local source_file="$GO_EXAMPLE_DIR/config.yaml"
  local output_file="$TMP_DIR/config.yaml"
  sed -E "s/(bootstrap_servers: \").*(\")/\\1${BROKERS}\\2/g" "$source_file" >"$output_file"
  printf "%s" "$output_file"
}

create_function_via_cli() {
  local wasm_path="$GO_EXAMPLE_DIR/build/processor.wasm"
  local config_path="$1"
  local cli_bin="$ROOT_DIR/target/release/cli"

  if [ ! -x "$cli_bin" ]; then
    CC=gcc CXX=g++ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=g++ \
      cargo build --release -p function-stream-cli --bin cli >/dev/null
  fi

  {
    printf "drop function go-processor-example;\n"
    printf "create function with ('function_path'='%s','config_path'='%s');\n" "$wasm_path" "$config_path"
    printf "show functions;\n"
    printf "quit\n"
  } | "$cli_bin" -h "$FS_HOST" -p "$FS_PORT"
}

run_validator() {
  CC=gcc CXX=g++ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=g++ \
    cargo run --release --manifest-path "$VALIDATOR_CARGO" --bin kafka_test -- \
    --brokers "$BROKERS" \
    --input-topic "$INPUT_TOPIC" \
    --output-topic "$OUTPUT_TOPIC" \
    --group-id "$GROUP_ID" \
    --msg-count "$MSG_COUNT"
}

if ! command -v tinygo >/dev/null 2>&1; then
  echo "tinygo is required"
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required"
  exit 1
fi

if ! command -v make >/dev/null 2>&1; then
  echo "make is required"
  exit 1
fi

if ! check_broker_port; then
  if [ "$AUTO_START_KAFKA" != "1" ]; then
    echo "kafka broker is not reachable at $BROKER_HOST:$BROKER_PORT"
    exit 1
  fi
  start_local_kafka
fi

make -C "$GO_SDK_DIR" bindings >/dev/null
"$GO_EXAMPLE_DIR/build.sh"

if ! check_port; then
  if [ "$AUTO_START_SERVER" != "1" ]; then
    echo "function-stream server is not reachable at $FS_HOST:$FS_PORT"
    exit 1
  fi

  CC=gcc CXX=g++ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=g++ \
    cargo build --release --bin function-stream >/dev/null
  FUNCTION_STREAM_HOME="$ROOT_DIR" \
    "$ROOT_DIR/target/release/function-stream" --config "$ROOT_DIR/conf/config.yaml" >"$TMP_DIR/server.log" 2>&1 &
  SERVER_PID="$!"

  if ! wait_for_server; then
    echo "failed to start function-stream server"
    echo "server log: $TMP_DIR/server.log"
    exit 1
  fi
fi

CONFIG_PATH="$(prepare_config)"
create_function_via_cli "$CONFIG_PATH"
run_validator

echo "validation completed"
