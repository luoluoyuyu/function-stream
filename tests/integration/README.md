# Integration Tests

This directory contains the end-to-end (E2E) integration test suite for FunctionStream.

## 📋 Prerequisites

Ensure the following dependencies are available in your `PATH` before running the suite:

| Dependency | Version  | Note                                         |
|------------|----------|----------------------------------------------|
| Python     | `>= 3.9` | Test framework runtime                       |
| Rust       | `stable` | For compiling the `function-stream` binary   |
| Docker     | `>= 20.10`| Required for containerized infrastructure (Kafka, MinIO) |

*Note: Infrastructure containers (e.g., `apache/kafka:3.7.0` in KRaft mode, `minio/minio`) are automatically provisioned and torn down by the test framework via the Docker daemon.*

## 🚀 Quick Start

The test suite requires a fresh release build of the engine with the `python` feature enabled.

```bash
# From the project root
make build
make integration-test
```
