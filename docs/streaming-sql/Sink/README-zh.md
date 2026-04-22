# Streaming SQL Sink 文档

本目录聚焦 Streaming SQL 的下游写出能力（Sink）。

## 支持矩阵

| Connector | 支持格式 |

|---|---|
| `kafka` | `json` / `raw_string` / `raw_bytes`（沿用 Kafka Sink 编码能力） |
| `filesystem` | `csv` / `parquet` / `json`(JSONL) / `avro` / `orc` |
| `s3` | `csv` / `parquet` |
| `delta` | `csv` / `parquet` / `json`(JSONL) / `avro` / `orc` |
| `iceberg` | `csv` / `parquet` |
| `lanceDB` | `lance` |

## 文档列表

- [Kafka Sink](kafka-sink-zh.md)
- [Filesystem Sink](filesystem-sink-zh.md)
- [S3 Sink](s3-sink-zh.md)
- [Delta Sink](delta-sink-zh.md)
- [Iceberg Sink](iceberg-sink-zh.md)
- [LanceDB Sink](lancedb-sink-zh.md)

## 通用约定

- 在 `CREATE STREAMING TABLE ... WITH (...) AS SELECT ...` 中通过 `WITH` 指定 `connector` 与 `format`。
- Sink 场景建议显式指定 `type='sink'`。
- 仅 `lanceDB` connector 允许 `format='lance'`；其余 Sink connector 不支持 `lance`。
- `format='json'` 的文件类 Sink 输出为 JSON Lines（NDJSON，`.jsonl`）。
