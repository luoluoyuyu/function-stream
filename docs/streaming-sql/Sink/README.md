# Streaming SQL Sink Docs

This directory documents sink connectors for Streaming SQL.

## Support matrix

| Connector | Supported formats |
|---|---|
| `kafka` | `json` / `raw_string` / `raw_bytes` |
| `filesystem` | `csv` / `parquet` / `json`(JSONL) / `avro` / `orc` |
| `s3` | `csv` / `parquet` |
| `delta` | `csv` / `parquet` / `json`(JSONL) / `avro` / `orc` |
| `iceberg` | `csv` / `parquet` |
| `lanceDB` | `lance` |

## Documents

- [Kafka Sink](kafka-sink.md)
- [Filesystem Sink](filesystem-sink.md)
- [S3 Sink](s3-sink.md)
- [Delta Sink](delta-sink.md)
- [Iceberg Sink](iceberg-sink.md)
- [LanceDB Sink](lancedb-sink.md)

## Notes

- Configure sink connectors via `WITH (...)` in `CREATE STREAMING TABLE ... AS SELECT ...`.
- Use `type='sink'` explicitly for sink tables.
- Only `lanceDB` accepts `format='lance'`.
- For file-like sinks, `format='json'` is written as JSON Lines (NDJSON, `.jsonl`).

