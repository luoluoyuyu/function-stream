# Filesystem Sink

`filesystem` 用于将流数据落到本地文件系统目录。

## 支持格式

- `csv`
- `parquet`
- `json`（写出为 JSON Lines / NDJSON，文件后缀 `.jsonl`）
- `avro`
- `orc`

## 常用 WITH 参数

- `connector='filesystem'`
- `type='sink'`
- `format='csv'|'parquet'|'json'|'avro'|'orc'`
- `path='/path/to/output'`（或 `sink.path`）
- `parquet.compression`（仅 `parquet` 生效）

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_fs_parquet
WITH (
  connector='filesystem',
  type='sink',
  format='parquet',
  path='/tmp/fs_orders',
  'parquet.compression'='zstd'
) AS
SELECT * FROM src_kafka_orders;
```
