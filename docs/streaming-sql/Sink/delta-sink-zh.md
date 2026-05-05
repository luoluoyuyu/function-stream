# Delta Sink

`delta` connector 对应 Delta 数据湖写出通道。

## 支持格式

- `csv`
- `parquet`
- `json`（写出为 JSON Lines / NDJSON，文件后缀 `.jsonl`）
- `avro`
- `orc`

## 常用 WITH 参数

- `connector='delta'`
- `type='sink'`
- `format='csv'|'parquet'|'json'|'avro'|'orc'`
- `path='/data/delta/orders'`（本地）或对象存储前缀
- 可选 S3 参数：`s3.bucket` / `s3.region` / `s3.endpoint` / AKSK
- `parquet.compression`（仅 `parquet`）

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_delta_parquet
WITH (
  connector='delta',
  type='sink',
  format='parquet',
  path='/tmp/delta_orders'
) AS
SELECT * FROM src_kafka_orders;
```
