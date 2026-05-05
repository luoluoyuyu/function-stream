# Iceberg Sink

`iceberg` connector 对应 Iceberg 数据湖写出通道。

## 支持格式

- `csv`
- `parquet`

## 常用 WITH 参数

- `connector='iceberg'`
- `type='sink'`
- `format='csv'|'parquet'`
- `path='/data/iceberg/orders'` 或对象存储前缀
- 可选 S3 参数：`s3.bucket` / `s3.region` / `s3.endpoint` / AKSK
- `parquet.compression`（仅 `parquet`）

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_iceberg_parquet
WITH (
  connector='iceberg',
  type='sink',
  format='parquet',
  path='/tmp/iceberg_orders',
  'parquet.compression'='zstd'
) AS
SELECT * FROM src_kafka_orders;
```
