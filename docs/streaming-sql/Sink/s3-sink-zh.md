# S3 Sink

`s3` 用于将流数据写入对象存储（AWS S3 / S3 兼容存储）。

## 支持格式

- `csv`
- `parquet`

## 常用 WITH 参数

- `connector='s3'`
- `type='sink'`
- `format='csv'|'parquet'`
- `path='prefix/path'`
- `s3.bucket='your-bucket'`
- `s3.region='us-east-1'`
- `s3.endpoint='http://minio:9000'`（可选，S3 兼容）
- `s3.access_key_id` / `s3.secret_access_key` / `s3.session_token`（可选）
- `parquet.compression`（仅 `parquet`）

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_s3_csv
WITH (
  connector='s3',
  type='sink',
  format='csv',
  path='streaming/orders',
  's3.bucket'='fs-dev',
  's3.region'='us-east-1'
) AS
SELECT * FROM src_kafka_orders;
```
