# LanceDB Sink

`lanceDB` 是 Lance 数据集专用写出 connector。

## 支持格式

- 仅支持 `lance`

## 常用 WITH 参数

- `connector='lanceDB'`
- `type='sink'`
- `format='lance'`
- `path='/data/lance/orders'`（本地目录）或对象存储前缀
- 可选 S3 参数：`s3.bucket` / `s3.region` / `s3.endpoint` / AKSK

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_lancedb
WITH (
  connector='lanceDB',
  type='sink',
  format='lance',
  path='/tmp/lance_orders'
) AS
SELECT * FROM src_kafka_orders;
```
