# Kafka Sink

`kafka` sink 用于将流数据写入 Kafka topic。

## 支持格式

- `json`
- `raw_string`
- `raw_bytes`

## 常用 WITH 参数

- `connector='kafka'`
- `type='sink'`
- `topic='topic_name'`
- `bootstrap.servers='host:9092'`
- `format='json'|'raw_string'|'raw_bytes'`
- `sink.commit.mode='at-least-once'|'exactly-once'`（可选）

## 示例（CREATE STREAMING TABLE）

```sql
CREATE STREAMING TABLE st_kafka_json
WITH (
  connector='kafka',
  type='sink',
  topic='topic_out',
  'bootstrap.servers'='127.0.0.1:9092',
  format='json'
) AS
SELECT * FROM src_kafka_orders;
```
