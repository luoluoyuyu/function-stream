# Kafka Source

`kafka` source 用于从 Kafka topic 持续消费数据。

## 支持格式

- `json`
- `raw_string`
- `raw_bytes`

## 常用 WITH 参数

- `connector='kafka'`
- `type='source'`（默认）
- `topic='topic_in'`
- `bootstrap.servers='host:9092'`
- `group.id='consumer_group'`
- `format='json'|'raw_string'|'raw_bytes'`
- `scan.startup.mode='earliest'|'latest'|'group-offsets'`（可选）

## 示例

```sql
CREATE TABLE src_kafka_json (
  user_id BIGINT,
  event STRING,
  ts TIMESTAMP
) WITH (
  connector='kafka',
  type='source',
  topic='topic_in',
  'bootstrap.servers'='127.0.0.1:9092',
  'group.id'='fs_demo',
  format='json'
);
```
