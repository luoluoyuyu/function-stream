# Kafka Sink

`kafka` sink writes streaming output records to Kafka topics.

## Supported formats

- `json`
- `raw_string`
- `raw_bytes`

## Common `WITH` options

- `connector='kafka'`
- `type='sink'`
- `topic='topic_name'`
- `bootstrap.servers='host:9092'`
- `format='json'|'raw_string'|'raw_bytes'`
- `sink.commit.mode='at-least-once'|'exactly-once'` (optional)

## Example

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

