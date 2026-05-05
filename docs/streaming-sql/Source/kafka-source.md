# Kafka Source

`kafka` source is used to ingest streaming records from Kafka topics.

## Supported formats

- `json`
- `raw_string`
- `raw_bytes`

## Common `WITH` options

- `connector='kafka'`
- `topic='topic_name'`
- `bootstrap.servers='host:9092'`
- `format='json'|'raw_string'|'raw_bytes'`
- `scan.startup.mode='earliest'|'latest'` (optional)
- `group.id='consumer_group'` (optional)

