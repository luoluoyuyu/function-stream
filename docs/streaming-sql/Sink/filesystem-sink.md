# Filesystem Sink

`filesystem` sink writes streaming data to local files.

## Supported formats

- `csv`
- `parquet`
- `json` (written as JSON Lines / NDJSON, `.jsonl`)
- `avro`
- `orc`

## Common `WITH` options

- `connector='filesystem'`
- `type='sink'`
- `format='csv'|'parquet'|'json'|'avro'|'orc'`
- `path='/path/to/output'` (or `sink.path`)
- `parquet.compression` (only for `parquet`)

