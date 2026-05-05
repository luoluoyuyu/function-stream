# Delta Sink

`delta` sink is the Delta Lake write path.

## Supported formats

- `csv`
- `parquet`
- `json` (written as JSON Lines / NDJSON, `.jsonl`)
- `avro`
- `orc`

## Common `WITH` options

- `connector='delta'`
- `type='sink'`
- `format='csv'|'parquet'|'json'|'avro'|'orc'`
- `path='/data/delta/orders'` (local) or object-store prefix
- Optional S3 options: `s3.bucket` / `s3.region` / `s3.endpoint` / access keys
- `parquet.compression` (only for `parquet`)

