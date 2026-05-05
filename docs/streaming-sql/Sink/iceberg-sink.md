# Iceberg Sink

`iceberg` sink is the Iceberg Lakehouse write path.

## Supported formats

- `csv`
- `parquet`

## Common `WITH` options

- `connector='iceberg'`
- `type='sink'`
- `format='csv'|'parquet'`
- `path='/data/iceberg/orders'` or object-store prefix
- Optional S3 options: `s3.bucket` / `s3.region` / `s3.endpoint` / access keys
- `parquet.compression` (only for `parquet`)

