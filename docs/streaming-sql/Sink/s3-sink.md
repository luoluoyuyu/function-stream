# S3 Sink

`s3` sink writes streaming data to object storage (AWS S3 or S3-compatible services).

## Supported formats

- `csv`
- `parquet`

## Common `WITH` options

- `connector='s3'`
- `type='sink'`
- `format='csv'|'parquet'`
- `path='prefix/path'`
- `s3.bucket='your-bucket'`
- `s3.region='us-east-1'`
- `s3.endpoint='http://minio:9000'` (optional, for S3-compatible storage)
- `s3.access_key_id` / `s3.secret_access_key` / `s3.session_token` (optional)
- `parquet.compression` (only for `parquet`)

