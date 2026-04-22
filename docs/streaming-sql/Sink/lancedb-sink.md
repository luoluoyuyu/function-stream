# LanceDB Sink

`lanceDB` is a dedicated sink connector for Lance datasets.

## Supported formats

- `lance` only

## Common `WITH` options

- `connector='lanceDB'`
- `type='sink'`
- `format='lance'`
- `path='/data/lance/orders'` (local directory) or object-store prefix
- Optional S3 options: `s3.bucket` / `s3.region` / `s3.endpoint` / access keys

