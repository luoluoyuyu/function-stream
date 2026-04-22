# Streaming SQL Connector 文档

本目录提供 Streaming SQL 的 Source / Sink 专项文档，推荐配合 `CREATE STREAMING TABLE ... AS SELECT ...` 使用。

## 目录

- [Source 文档](Source/README-zh.md)
- [Sink 文档](Sink/README-zh.md)

## 使用建议

1. 先用 `CREATE TABLE ... WITH (...)` 注册 Source（当前仅 Kafka）。
2. 再用 `CREATE STREAMING TABLE ... WITH (...) AS SELECT ...` 创建持续运行的 Pipeline 并写入 Sink。
