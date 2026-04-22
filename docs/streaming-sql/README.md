# Streaming SQL Connector Docs

This directory contains Source/Sink focused docs for Streaming SQL, intended to be used with `CREATE STREAMING TABLE ... AS SELECT ...`.

## Index

- [Source Docs](Source/README.md)
- [Sink Docs](Sink/README.md)

## Recommended workflow

1. Register sources using `CREATE TABLE ... WITH (...)` (currently Kafka source).
2. Build a continuous pipeline using `CREATE STREAMING TABLE ... WITH (...) AS SELECT ...` and write to sinks.

