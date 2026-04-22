# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import datetime as dt
import io
import json
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import lance
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dateutil import parser as dt_parser

from .test_data_flow import produce_messages


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _bucket_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}".lower()


def _sql_ok(fs_server: Any, sql: str) -> Any:
    resp = fs_server.execute_sql(sql)
    assert resp.status_code == 200, f"SQL failed: {sql}\nstatus={resp.status_code}\nmsg={resp.message}"
    return resp


def _sql_drop_streaming_table(fs_server: Any, table_name: str) -> None:
    resp = fs_server.execute_sql(f"DROP STREAMING TABLE {table_name};")
    if resp.status_code == 200:
        return
    msg = (resp.message or "").lower()
    if "not found" in msg or "does not exist" in msg:
        return
    raise AssertionError(
        f"SQL failed: DROP STREAMING TABLE {table_name};\n"
        f"status={resp.status_code}\nmsg={resp.message}"
    )


def _create_source(fs_server: Any, source_name: str, in_topic: str, bootstrap: str) -> None:
    _sql_ok(
        fs_server,
        f"""
        CREATE TABLE {source_name} (
            id BIGINT,
            value VARCHAR,
            ts TIMESTAMP NOT NULL,
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{in_topic}',
            'format' = 'json',
            'scan.startup.mode' = 'earliest',
            'bootstrap.servers' = '{bootstrap}'
        );
        """,
    )


def _create_sink_streaming_table(
    fs_server: Any,
    stream_name: str,
    connector: str,
    format_name: str,
    with_extra: Dict[str, str],
    source_name: str,
    select_expr: str = "id, value, ts",
) -> None:
    with_pairs = {
        "connector": connector,
        "type": "sink",
        "format": format_name,
        "checkpoint.interval.ms": "1000",
        **with_extra,
    }
    with_sql = ",\n".join([f"'{k}' = '{v}'" for k, v in with_pairs.items()])
    _sql_ok(
        fs_server,
        f"""
        CREATE STREAMING TABLE {stream_name} WITH (
            {with_sql}
        ) AS
        SELECT {select_expr} FROM {source_name};
        """,
    )


def _publish_rows(kafka_bootstrap: str, topic: str, rows: List[Dict[str, Any]]) -> None:
    produce_messages(kafka_bootstrap, topic, [json.dumps(r) for r in rows])


def _sample_rows() -> List[Dict[str, Any]]:
    now = dt.datetime.now(dt.timezone.utc)
    return [
        {"id": 1, "value": "alpha", "ts": (now - dt.timedelta(seconds=2)).isoformat()},
        {"id": 2, "value": "beta", "ts": (now - dt.timedelta(seconds=1)).isoformat()},
        {"id": 3, "value": "gamma", "ts": now.isoformat()},
    ]


def _parse_timestamp(val: Any) -> float:
    if isinstance(val, dt.datetime):
        return val.timestamp()
    if isinstance(val, str):
        return dt_parser.isoparse(val).timestamp()
    raise TypeError(f"Unknown timestamp type: {type(val)}")


def _assert_data_integrity(actual_rows: List[Dict[str, Any]], expected_rows: List[Dict[str, Any]]) -> None:
    assert len(actual_rows) >= len(expected_rows), f"Expected at least {len(expected_rows)} rows, got {len(actual_rows)}"

    actual_mapped = {str(r["id"]): r for r in actual_rows}
    for expected in expected_rows:
        exp_id = str(expected["id"])
        assert exp_id in actual_mapped, f"Data Loss: Missing row with id {exp_id}"

        actual = actual_mapped[exp_id]
        if "value" in expected:
            assert str(actual.get("value")) == str(expected["value"]), f"Data Corruption: Value mismatch for id {exp_id}"

        assert "ts" in actual, f"Data Corruption: Missing timestamp column in output for id {exp_id}"

        expected_ts = _parse_timestamp(expected["ts"])
        actual_ts = _parse_timestamp(actual["ts"])
        assert abs(actual_ts - expected_ts) < 1.0, f"Timestamp drift too large for id {exp_id}: expected {expected_ts}, got {actual_ts}"


def _wait_and_verify_local_csv(dir_path: Path, expected_count: int, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        files = list(dir_path.glob("*.csv"))
        if files:
            merged: List[Dict[str, Any]] = []
            for f in files:
                try:
                    with f.open("r", encoding="utf-8") as fp:
                        merged.extend(list(csv.DictReader(fp)))
                except Exception:
                    pass
            if len(merged) >= expected_count:
                return merged
        time.sleep(1.0)
    raise TimeoutError(f"Failed to verify {expected_count} rows in local CSV at {dir_path}")


def _wait_and_verify_local_parquet(dir_path: Path, expected_count: int, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        files = list(dir_path.glob("**/*.parquet"))
        if files:
            try:
                tables = [pq.read_table(f) for f in files]
                if tables:
                    combined = pa.concat_tables(tables)
                    if combined.num_rows >= expected_count:
                        return combined.to_pylist()
            except Exception:
                pass
        time.sleep(1.0)
    raise TimeoutError(f"Failed to verify {expected_count} rows in local Parquet at {dir_path}")


def _wait_and_verify_local_lance(dir_path: Path, expected_count: int, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            ds = lance.dataset(dir_path.as_posix())
            if ds.count_rows() >= expected_count:
                return ds.to_table().to_pylist()
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"Failed to verify {expected_count} rows in local Lance dataset at {dir_path}")


def _wait_and_verify_s3_parquet(minio: Any, bucket: str, prefix: str, expected_count: int, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_s
    client = minio.s3_client
    while time.time() < deadline:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
        if keys:
            try:
                tables = []
                for k in keys:
                    body = client.get_object(Bucket=bucket, Key=k)["Body"].read()
                    tables.append(pq.read_table(io.BytesIO(body)))
                if tables:
                    combined = pa.concat_tables(tables)
                    if combined.num_rows >= expected_count:
                        return combined.to_pylist()
            except Exception:
                pass
        time.sleep(1.0)
    raise TimeoutError(f"Failed to verify {expected_count} rows in S3 Parquet at s3://{bucket}/{prefix}")


def _wait_and_verify_s3_lance(minio: Any, bucket: str, prefix: str, expected_count: int, timeout_s: float = 35.0) -> List[Dict[str, Any]]:
    uri = f"s3://{bucket}/{prefix}"
    deadline = time.time() + timeout_s
    storage_options = {
        "endpoint": minio.config.endpoint_url,
        "access_key_id": minio.config.root_user,
        "secret_access_key": minio.config.root_password,
        "region": "us-east-1",
        "allow_http": "true",
    }
    while time.time() < deadline:
        try:
            ds = lance.dataset(uri, storage_options=storage_options)
            if ds.count_rows() >= expected_count:
                return ds.to_table().to_pylist()
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"Failed to verify {expected_count} rows in S3 Lance dataset at {uri}")


class TestStreamingSqlSinks:
    def test_filesystem_csv_sink(self, fs_server: Any, kafka: Any, kafka_topics: str) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_fs_csv")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        temp_dir = Path(tempfile.mkdtemp(prefix="fs_sink_csv_"))
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="filesystem",
                format_name="csv",
                with_extra={"path": temp_dir.as_posix()},
                source_name=source_name,
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_local_csv(temp_dir, len(expected_data))
            _assert_data_integrity(actual_data, expected_data)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_s3_parquet_sink(self, fs_server: Any, kafka: Any, kafka_topics: str, minio: Any) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_s3_pq")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        bucket = _bucket_name("sink-bucket")
        prefix = f"sinks/{stream_name}"
        minio.create_bucket_if_not_exists(bucket)
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="s3",
                format_name="parquet",
                with_extra={
                    "path": prefix,
                    "s3.bucket": bucket,
                    "s3.region": "us-east-1",
                    "s3.endpoint": minio.config.endpoint_url,
                    "s3.access_key_id": minio.config.root_user,
                    "s3.secret_access_key": minio.config.root_password,
                },
                source_name=source_name,
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_s3_parquet(minio, bucket, prefix, len(expected_data))
            _assert_data_integrity(actual_data, expected_data)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            minio.clear_bucket(bucket)

    def test_delta_parquet_sink(self, fs_server: Any, kafka: Any, kafka_topics: str) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_delta_pq")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        temp_dir = Path(tempfile.mkdtemp(prefix="delta_sink_pq_"))
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="delta",
                format_name="parquet",
                with_extra={"path": temp_dir.as_posix()},
                source_name=source_name,
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_local_parquet(temp_dir, len(expected_data))
            _assert_data_integrity(actual_data, expected_data)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_iceberg_parquet_sink(self, fs_server: Any, kafka: Any, kafka_topics: str) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_iceberg_pq")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        temp_dir = Path(tempfile.mkdtemp(prefix="iceberg_sink_pq_"))
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="iceberg",
                format_name="parquet",
                with_extra={"path": temp_dir.as_posix()},
                source_name=source_name,
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_local_parquet(temp_dir, len(expected_data))
            _assert_data_integrity(actual_data, expected_data)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_lancedb_lance_sink(self, fs_server: Any, kafka: Any, kafka_topics: str) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_lancedb")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        temp_dir = Path(tempfile.mkdtemp(prefix="lancedb_sink_"))
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="lanceDB",
                format_name="lance",
                with_extra={"path": temp_dir.as_posix()},
                source_name=source_name,
                select_expr="id, ts",
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_local_lance(temp_dir, len(expected_data))
            expected_truncated = [{"id": r["id"], "ts": r["ts"]} for r in expected_data]
            _assert_data_integrity(actual_data, expected_truncated)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_lancedb_lance_sink_to_s3(
        self, fs_server: Any, kafka: Any, kafka_topics: str, minio: Any
    ) -> None:
        source_name = _uid("src")
        stream_name = _uid("st_lancedb_s3")
        in_topic = _uid("topic_in")
        kafka.create_topics_if_not_exist([in_topic])
        _create_source(fs_server, source_name, in_topic, kafka_topics)

        bucket = _bucket_name("lance-bucket")
        prefix = f"sinks/{stream_name}"
        minio.create_bucket_if_not_exists(bucket)
        s3_path = f"s3://{bucket}/{prefix}"
        try:
            _create_sink_streaming_table(
                fs_server,
                stream_name,
                connector="lanceDB",
                format_name="lance",
                with_extra={
                    "path": s3_path,
                    "s3.bucket": bucket,
                    "s3.region": "us-east-1",
                    "s3.endpoint": minio.config.endpoint_url,
                    "s3.access_key_id": minio.config.root_user,
                    "s3.secret_access_key": minio.config.root_password,
                },
                source_name=source_name,
                select_expr="id, ts",
            )
            expected_data = _sample_rows()
            _publish_rows(kafka_topics, in_topic, expected_data)
            actual_data = _wait_and_verify_s3_lance(minio, bucket, prefix, len(expected_data))
            expected_truncated = [{"id": r["id"], "ts": r["ts"]} for r in expected_data]
            _assert_data_integrity(actual_data, expected_truncated)
        finally:
            _sql_drop_streaming_table(fs_server, stream_name)
            minio.clear_bucket(bucket)
