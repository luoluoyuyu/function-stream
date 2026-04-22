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

"""
Docker-managed MinIO service for integration tests.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
import docker
import requests
from docker.errors import APIError, DockerException, NotFound

from .utils import find_free_port

logger = logging.getLogger(__name__)


class MinioDockerManagerError(Exception):
    """Base exception for MinioDockerManager errors."""


@dataclass(frozen=True)
class MinioConfig:
    image: str = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
    container_name: str = "fs-integration-minio"
    root_user: str = "minioadmin"
    root_password: str = "minioadmin"
    api_host: str = "127.0.0.1"
    api_port: int = 9000
    console_port: int = 9001
    readiness_timeout_sec: int = 60

    @property
    def endpoint_url(self) -> str:
        return f"http://{self.api_host}:{self.api_port}"


class MinioDockerManager:
    def __init__(
        self,
        config: Optional[MinioConfig] = None,
        docker_client: Optional[docker.DockerClient] = None,
    ) -> None:
        if config is None:
            config = MinioConfig(api_port=find_free_port(), console_port=find_free_port())
        self.config = config
        self._docker_client = docker_client

    @property
    def docker_client(self) -> docker.DockerClient:
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
            except DockerException as e:
                raise MinioDockerManagerError(f"Failed to connect to Docker daemon: {e}") from e
        return self._docker_client

    @property
    def s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.root_user,
            aws_secret_access_key=self.config.root_password,
            region_name="us-east-1",
        )

    def __enter__(self) -> "MinioDockerManager":
        self.setup_minio()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.teardown_minio()

    def setup_minio(self) -> None:
        self._ensure_image()
        self._ensure_container()
        self._wait_for_readiness()
        logger.info("MinIO ready at %s", self.config.endpoint_url)

    def teardown_minio(self) -> None:
        try:
            container = self.docker_client.containers.get(self.config.container_name)
            container.stop(timeout=5)
        except NotFound:
            logger.debug("MinIO container not found during teardown.")
        except APIError as exc:
            logger.warning("Docker API error while stopping MinIO: %s", exc)

    def create_bucket_if_not_exists(self, bucket: str) -> None:
        client = self.s3_client
        buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
        if bucket not in buckets:
            client.create_bucket(Bucket=bucket)

    def clear_bucket(self, bucket: str) -> None:
        client = self.s3_client
        token = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": bucket}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            contents = resp.get("Contents", [])
            if contents:
                for item in contents:
                    client.delete_object(Bucket=bucket, Key=item["Key"])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

    def _ensure_image(self) -> None:
        try:
            self.docker_client.images.get(self.config.image)
        except NotFound:
            logger.info("Pulling MinIO image '%s'...", self.config.image)
            self.docker_client.images.pull(self.config.image)

    def _ensure_container(self) -> None:
        try:
            container = self.docker_client.containers.get(self.config.container_name)
            if container.status != "running":
                container.start()
            return
        except NotFound:
            pass

        self.docker_client.containers.run(
            image=self.config.image,
            name=self.config.container_name,
            command='server /data --console-address ":9001"',
            ports={"9000/tcp": self.config.api_port, "9001/tcp": self.config.console_port},
            environment={
                "MINIO_ROOT_USER": self.config.root_user,
                "MINIO_ROOT_PASSWORD": self.config.root_password,
            },
            detach=True,
            remove=True,
        )

    def _wait_for_readiness(self) -> None:
        deadline = time.time() + self.config.readiness_timeout_sec
        url = f"{self.config.endpoint_url}/minio/health/ready"
        while time.time() < deadline:
            try:
                r = requests.get(url, timeout=1.5)
                if r.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(1.0)
        raise MinioDockerManagerError(
            f"MinIO not ready within {self.config.readiness_timeout_sec}s ({url})"
        )
