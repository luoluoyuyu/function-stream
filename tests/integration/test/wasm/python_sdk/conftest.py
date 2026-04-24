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

import concurrent.futures
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Generator, Set

_CURRENT_DIR = Path(__file__).resolve().parent
_INTEGRATION_ROOT = _CURRENT_DIR.parents[2]


def _inject_path_safely(target_path: Path) -> None:
    path_str = str(target_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        os.environ["PYTHONPATH"] = f"{path_str}{os.pathsep}{os.environ.get('PYTHONPATH', '')}"


_inject_path_safely(_INTEGRATION_ROOT)
_inject_path_safely(_CURRENT_DIR)

import pytest
from framework import FunctionStreamInstance, KafkaDockerManager
from fs_client.client import FsClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def kafka() -> Generator[KafkaDockerManager, None, None]:
    with KafkaDockerManager() as mgr:
        yield mgr


@pytest.fixture(scope="session")
def kafka_topics(kafka: KafkaDockerManager) -> str:
    kafka.create_topics_if_not_exist(["in", "out", "events", "counts"])
    return kafka.config.bootstrap_servers


@pytest.fixture(scope="session")
def minio() -> Generator[Any, None, None]:
    try:
        from framework import MinioDockerManager
    except ModuleNotFoundError as exc:
        pytest.skip(f"MinIO tests require optional dependency: {exc}")
    with MinioDockerManager() as mgr:
        yield mgr


def _sanitize_segment(segment: str) -> str:
    clean = re.sub(r"[^\w\-]+", "_", segment).strip("_")
    return clean or "unknown"


def _nodeid_to_workspace_path(nodeid: str) -> str:
    parts = nodeid.split("::")
    file_part = Path(parts[0]).with_suffix("")
    file_segments = [_sanitize_segment(seg) for seg in file_part.parts]
    extra_segments = [_sanitize_segment(seg) for seg in parts[1:]]
    return str(Path(*file_segments, *extra_segments))


@pytest.fixture
def fs_server(request: pytest.FixtureRequest) -> Generator[FunctionStreamInstance, None, None]:
    test_name = _nodeid_to_workspace_path(request.node.nodeid)
    with FunctionStreamInstance(test_name=test_name) as instance:
        yield instance


@pytest.fixture
def fs_client(fs_server: FunctionStreamInstance) -> Generator[FsClient, None, None]:
    with fs_server.get_client() as client:
        yield client


class FunctionTracker:
    def __init__(self, client: FsClient):
        self._client = client
        self._registered: Set[str] = set()

    def __contains__(self, name: str) -> bool:
        return name in self._registered

    def append(self, name: str) -> None:
        self._registered.add(name)

    def extend(self, names) -> None:
        for name in names:
            self._registered.add(name)

    def remove(self, name: str) -> None:
        self._registered.discard(name)

    def register(self, name: str) -> None:
        self._registered.add(name)

    def _teardown_single_function(self, name: str) -> None:
        try:
            self._client.stop_function(name)
        except Exception as e:
            logger.debug("Ignored stop error for '%s': %s", name, e)

        try:
            self._client.drop_function(name)
        except Exception as e:
            logger.debug("Ignored drop error for '%s': %s", name, e)

    def teardown_all(self) -> None:
        if not self._registered:
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(self._teardown_single_function, self._registered))

        self._registered.clear()


@pytest.fixture
def function_registry(fs_client: FsClient) -> Generator[FunctionTracker, None, None]:
    tracker = FunctionTracker(fs_client)
    yield tracker
    tracker.teardown_all()