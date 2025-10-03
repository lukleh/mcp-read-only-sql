"""Unified timeout and size-limit regression tests."""

import warnings

import shutil
import time
from typing import Dict, Tuple

import pytest

from conftest import make_connection
from src.connectors.clickhouse.cli import ClickHouseCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.utils.timeout_wrapper import HardTimeoutError

pytestmark = [pytest.mark.docker, pytest.mark.usefixtures("docker_check")]

warnings.filterwarnings("ignore", category=pytest.PytestUnraisableExceptionWarning)

CONNECTOR_CLASSES: Dict[Tuple[str, str], type] = {
    ("postgresql", "python"): PostgreSQLPythonConnector,
    ("postgresql", "cli"): PostgreSQLCLIConnector,
    ("clickhouse", "python"): ClickHousePythonConnector,
    ("clickhouse", "cli"): ClickHouseCLIConnector,
}

DEFAULT_PORT = {
    "postgresql": 5432,
    "clickhouse": 9000,
}


def ensure_cli_available(db_type: str, implementation: str) -> None:
    """Skip tests gracefully when CLI tools are missing."""
    if implementation != "cli":
        return
    tool = "psql" if db_type == "postgresql" else "clickhouse-client"
    if shutil.which(tool) is None:
        pytest.skip(f"{tool} command not available on PATH")


def build_connector(db_type: str, implementation: str, **overrides):
    """Construct a connector for the given database/implementation pair."""
    servers = overrides.pop(
        "servers",
        [{"host": overrides.pop("host", "localhost"), "port": overrides.pop("port", DEFAULT_PORT[db_type])}],
    )
    connection_name = overrides.pop(
        "connection_name", f"{db_type}_{implementation}_limits"
    )
    config = {
        "connection_name": connection_name,
        "type": db_type,
        "implementation": implementation,
        "servers": servers,
        "db": overrides.pop("db", "testdb"),
        "username": overrides.pop("username", "testuser"),
        "password": overrides.pop("password", "testpass"),
    }
    config.update(overrides)

    hard_timeout_override = config.pop("hard_timeout", None)
    connection = make_connection(config)
    connector = CONNECTOR_CLASSES[(db_type, implementation)](connection)

    if hard_timeout_override is not None:
        connector.hard_timeout = hard_timeout_override

    return connector


QUERY_TIMEOUT_CASES = [
    ("postgresql", "python", "SELECT pg_sleep(3)"),
    ("postgresql", "cli", "SELECT pg_sleep(3)"),
    ("clickhouse", "python", "SELECT sleep(3)"),
    ("clickhouse", "cli", "SELECT sleep(3)"),
]

QUERY_TIMEOUT_IDS = [f"{db}-{impl}" for db, impl, _ in QUERY_TIMEOUT_CASES]


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
@pytest.mark.parametrize("db_type, implementation, query", QUERY_TIMEOUT_CASES, ids=QUERY_TIMEOUT_IDS)
async def test_query_timeouts_are_enforced(db_type, implementation, query):
    ensure_cli_available(db_type, implementation)
    connector = build_connector(
        db_type,
        implementation,
        connection_name=f"{db_type}_{implementation}_timeout",
        query_timeout=2,
        connection_timeout=2,
    )

    start = time.time()
    with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
        await connector.execute_query_with_timeout(query)
    elapsed = time.time() - start

    assert "timeout" in str(exc_info.value).lower()
    assert elapsed < 3.5


NORMAL_QUERY_CASES = [
    ("postgresql", "python", "SELECT COUNT(*) as count FROM users"),
    ("postgresql", "cli", "SELECT 1 as test"),
    ("clickhouse", "python", "SELECT 1 as test"),
    ("clickhouse", "cli", "SELECT 1 as test"),
]

NORMAL_QUERY_IDS = [f"{db}-{impl}" for db, impl, _ in NORMAL_QUERY_CASES]


@pytest.mark.docker
@pytest.mark.anyio
@pytest.mark.parametrize("db_type, implementation, query", NORMAL_QUERY_CASES, ids=NORMAL_QUERY_IDS)
async def test_normal_queries_succeed(db_type, implementation, query):
    ensure_cli_available(db_type, implementation)
    connector = build_connector(
        db_type,
        implementation,
        connection_name=f"{db_type}_{implementation}_normal",
        query_timeout=10,
        connection_timeout=5,
    )

    result = await connector.execute_query_with_timeout(query)
    assert isinstance(result, str)
    assert len(result.strip().split('\n')) >= 2


SIZE_LIMIT_CASES = [
    {
        "ids": "postgresql-python",
        "db_type": "postgresql",
        "implementation": "python",
        "limit": 10_000,
        "small_query": "SELECT id, username FROM users LIMIT 5",
        "large_query": "SELECT * FROM users, products",
    },
    {
        "ids": "postgresql-cli",
        "db_type": "postgresql",
        "implementation": "cli",
        "limit": 1_000,
        "small_query": "SELECT id FROM users LIMIT 1",
        "large_query": "SELECT * FROM users, products",
    },
    {
        "ids": "clickhouse-python",
        "db_type": "clickhouse",
        "implementation": "python",
        "limit": 10_000,
        "small_query": "SELECT 1 as num",
        "large_query": "SELECT * FROM testdb.events LIMIT 1000",
    },
    {
        "ids": "clickhouse-cli",
        "db_type": "clickhouse",
        "implementation": "cli",
        "limit": 1_000,
        "small_query": "SELECT 1 as num",
        "large_query": "SELECT * FROM testdb.events LIMIT 100",
    },
]


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
@pytest.mark.parametrize("case", SIZE_LIMIT_CASES, ids=lambda c: c["ids"])
async def test_size_limits_allow_small_results(case):
    ensure_cli_available(case["db_type"], case["implementation"])
    connector = build_connector(
        case["db_type"],
        case["implementation"],
        connection_name=f"{case['ids']}_size_small",
        max_result_bytes=case["limit"],
        query_timeout=10,
        connection_timeout=5,
    )

    result = await connector.execute_query(case["small_query"])
    assert isinstance(result, str)
    lines = result.strip().split('\n')
    # Always expect header + at least one row
    assert len(lines) >= 2


@pytest.mark.security
@pytest.mark.docker
@pytest.mark.anyio
@pytest.mark.parametrize("case", SIZE_LIMIT_CASES, ids=lambda c: c["ids"])
async def test_size_limits_block_or_truncate_large_results(case):
    ensure_cli_available(case["db_type"], case["implementation"])
    connector = build_connector(
        case["db_type"],
        case["implementation"],
        connection_name=f"{case['ids']}_size_large",
        max_result_bytes=case["limit"],
        query_timeout=10,
        connection_timeout=5,
    )

    try:
        result = await connector.execute_query(case["large_query"])
        assert isinstance(result, str)
        assert len(result.encode()) <= int(case["limit"] * 1.5)
    except RuntimeError as exc:
        error_msg = str(exc).lower()
        assert any(keyword in error_msg for keyword in ("size", "exceed"))


@pytest.mark.docker
@pytest.mark.anyio
async def test_hard_timeout_is_enforced():
    connector = build_connector(
        "postgresql",
        "python",
        connection_name="hard_timeout_override",
        query_timeout=120,
        connection_timeout=1,
        hard_timeout=2,
    )

    start = time.time()
    with pytest.raises(HardTimeoutError) as exc_info:
        await connector.execute_query_with_timeout("SELECT pg_sleep(5)")
    elapsed = time.time() - start

    assert "hard timeout" in str(exc_info.value).lower()
    assert elapsed < 3


UNREACHABLE_HOST_CASES = [
    (
        "postgresql",
        "python",
        {"servers": [{"host": "192.0.2.10", "port": 5432}], "connection_timeout": 1, "query_timeout": 2},
    ),
    (
        "postgresql",
        "cli",
        {"servers": [{"host": "192.0.2.10", "port": 5432}], "connection_timeout": 1, "hard_timeout": 3},
    ),
    (
        "clickhouse",
        "python",
        {"servers": [{"host": "198.51.100.10", "port": 9000}], "connection_timeout": 1, "query_timeout": 2},
    ),
    (
        "clickhouse",
        "cli",
        {"servers": [{"host": "198.51.100.10", "port": 9000}], "connection_timeout": 1, "hard_timeout": 3},
    ),
]


UNREACHABLE_IDS = [f"{db}-{impl}" for db, impl, _ in UNREACHABLE_HOST_CASES]


@pytest.mark.docker
@pytest.mark.anyio
@pytest.mark.parametrize("db_type, implementation, overrides", UNREACHABLE_HOST_CASES, ids=UNREACHABLE_IDS)
async def test_unreachable_hosts_timeout(db_type, implementation, overrides):
    ensure_cli_available(db_type, implementation)
    connector = build_connector(
        db_type,
        implementation,
        connection_name=f"{db_type}_{implementation}_unreachable",
        **overrides,
    )

    start = time.time()
    with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
        await connector.execute_query_with_timeout("SELECT 1")
    elapsed = time.time() - start

    message = str(exc_info.value).lower()
    assert any(keyword in message for keyword in ("connect", "timeout", "refused", "unreach"))
    assert elapsed < 5
