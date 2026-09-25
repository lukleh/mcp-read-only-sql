"""The clickhouse-client connector must return the last row even when empty."""

import asyncio

import pytest

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from tests.test_clickhouse_readonly_profiles import _Process


@pytest.mark.anyio
async def test_cli_keeps_trailing_empty_line(clickhouse_config, monkeypatch):
    async def fake_exec(*cmd, **kwargs):
        # Served to the settings probe and to the query alike.
        return _Process(["x\n", "a\n", "\n"])

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    result = await ClickHouseCLIConnector(clickhouse_config).execute_query(
        "SELECT x FROM t"
    )

    assert result.split("\n") == ["x", "a", ""]


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
async def test_real_trailing_empty_row_is_kept(implementation, clickhouse_config):
    connector = (
        ClickHouseCLIConnector(clickhouse_config)
        if implementation == "cli"
        else ClickHousePythonConnector(clickhouse_config)
    )

    result = await connector.execute_query("SELECT 'a' AS x UNION ALL SELECT ''")

    lines = result.split("\n")
    assert lines[:2] == ["x", "a"]
    assert len(lines) == 3
    # clickhouse-client prints the empty string as an empty line; the
    # csv-based formatter writes a lone empty field as "".
    assert lines[2] in ("", '""')
