"""Connection metadata must remain parseable when values contain delimiters."""

import csv
import io

import pytest
from mcp.server.mcpserver import Context

from mcp_read_only_sql.config import Connection
from tests.test_run_query_file_output import (
    StubConnector,
    build_stub_server,
    make_runtime_paths,
)


@pytest.mark.anyio
async def test_connection_metadata_round_trips_delimiters(tmp_path):
    values = {
        "connection_name": 'report\t"production"',
        "description": "Read access\nfor analysts\r\nproduction\twarehouse",
        "username": 'user\t"name"',
        "db": 'data\t"base"',
    }
    connector = StubConnector(
        Connection(
            {
                **values,
                "type": "postgresql",
                "servers": ["localhost:5432"],
            }
        )
    )
    server = build_stub_server(connector, make_runtime_paths(tmp_path))
    result = await server.mcp._tool_manager.call_tool(
        "list_connections",
        {},
        Context(mcp_server=server.mcp, subscriptions=server.mcp._subscriptions),
        convert_result=False,
    )
    rows = list(csv.DictReader(io.StringIO(result, newline=""), delimiter="\t"))
    assert rows == [
        {
            "name": values["connection_name"],
            "type": "postgresql",
            "description": values["description"],
            "servers": "localhost",
            "database": values["db"],
            "databases": values["db"],
            "user": values["username"],
        }
    ]
