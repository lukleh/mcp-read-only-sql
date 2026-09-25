"""Tests for the tool-boundary error translation under mcp SDK 2.x."""

import pytest
from mcp import MCPError
from mcp.server.mcpserver.exceptions import ToolError

from mcp_read_only_sql.errors import ConnectorError
from mcp_read_only_sql.server import ANTICIPATED_TOOL_ERRORS, _surface_tool_errors
from mcp_read_only_sql.utils.timeout_wrapper import HardTimeoutError


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("Connection 'alpha' not found"),
        ConnectorError("PostgreSQL: connection refused"),
        TimeoutError("ClickHouse: Operation exceeded combined timeout"),
        OSError("psql binary missing"),
        HardTimeoutError("Operation exceeded hard timeout"),
        FileExistsError("Could not allocate a unique managed result file"),
    ],
)
def test_anticipated_failures_become_tool_errors(exc):
    """Operational failures keep their text and their cause."""
    with pytest.raises(ToolError, match=str(exc)) as info, _surface_tool_errors():
        raise exc

    assert info.value.__cause__ is exc


def test_anticipated_list_matches_parametrized_cases():
    """Guard the allow-list against silent drift."""
    assert ANTICIPATED_TOOL_ERRORS == (
        ValueError,
        ConnectorError,
        TimeoutError,
        OSError,
        HardTimeoutError,
    )


def test_empty_message_falls_back_to_class_name():
    """A bare exception still yields a readable result, not a trailing colon."""
    with pytest.raises(ToolError, match="^TimeoutError$"), _surface_tool_errors():
        raise TimeoutError


@pytest.mark.parametrize(
    "exc_type", [TypeError, AttributeError, KeyError, RuntimeError]
)
def test_programming_errors_keep_sdk_crash_handling(exc_type):
    """Bugs propagate unchanged so the SDK masks the text and logs the traceback.

    A plain RuntimeError counts as a bug: connectors raise ConnectorError for
    every failure they wrap on purpose.
    """
    with pytest.raises(exc_type), _surface_tool_errors():
        raise exc_type("internal detail")


def test_tool_error_passes_through_unchanged():
    """No double prefix when the body already raised a ToolError."""
    original = ToolError("already anticipated")
    with pytest.raises(ToolError) as info, _surface_tool_errors():
        raise original

    assert info.value is original


def test_protocol_error_passes_through_unchanged():
    """MCPError is a protocol error and must not become an is_error result."""
    original = MCPError(-32600, "invalid request")
    with pytest.raises(MCPError) as info, _surface_tool_errors():
        raise original

    assert info.value is original
