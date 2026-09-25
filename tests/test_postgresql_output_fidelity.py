"""Rows must reach the caller exactly as PostgreSQL returned them.

The psql connector used to parse psql's chatty output and drop lines that
looked like command tags or the row-count footer, which also dropped data rows
with those values and a trailing NULL row. The Python connector used a dict
cursor, which collapsed duplicate column names. These tests pin the fixed
behaviour for both implementations.
"""

import asyncio

import pytest

from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from tests.test_postgresql_cli_fallback import DummyProcess

# Values that the old psql output filter mistook for psql chatter.
CHATTER_LOOKALIKE_ROWS = ["SET", "DO", "BEGIN", "COMMIT", "ROLLBACK", "(1 row)", ""]


@pytest.mark.anyio
async def test_psql_stdout_lines_are_all_data(postgres_config, monkeypatch):
    """No stdout line is filtered: with -q and footer=off, psql prints only data."""

    lines = ["x\n"] + [f"{value}\n" for value in CHATTER_LOOKALIKE_ROWS]

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return DummyProcess(lines, returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = await PostgreSQLCLIConnector(postgres_config).execute_query(
        "SELECT x FROM t"
    )

    assert result.split("\n") == ["x", *CHATTER_LOOKALIKE_ROWS]


def _connector(implementation: str, config):
    if implementation == "cli":
        return PostgreSQLCLIConnector(config)
    return PostgreSQLPythonConnector(config)


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT 'SET' AS x", "x\nSET"),
        ("SELECT 'DO' AS x", "x\nDO"),
        ("SELECT '(1 row)' AS x", "x\n(1 row)"),
        ("SELECT 1 AS a, 2 AS a", "a\ta\n1\t2"),
    ],
)
async def test_real_rows_are_returned_verbatim(
    implementation, query, expected, postgres_config
):
    result = await _connector(implementation, postgres_config).execute_query(query)
    assert result == expected


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("implementation", ["cli", "python"])
async def test_real_trailing_null_row_is_kept(implementation, postgres_config):
    """A last row that renders empty (NULL in the only column) is still a row."""
    result = await _connector(implementation, postgres_config).execute_query(
        "SELECT v FROM (VALUES ('a'), (NULL)) AS t(v) ORDER BY v NULLS LAST"
    )
    lines = result.split("\n")
    assert lines[:2] == ["v", "a"]
    assert len(lines) == 3
    # psql renders the NULL as an empty line; the csv-based formatter writes
    # a lone empty field as "" so the line is not mistaken for no row at all.
    assert lines[2] in ("", '""')
