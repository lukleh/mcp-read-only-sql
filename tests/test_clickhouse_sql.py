"""ClickHouse client output policy respects lexical SQL boundaries."""

import pytest

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.clickhouse.sql import (
    sanitize_clickhouse_read_only_sql,
)
from mcp_read_only_sql.utils.sql_guard import ReadOnlyQueryError


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FORMAT JSON", "SELECT 1 format /* comment */ JSON;",
        "SELECT count() FROM t FORMAT CSV SETTINGS x=1",
        "SELECT 1 // comment\nFORMAT JSON", "SELECT 1 FORMAT `JSON`",
        'SELECT 1 FORMAT "JSON"',
        "SELECT 1 AS settings FORMAT JSON", "SELECT 1 AS limit FORMAT JSON",
        "WITH 1 AS settings SELECT settings FORMAT JSON",
        "SELECT 1 AS `format` FORMAT JSON",
        "DESCRIBE TABLE system.one FORMAT JSON",
        "DESC system.one FORMAT JSONEachRow",
        "SHOW TABLES FORMAT JSON", "SHOW DATABASES FORMAT JSON",
        "EXPLAIN SELECT 1 FORMAT JSON", "EXPLAIN AST SELECT 1 FORMAT JSON",
        "EXPLAIN PLAN SELECT 1 FORMAT JSON",
        "SELECT 1 UNION ALL SELECT 2 FORMAT JSON",
        "(SELECT 1) FORMAT JSON",
        "VALUES (1) FORMAT JSON",
        "SELECT 1 FORMAT JSON; -- trailing comment",
        "SELECT 1 FORMAT JSON; /* trailing comment */",
        "SELECT 1 FORMAT JSON; // trailing comment",
        "SELECT 1 FORMAT JSON // trailing comment\n;",
        "SELECT formatDateTime(toDateTime('2024-01-02'), '%Y') AS format FORMAT JSON",
    ],
)
def test_explicit_format_is_refused(query):
    with pytest.raises(ReadOnlyQueryError, match="FORMAT"):
        sanitize_clickhouse_read_only_sql(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'FORMAT JSON' AS x", "SELECT 1 AS format", "SELECT format FROM t",
        "SELECT x + format AS y FROM t", "SELECT format('x')",
        "SELECT `FORMAT`, \"FORMAT JSON\" FROM t", "SELECT $sql$FORMAT JSON; x$sql$",
        "SELECT 'it\\'s;ok'", "SELECT 1; // FORMAT JSON",
        "SELECT 1 /* FORMAT /* JSON */ inner */", "SELECT 1 # FORMAT JSON\n",
        "SELECT * FROM t ORDER BY format DESC", "SELECT 1 AS $format",
        "SELECT format JSON FROM (SELECT 1 AS format)",
        "SELECT formatDateTime(toDateTime('2024-01-02 03:04:05'), '%Y') AS format",
        "SELECT formatDateTime(toDateTime('2024-01-02'), format) FROM (SELECT '%Y' AS format)",
        "SELECT 1 AS format; -- trailing comment",
        "SELECT 1 AS format; /* trailing comment */",
        "SELECT 1 AS format; -- first comment\n/* second comment */",
        "SELECT 1 AS format; // trailing comment",
        "SELECT 1 AS format // trailing comment\n;",
        "SELECT 1 AS format; #! trailing comment",
        "SELECT 1 AS format; # trailing comment",
        "SELECT format('x'), 'last string'; // trailing comment",
        "SELECT format('x'), $str$quoted;value$str$; // trailing comment",
        "SELECT 1 AS format, 2 AS `year`; // trailing comment",
    ],
)
def test_data_comments_and_identifiers_are_preserved(query):
    assert sanitize_clickhouse_read_only_sql(query) == query.strip()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 AS format; -- comment\nSELECT 2",
        "SELECT 1 AS format; /* comment */ SELECT 2",
        "SELECT 1 AS format; // comment\nSELECT 2",
        "SELECT 1 AS format;; -- comment",
    ],
)
def test_trailing_comments_do_not_hide_additional_statements(query):
    with pytest.raises(ReadOnlyQueryError, match="Multiple SQL statements"):
        sanitize_clickhouse_read_only_sql(query)


@pytest.mark.anyio
@pytest.mark.parametrize("connector_class", [ClickHouseCLIConnector, ClickHousePythonConnector])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FORMAT JSON", "DESCRIBE TABLE system.one FORMAT JSON",
        "DESC system.one FORMAT JSONEachRow", "SHOW TABLES FORMAT JSON",
        "EXPLAIN AST SELECT 1 FORMAT JSON",
    ],
)
async def test_format_is_refused_before_connecting(
    connector_class, query, clickhouse_config, monkeypatch
):
    connector = connector_class(clickhouse_config)

    def unexpected(*args, **kwargs):
        raise AssertionError("rejected queries must not connect")

    monkeypatch.setattr(connector, "_get_ssh_tunnel", unexpected)
    with pytest.raises(ReadOnlyQueryError, match="FORMAT"):
        await connector.execute_query(query)


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("connector_class", [ClickHouseCLIConnector, ClickHousePythonConnector])
async def test_real_format_column_alias_keeps_tsv(connector_class, clickhouse_config, tmp_path):
    connector = connector_class(clickhouse_config)
    output = tmp_path / "result.tsv"
    output.touch(mode=0o600)
    await connector.execute_query_to_file("SELECT format JSON FROM (SELECT 1 AS format)", output)
    assert output.read_text(encoding="utf-8").splitlines() == ["JSON", "1"]


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("connector_class", [ClickHouseCLIConnector, ClickHousePythonConnector])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT formatDateTime(toDateTime('2024-01-02 03:04:05'), '%Y') AS format",
            ["format", "2024"],
        ),
        (
            (
                "SELECT formatDateTime(toDateTime('2024-01-02'), format) AS year "
                "FROM (SELECT '%Y' AS format)"
            ),
            ["year", "2024"],
        ),
        ("SELECT 1 AS format; -- trailing comment", ["format", "1"]),
        ("SELECT 1 AS format; /* trailing comment */", ["format", "1"]),
        ("SELECT 1 AS format; // trailing comment", ["format", "1"]),
        ("SELECT 1 AS format // trailing comment\n;", ["format", "1"]),
    ],
)
async def test_real_format_identifiers_and_trailing_comments_keep_tsv(
    connector_class, query, expected, clickhouse_config, tmp_path
):
    connector = connector_class(clickhouse_config)
    output = tmp_path / "result.tsv"
    output.touch(mode=0o600)

    await connector.execute_query_to_file(query, output)

    assert output.read_text(encoding="utf-8").splitlines() == expected
