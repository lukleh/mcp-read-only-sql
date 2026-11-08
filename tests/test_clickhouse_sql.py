"""ClickHouse client output policy respects lexical SQL boundaries."""

import shutil
import subprocess

import pytest
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.clickhouse.sql import (
    sanitize_clickhouse_read_only_sql,
)
from mcp_read_only_sql.utils.sql_guard import ReadOnlyQueryError
from tests.test_run_query_file_output import build_stub_server, make_runtime_paths


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
    ],
)
def test_data_comments_and_identifiers_are_preserved(query):
    assert sanitize_clickhouse_read_only_sql(query) == query.strip()


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


@pytest.mark.parametrize(
    "suffix",
    [
        "INTO OUTFILE '/tmp/query.tsv'", "into/**/outfile '/tmp/query.tsv' TRUNCATE",
        "INTO -- comment\nOUTFILE '/tmp/query.tsv' APPEND",
        "INTO /* outer /* nested */ comment */ OUTFILE '/tmp/query.tsv' AND STDOUT",
        "INTO // comment\nOUTFILE '/tmp/query.tsv' COMPRESSION 'gzip'",
        "INTO #! comment\nOUTFILE $file$/tmp/query.tsv$file$",
        'INTO OUTFILE "/tmp/query.tsv"', "INTO OUTFILE `/tmp/query.tsv`",
    ],
)
def test_outfile_variants_are_refused(suffix):
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        sanitize_clickhouse_read_only_sql("SELECT 1 " + suffix)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'INTO OUTFILE'", 'SELECT "INTO", `OUTFILE` FROM t',
        "SELECT 'it\\'s INTO OUTFILE; text'", "SELECT $sql$INTO OUTFILE '/tmp/x'$sql$",
        "SELECT 1 AS into", "SELECT 1 /* INTO OUTFILE */", "SELECT 1 // INTO OUTFILE",
        "SELECT into outfile FROM (SELECT 1 AS into)",
    ],
)
def test_outfile_text_inside_data_and_comments_is_preserved(query):
    assert sanitize_clickhouse_read_only_sql(query) == query


def test_local_client_accepts_outfile_identifier_alias(tmp_path):
    binary = shutil.which("clickhouse-local")
    if binary is None:
        pytest.skip("clickhouse-local is unavailable")
    query = "SELECT into outfile FROM (SELECT 1 AS into)"
    assert sanitize_clickhouse_read_only_sql(query) == query
    result = subprocess.run(
        [binary, "--path", str(tmp_path / "local"), "--format", "TabSeparatedWithNames", "--query", query],
        capture_output=True, text=True, timeout=15, check=True,
    )
    assert result.stdout.splitlines() == ["outfile", "1"]


@pytest.mark.anyio
@pytest.mark.parametrize("connector_class", [ClickHouseCLIConnector, ClickHousePythonConnector])
async def test_outfile_is_refused_before_connecting(
    connector_class, clickhouse_config, monkeypatch, tmp_path
):
    target = tmp_path / "existing.txt"
    target.write_text("original\n", encoding="utf-8")
    connector = connector_class(clickhouse_config)

    def unexpected(*args, **kwargs):
        raise AssertionError("a local file query must not connect or spawn")

    monkeypatch.setattr(connector, "_get_ssh_tunnel", unexpected)
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        await connector.execute_query(f"SELECT 'replacement' INTO OUTFILE '{target}' TRUNCATE")
    assert target.read_text(encoding="utf-8") == "original\n"


@pytest.mark.anyio
async def test_mcp_outfile_refusal_preserves_local_file(clickhouse_config, tmp_path):
    connector = ClickHouseCLIConnector(clickhouse_config)
    paths = make_runtime_paths(tmp_path)
    server = build_stub_server(connector, paths)
    target = tmp_path / "existing.txt"
    target.write_text("original\n", encoding="utf-8")
    with pytest.raises(ToolError, match="INTO OUTFILE"):
        await server.mcp._tool_manager.call_tool(
            "run_query_read_only",
            {
                "connection_name": connector.name,
                "query": f"SELECT 'replacement' INTO OUTFILE '{target}' TRUNCATE",
            },
            Context(mcp_server=server.mcp, subscriptions=server.mcp._subscriptions),
            convert_result=False,
        )
    assert target.read_text(encoding="utf-8") == "original\n"
    assert list(paths.results_dir.rglob("*.tsv")) == []


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
async def test_real_client_keeps_managed_output_control(
    connector_class, clickhouse_config, tmp_path
):
    output = tmp_path / "result.tsv"
    output.touch(mode=0o600)
    connector = connector_class(clickhouse_config)
    await connector.execute_query_to_file("SELECT 1 AS one", output)
    assert output.read_text(encoding="utf-8").splitlines() == ["one", "1"]
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        await connector.execute_query(f"SELECT 2 INTO OUTFILE '{output}' TRUNCATE")
    assert output.read_text(encoding="utf-8").splitlines() == ["one", "1"]
