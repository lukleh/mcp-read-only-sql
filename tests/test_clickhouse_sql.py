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


@pytest.mark.parametrize("space", ["\u180e", "\u200b", "\u200c", "\u200d", "\u2060", "\ufeff"])
@pytest.mark.parametrize("prefix", ["SELECT 1", "SELECT 1 FROM system.one"])
def test_native_whitespace_cannot_hide_output_format(space, prefix):
    with pytest.raises(ReadOnlyQueryError, match="FORMAT"):
        sanitize_clickhouse_read_only_sql(f"{prefix} FORMAT{space}JSON")


@pytest.mark.parametrize("query", [
    "SELECT ‘--’ FORMAT JSON",
    "SELECT “--” FROM (SELECT 1 AS “--”) FORMAT JSON",
])
def test_unicode_quotes_cannot_hide_output_format(query):
    with pytest.raises(ReadOnlyQueryError, match="FORMAT"):
        sanitize_clickhouse_read_only_sql(query)


def test_unicode_quote_cannot_hide_additional_statement():
    with pytest.raises(ReadOnlyQueryError, match="Multiple SQL statements"):
        sanitize_clickhouse_read_only_sql("SELECT ‘'’; SELECT 2 -- '")


@pytest.mark.parametrize("query", [
    "/* comment */", "-- comment", "// comment", "# comment",
    "\u180e", "\u200b", "\u200c", "\u200d", "\u2060", "\ufeff",
    "\u200b /* comment */",
])
def test_comment_or_native_whitespace_only_is_empty(query):
    with pytest.raises(ReadOnlyQueryError, match="Query must not be empty"):
        sanitize_clickhouse_read_only_sql(query)


@pytest.mark.parametrize("query", [
    "SELECT ‘;’", "SELECT “;” FROM t",
    'SELECT ‘--’ AS "format"',
    "SELECT '‘--’\u200b' AS format",
    'SELECT "‘x’" AS format FROM t',
    "SELECT 1 AS format; -- \u200b ‘x’",
    "SELECT 1 AS format /* \u200b ‘x’ */",
])
def test_native_syntax_outside_parser_or_inside_ascii_quotes_is_preserved(query):
    assert sanitize_clickhouse_read_only_sql(query) == query


@pytest.mark.anyio
@pytest.mark.parametrize("connector_class", [ClickHouseCLIConnector, ClickHousePythonConnector])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FORMAT JSON", "DESCRIBE TABLE system.one FORMAT JSON",
        "DESC system.one FORMAT JSONEachRow", "SHOW TABLES FORMAT JSON",
        "EXPLAIN AST SELECT 1 FORMAT JSON",
        "SELECT 1 FORMAT\u200bJSON", "SELECT ‘--’ FORMAT JSON",
        "SELECT “--” FROM (SELECT 1 AS “--”) FORMAT JSON",
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
        "INTO OUTFILE X'2f746d702f71756572792e747376'",
        "INTO OUTFILE x'2f746d702f71756572792e747376' TRUNCATE",
        "INTO OUTFILE b'01110000'", "INTO OUTFILE B'01110000' APPEND",
        "INTO /* comment */ OUTFILE X'70' AND STDOUT",
        "INTO OUTFILE ‘/tmp/query.tsv’", "INTO OUTFILE “/tmp/query.tsv”",
    ],
)
def test_outfile_variants_are_refused(suffix):
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        sanitize_clickhouse_read_only_sql("SELECT 1 " + suffix)


@pytest.mark.parametrize("space", ["\u180e", "\u200b", "\u200c", "\u200d", "\u2060", "\ufeff"])
@pytest.mark.parametrize("literal", ["'/tmp/query.tsv'", "X'70'", "b'01110000'", "‘/tmp/query.tsv’", "$x$/tmp/query.tsv$x$"])
@pytest.mark.parametrize("position", ["keyword", "operand"])
def test_clickhouse_unicode_whitespace_does_not_hide_outfile(space, literal, position):
    query = (
        f"SELECT 1 INTO{space}OUTFILE {literal}"
        if position == "keyword" else f"SELECT 1 INTO OUTFILE{space}{literal}"
    )
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        sanitize_clickhouse_read_only_sql(query)


@pytest.mark.parametrize("space", ["\u180e", "\u200b", "\u200c", "\u200d", "\u2060", "\ufeff"])
def test_clickhouse_unicode_whitespace_keeps_identifier_alias(space):
    query = f"SELECT into{space}outfile FROM (SELECT 1 AS into)"
    assert sanitize_clickhouse_read_only_sql(query) == query


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'INTO OUTFILE'", 'SELECT "INTO", `OUTFILE` FROM t',
        "SELECT 'it\\'s INTO OUTFILE; text'", "SELECT $sql$INTO OUTFILE '/tmp/x'$sql$",
        "SELECT 1 AS into", "SELECT 1 /* INTO OUTFILE */", "SELECT 1 // INTO OUTFILE",
        "SELECT into outfile FROM (SELECT 1 AS into)",
        "SELECT X'494e544f204f555446494c45' AS value",
        "SELECT b'01110000' AS value",
        "SELECT ‘INTO OUTFILE’ AS value", "SELECT “INTO OUTFILE” FROM t",
    ],
)
def test_outfile_text_inside_data_and_comments_is_preserved(query):
    assert sanitize_clickhouse_read_only_sql(query) == query


@pytest.mark.parametrize("query", ["SELECT X'not-hex'", "SELECT B'012'", "SELECT X'41", "SELECT ‘open"])
def test_malformed_literal_forms_are_refused(query):
    with pytest.raises(ReadOnlyQueryError):
        sanitize_clickhouse_read_only_sql(query)


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
@pytest.mark.parametrize("literal_kind", ["string", "hex", "binary", "unicode"])
async def test_outfile_is_refused_before_connecting(
    connector_class, literal_kind, clickhouse_config, monkeypatch, tmp_path
):
    target = tmp_path / "existing.txt"
    target.write_text("original\n", encoding="utf-8")
    connector = connector_class(clickhouse_config)

    def unexpected(*args, **kwargs):
        raise AssertionError("a local file query must not connect or spawn")

    monkeypatch.setattr(connector, "_get_ssh_tunnel", unexpected)
    path = str(target)
    literal = {
        "string": f"'{path}'",
        "hex": f"X'{path.encode().hex()}'",
        "binary": "b'" + "".join(f"{byte:08b}" for byte in path.encode()) + "'",
        "unicode": f"‘{path}’",
    }[literal_kind]
    with pytest.raises(ReadOnlyQueryError, match="INTO OUTFILE"):
        await connector.execute_query(f"SELECT 'replacement' INTO OUTFILE {literal} TRUNCATE")
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
