"""Rows must reach the caller exactly as PostgreSQL returned them.

The psql connector used to parse psql's chatty output and drop lines that
looked like command tags or the row-count footer, which also dropped data
rows with those values and a trailing NULL row, and its unaligned output let
a tab or a line break inside a value corrupt the row. The Python connector
used a dict cursor, which collapsed duplicate column names. Both now produce
the same bytes for the same row: psql runs in CSV mode with a tab separator,
and the Python formatter follows psql's quoting rules.
"""

import asyncio
import csv
import io

import pytest

from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.utils.tsv_formatter import format_tsv_field
from tests.conftest import FakeCLIProcess

# Values that the old psql output filter mistook for psql chatter.
CHATTER_LOOKALIKE_ROWS = ["SET", "DO", "BEGIN", "COMMIT", "ROLLBACK", "(1 row)", ""]


def _fake_psql(monkeypatch, lines):
    async def fake_create_subprocess_exec(*cmd, **kwargs):
        return FakeCLIProcess(lines)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)


@pytest.mark.anyio
async def test_psql_stdout_lines_are_all_data(postgres_config, monkeypatch):
    """No stdout line is filtered: in quiet CSV mode psql prints only data."""
    _fake_psql(monkeypatch, ["x\n"] + [f"{value}\n" for value in CHATTER_LOOKALIKE_ROWS])

    result = await PostgreSQLCLIConnector(postgres_config).execute_query(
        "SELECT x FROM t"
    )

    assert result.split("\n") == ["x", *CHATTER_LOOKALIKE_ROWS]


@pytest.mark.anyio
async def test_psql_quoted_field_spanning_lines_is_kept_intact(
    postgres_config, monkeypatch
):
    """Only the record terminator is removed, so a quoted line break survives."""
    _fake_psql(monkeypatch, ["v\n", '"l1\n', 'l2"\n', '"abc\r"\n'])

    result = await PostgreSQLCLIConnector(postgres_config).execute_query(
        "SELECT v FROM t"
    )

    assert result == 'v\n"l1\nl2"\n"abc\r"'


@pytest.mark.parametrize(
    ("value", "rendered"),
    [
        (None, ""),
        ("", ""),
        ("plain", "plain"),
        (7, "7"),
        ("x\ty", '"x\ty"'),
        ('q"q', '"q""q"'),
        ("l1\nl2", '"l1\nl2"'),
        ("abc\r", '"abc\r"'),
    ],
)
def test_format_tsv_field_follows_psql_csv_rules(value, rendered):
    assert format_tsv_field(value) == rendered


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
        ("SELECT E'x\\ty' AS a, 1 AS b", 'a\tb\n"x\ty"\t1'),
        ("SELECT E'l1\\nl2' AS v", 'v\n"l1\nl2"'),
        ("SELECT E'abc\\r' AS v", 'v\n"abc\r"'),
        ("SELECT 'q\"q' AS v", 'v\n"q""q"'),
        ("SELECT '' AS x", "x\n"),
        ("SELECT NULL AS x", "x\n"),
        ("SELECT v FROM (VALUES ('a'), (NULL)) AS t(v) ORDER BY v NULLS LAST", "v\na\n"),
    ],
)
async def test_real_rows_are_returned_verbatim(
    implementation, query, expected, postgres_config
):
    """Both implementations return the same bytes for the same row."""
    result = await _connector(implementation, postgres_config).execute_query(query)
    assert result == expected


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("to_file", [False, True], ids=["inline", "file"])
async def test_python_preserves_postgresql_text_for_every_value_type(
    postgres_config, tmp_path, to_file
):
    query = """
        SELECT decode('000102ff', 'hex') AS bytes,
               '{"a":true,"b":null,"a":false}'::json AS json,
               'infinity'::date AS pos_inf,
               '-infinity'::timestamptz AS neg_inf,
               ARRAY[1,2,NULL] AS array,
               ARRAY['a,b', 'q"q', NULL] AS strings,
               true AS boolean,
               0.00000::numeric AS decimal,
               interval '1 year 2 months 3 days' AS span,
               date '12000-01-01' AS far_date
    """
    cli = PostgreSQLCLIConnector(postgres_config)
    python = PostgreSQLPythonConnector(postgres_config)
    expected = await cli.execute_query(query)
    if to_file:
        output = tmp_path / "raw-values.tsv"
        await python.execute_query_to_file(query, output)
        actual = output.read_text(encoding="utf-8")
    else:
        actual = await python.execute_query(query)

    assert actual == expected
    _, values = list(csv.reader(io.StringIO(actual), delimiter="\t"))
    assert values[:5] == [
        r"\x000102ff",
        '{"a":true,"b":null,"a":false}',
        "infinity",
        "-infinity",
        "{1,2,NULL}",
    ]
    assert values[6:] == ["t", "0.00000", "1 year 2 mons 3 days", "12000-01-01"]


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
async def test_text_typecasting_does_not_change_other_database_cursors(postgres_config):
    import psycopg2

    await PostgreSQLPythonConnector(postgres_config).execute_query("SELECT true")
    conn = psycopg2.connect(
        host=postgres_config.servers[0].host,
        port=postgres_config.servers[0].port,
        dbname=postgres_config.database,
        user=postgres_config.username,
        password=postgres_config.password,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT true, 1, ARRAY[1,2]")
            assert cursor.fetchone() == (True, 1, [1, 2])
    finally:
        conn.close()
