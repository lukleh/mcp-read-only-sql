"""Read-only enforcement tests executed against real Docker databases."""

import shutil
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.connectors.postgresql.python import PostgreSQLPythonConnector
from mcp_read_only_sql.utils.sql_guard import ReadOnlyQueryError
from tests.conftest import make_connection
from tests.docker_test_config import docker_test_server
from tests.sql_statement_lists import (
    CLICKHOUSE_ALLOWED_LITERAL_QUERIES,
    CLICKHOUSE_INTEGRATION_BLOCKED_STATEMENTS,
    POSTGRESQL_ALLOWED_LITERAL_QUERIES,
    POSTGRESQL_INTEGRATION_BLOCKED_STATEMENTS,
)

POSTGRES_BASE_CONFIG = {
    "connection_name": "integration_postgres",
    "type": "postgresql",
    "servers": [docker_test_server("postgresql")],
    "db": "testdb",
    "username": "testuser",
    "password": "testpass",
}

CLICKHOUSE_BASE_CONFIG = {
    "connection_name": "integration_clickhouse",
    "type": "clickhouse",
    "servers": [docker_test_server("clickhouse")],
    "db": "testdb",
    "username": "testuser",
    "password": "testpass",
}


def _is_readonly_error(message: str) -> bool:
    lowered = message.lower()
    keywords = [
        "read only",
        "read-only",
        "readonly",
        "cannot execute",
        "permission denied",
        "readonly mode",
    ]
    return any(keyword in lowered for keyword in keywords)


async def _verify_connection(connector, vendor: str):
    """Ensure the target database is reachable; otherwise skip."""
    try:
        await connector.execute_query("SELECT 1")
    except Exception as exc:  # pragma: no cover - only exercised when docker is absent
        pytest.skip(f"{vendor} not reachable: {exc}")


def _build_postgres_connector(implementation: str):

    config = make_connection(POSTGRES_BASE_CONFIG.copy())
    if implementation == "cli":
        if shutil.which("psql") is None:
            pytest.skip("psql command not available in PATH")
        return PostgreSQLCLIConnector(config)
    return PostgreSQLPythonConnector(config)


def _build_clickhouse_connector(implementation: str):

    config = make_connection(CLICKHOUSE_BASE_CONFIG.copy())
    if implementation == "cli":
        if shutil.which("clickhouse-client") is None:
            pytest.skip("clickhouse-client command not available in PATH")
        return ClickHouseCLIConnector(config)
    return ClickHousePythonConnector(config)


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("statement", POSTGRESQL_INTEGRATION_BLOCKED_STATEMENTS)
async def test_postgres_real_blocks_mutations(implementation, statement):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query(statement)

    assert _is_readonly_error(
        str(exc_info.value)
    ), f"Expected read-only error for {statement}"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("statement", POSTGRESQL_INTEGRATION_BLOCKED_STATEMENTS)
async def test_postgres_real_server_rejects_mutations_without_client_guard(
    implementation, statement, monkeypatch
):
    """The read-only transaction still holds when the AST guard is bypassed."""
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    module = "mcp_read_only_sql.connectors.postgresql." + implementation
    monkeypatch.setattr(
        f"{module}.sanitize_postgresql_read_only_sql",
        lambda query, allowed_functions=(): query.strip(),
    )
    monkeypatch.setattr(
        f"{module}.postgresql_shadow_query",
        lambda query, allowed_functions=(): None,
    )

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert _is_readonly_error(
        str(exc_info.value)
    ), f"Expected server-side read-only error for {statement}"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("query", POSTGRESQL_ALLOWED_LITERAL_QUERIES)
async def test_postgres_real_allows_selects_with_keywords(implementation, query):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    result = await connector.execute_query(query)
    assert "INSERT" in result, "Keyword inside literal should be preserved"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize(
    "query,shadow",
    [
        ("SELECT md5('x')", "public.md5(text)"),
        ("SELECT 1 @@@ 2", "public.@@@(integer,integer)"),
    ],
)
async def test_postgres_real_refuses_names_shadowed_on_search_path(
    implementation, query, shadow
):
    """docker/postgres/init/03_shadows.sql plants public.md5 and public.@@@."""
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query(query)
    assert shadow in str(exc_info.value)

    # The catalog-qualified spelling cannot be shadowed and still works.
    result = await connector.execute_query("SELECT pg_catalog.md5('x') AS h")
    assert "shadowed" not in result


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
async def test_postgres_real_overload_of_catalog_name_is_refused_conservatively(
    implementation,
):
    """public.upper(integer) is visible; upper('x') would resolve to pg_catalog, but
    the name-based check refuses it and says how to proceed."""
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query("SELECT upper('x')")
    message = str(exc_info.value)
    assert "public.upper(integer)" in message
    assert "pg_catalog.<name>(...)" in message

    result = await connector.execute_query("SELECT pg_catalog.upper('x') AS u")
    assert "X" in result


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
async def test_postgres_real_qualified_allowance_pins_bare_call_to_its_schema(
    implementation,
):
    """testuser's search_path is (shadow, public); review_helper() exists in both."""

    def build(allowed):
        config = make_connection({**POSTGRES_BASE_CONFIG, "allowed_functions": allowed})
        if implementation == "cli":
            return PostgreSQLCLIConnector(config)
        return PostgreSQLPythonConnector(config)

    connector = build(["public.review_helper"])
    await _verify_connection(connector, "PostgreSQL")

    # Bare call would resolve to shadow.review_helper(), not the allowed schema.
    with pytest.raises(ReadOnlyQueryError) as exc_info:
        await connector.execute_query("SELECT review_helper()")
    assert "shadow.review_helper()" in str(exc_info.value)

    # The qualified call and a bare call with the resolving schema listed both work.
    assert "public" in await connector.execute_query("SELECT public.review_helper() AS r")
    both = build(["public.review_helper", "shadow.review_helper"])
    assert "shadow" in await both.execute_query("SELECT review_helper() AS r")


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
async def test_postgres_qualified_allowance_rejects_quoted_dotted_identifier(
    implementation,
):
    config = make_connection(
        {**POSTGRES_BASE_CONFIG, "allowed_functions": ["public.review_helper"]}
    )
    connector = (
        PostgreSQLCLIConnector(config)
        if implementation == "cli"
        else PostgreSQLPythonConnector(config)
    )
    assert "public" in await connector.execute_query("SELECT public.review_helper()")
    with pytest.raises(ReadOnlyQueryError, match="allow-list"):
        await connector.execute_query('SELECT "public.review_helper"()')


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
async def test_postgres_real_blocks_transaction_escape(implementation):
    connector = _build_postgres_connector(implementation)
    await _verify_connection(connector, "PostgreSQL")

    payload = "SET transaction_read_only TO false ; DROP TABLE newtable"
    with pytest.raises(ReadOnlyQueryError):
        await connector.execute_query(payload)


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("statement", CLICKHOUSE_INTEGRATION_BLOCKED_STATEMENTS)
async def test_clickhouse_real_blocks_mutations(implementation, statement):
    connector = _build_clickhouse_connector(implementation)
    await _verify_connection(connector, "ClickHouse")

    with pytest.raises(RuntimeError) as exc_info:
        await connector.execute_query(statement)

    assert _is_readonly_error(
        str(exc_info.value)
    ), f"Expected read-only error for {statement}"


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize("query", CLICKHOUSE_ALLOWED_LITERAL_QUERIES)
async def test_clickhouse_real_allows_selects_with_keywords(implementation, query):
    connector = _build_clickhouse_connector(implementation)
    await _verify_connection(connector, "ClickHouse")

    result = await connector.execute_query(query)
    assert "INSERT" in result, "Keyword inside literal should be preserved"


@pytest.fixture(scope="module")
def operator_shadow_config():
    """A private login sees harmless comparison overloads before pg_catalog."""
    endpoint = POSTGRES_BASE_CONFIG["servers"][0]
    admin = psycopg2.connect(
        host=endpoint["host"],
        port=endpoint["port"],
        dbname=POSTGRES_BASE_CONFIG["db"],
        user=POSTGRES_BASE_CONFIG["username"],
        password=POSTGRES_BASE_CONFIG["password"],
    )
    admin.autocommit = True
    suffix = uuid4().hex
    schema = "mcp_ops_" + suffix
    role = "mcp_role_" + suffix
    role_created = False
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD %s").format(sql.Identifier(role)),
                ("testpass",),
            )
            role_created = True
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
            cursor.execute(
                sql.SQL("ALTER ROLE {} SET search_path = {}, pg_catalog").format(
                    sql.Identifier(role), sql.Identifier(schema)
                )
            )
            for operator, function in [
                ("<=", "le"), (">=", "ge"), ("<", "lt"), (">", "gt"), ("=", "eq")
            ]:
                cursor.execute(
                    sql.SQL(
                        "CREATE FUNCTION {}.{}(integer, integer) RETURNS boolean "
                        "LANGUAGE sql AS 'SELECT $1 OPERATOR(pg_catalog.{}) $2'"
                    ).format(
                        sql.Identifier(schema), sql.Identifier(function), sql.SQL(operator)
                    )
                )
                cursor.execute(
                    sql.SQL(
                        "CREATE OPERATOR {}.{} (LEFTARG = integer, RIGHTARG = integer, "
                        "FUNCTION = {}.{})"
                    ).format(
                        sql.Identifier(schema),
                        sql.SQL(operator),
                        sql.Identifier(schema),
                        sql.Identifier(function),
                    )
                )
        yield {**POSTGRES_BASE_CONFIG, "username": role, "password": "testpass"}
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
            if role_created:
                cursor.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        admin.close()


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT 5 BETWEEN 1 AND 10",
        "SELECT 5 NOT BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN SYMMETRIC 1 AND 10",
        "SELECT 5 NOT BETWEEN SYMMETRIC 1 AND 10",
        "SELECT * FROM (VALUES (1)) a(id) JOIN (VALUES (1)) b(id) USING (id)",
        "SELECT * FROM (VALUES (1)) a(id) NATURAL JOIN (VALUES (1)) b(id)",
        "SELECT CASE 1 WHEN 2 THEN true ELSE false END",
        "SELECT 1 IN (SELECT 2)",
        "SELECT 1 NOT IN (SELECT 2)",
        "SELECT 1 <= ALL (SELECT 2)",
        "SELECT 1 >= ANY (SELECT 2)",
    ],
)
async def test_postgres_refuses_shadowed_implicit_comparisons(
    operator_shadow_config, implementation, query
):
    config = make_connection(operator_shadow_config)
    connector = (
        PostgreSQLCLIConnector(config)
        if implementation == "cli"
        else PostgreSQLPythonConnector(config)
    )
    assert await connector.execute_query(
        "SELECT (5 OPERATOR(pg_catalog.<=) 10)::text AS ok"
    ) == "ok\ntrue"
    with pytest.raises(ReadOnlyQueryError, match="Read-only guard"):
        await connector.execute_query(query)


@pytest.mark.anyio
@pytest.mark.security
@pytest.mark.docker
@pytest.mark.parametrize("implementation", ["python", "cli"])
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT (5 BETWEEN 1 AND 10)::text AS ok", "ok\ntrue"),
        ("SELECT CASE 1 WHEN 1 THEN 'yes' ELSE 'no' END AS ok", "ok\nyes"),
        ("SELECT (1 IN (SELECT 1))::text AS ok", "ok\ntrue"),
        (
            "SELECT * FROM (VALUES (1)) a(id) JOIN (VALUES (1)) b(id) USING (id)",
            "id\n1",
        ),
    ],
)
async def test_postgres_unshadowed_implicit_comparisons_still_work(
    implementation, query, expected
):
    connector = _build_postgres_connector(implementation)
    assert await connector.execute_query(query) == expected
