"""Unit tests for the PostgreSQL AST read-only policy in ``sql_guard``."""

import pytest

from mcp_read_only_sql.utils.sql_guard import (
    PG_CATALOG_ALLOWED_FUNCTIONS,
    SHADOW_GUARD_PREFIX,
    ReadOnlyQueryError,
    postgresql_shadow_guard_block,
    postgresql_shadow_query,
    sanitize_postgresql_read_only_sql,
)
from tests.sql_statement_lists import (
    POSTGRESQL_DDL_ALTER_STATEMENTS,
    POSTGRESQL_DDL_CREATE_STATEMENTS,
    POSTGRESQL_DDL_DROP_STATEMENTS,
    POSTGRESQL_DML_STATEMENTS,
    POSTGRESQL_LOCK_STATEMENTS,
    POSTGRESQL_MAINTENANCE_STATEMENTS,
    POSTGRESQL_PROCEDURAL_STATEMENTS,
    POSTGRESQL_TRANSACTION_STATEMENTS,
)

ALLOWED_QUERIES = [
    "SELECT 1",
    "SELECT 1;",
    "select count(*), max(id), string_agg(name, ',') from users where id > 1",
    "SELECT length('a;b'), lower(name), now(), date_trunc('day', created_at) FROM users",
    "SELECT pg_catalog.length('x'), pg_size_pretty(pg_relation_size('users'))",
    "SELECT * FROM users u JOIN orders o ON o.user_id = u.id ORDER BY 1 LIMIT 5",
    "SELECT row_number() OVER (PARTITION BY x ORDER BY y), lag(y) OVER () FROM t",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
    "SELECT * FROM generate_series(1, 3) AS g(n), unnest(ARRAY[1, 2]) AS u(v)",
    "SELECT * FROM users TABLESAMPLE SYSTEM (10)",
    "SELECT * FROM users TABLESAMPLE BERNOULLI (10)",
    "SELECT * FROM users TABLESAMPLE pg_catalog.system (10)",
    "SELECT * FROM users ORDER BY id USING <, name USING OPERATOR(pg_catalog.>)",
    "SELECT 1 UNION SELECT 2 EXCEPT SELECT 3",
    "VALUES (1, 'a'), (2, 'b')",
    "TABLE users",
    "SELECT 'x'::text, CAST(1 AS bigint), 1 OPERATOR(pg_catalog.+) 2, 'a' ~ 'b'",
    "SELECT current_setting('server_version'), current_user, pg_backend_pid()",
    "SELECT * FROM pg_stat_activity WHERE state = 'active'",
    "SELECT * FROM users -- trailing comment",
    "/* leading */ SELECT 1",
    "SELECT 'INSERT INTO users VALUES (1); COMMIT' AS sample_text",
    "SELECT json_agg(u), jsonb_build_object('a', 1), to_char(now(), 'YYYY')",
    "SELECT random(), gen_random_uuid(), clock_timestamp(), pg_is_in_recovery()",
    "SELECT pg_sleep(0.01)",
    "SELECT (SELECT max(id) FROM users) AS m, EXISTS (SELECT 1 FROM orders)",
    "SELECT * FROM (SELECT 1) AS sub, LATERAL (SELECT 2) AS l",
    "EXPLAIN SELECT * FROM users",
    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM users",
    "SHOW search_path",
    "SELECT xmlelement(name foo, 'bar'), xpath('/a', '<a/>'::xml)",
    "SELECT * FROM XMLTABLE('/r' PASSING xml '<r/>' COLUMNS a text)",
    'SELECT "Mixed_Case_Column" FROM "Mixed_Case_Table"',
]

# Statement shapes a read-only transaction alone does not stop.
BLOCKED_READ_ONLY_TRANSACTION_ESCAPES = [
    "COPY (SELECT 1) TO PROGRAM 'id'",
    "COPY (SELECT 1) TO '/tmp/out.txt'",
    "COPY users TO STDOUT",
    "COPY (SELECT 1) TO STDOUT",
    "DO $$ BEGIN PERFORM 1; END $$",
    "SELECT pg_terminate_backend(1)",
    "SELECT pg_cancel_backend(1)",
    "SELECT pg_reload_conf()",
    "SELECT pg_rotate_logfile()",
    "SELECT lo_export(1, '/tmp/x')",
    "SELECT lo_import('/etc/passwd')",
    "SELECT pg_read_file('/etc/passwd')",
    "SELECT pg_ls_dir('.')",
    "SELECT set_config('role', 'postgres', true)",
    "SELECT pg_create_physical_replication_slot('s')",
    "SELECT pg_drop_replication_slot('s')",
    "SELECT query_to_xml('SELECT 1', true, true, '')",
    "SELECT ts_rewrite('a'::tsquery, 'SELECT 1')",
    "SELECT txid_current()",
    "SELECT pg_current_xact_id()",
    "SELECT pg_advisory_lock(1)",
    "SELECT nextval('seq')",
    "SELECT dblink_exec('conn', 'SELECT 1')",
    "SELECT public.my_function()",
    "SELECT my_function()",
    "SELECT * FROM my_set_returning_function()",
    "SELECT PG_TERMINATE_BACKEND(1)",
    "SELECT pg_catalog.pg_terminate_backend(1)",
    "SELECT 1 OPERATOR(public.===) 2",
    "SELECT * FROM users TABLESAMPLE system_rows (10)",
    "SELECT * FROM users TABLESAMPLE public.system (10)",
    "SELECT * FROM users ORDER BY id USING OPERATOR(public.<<<)",
    "SELECT * FROM users ORDER BY id USING OPERATOR(myschema.<)",
    "SELECT 1 FOR UPDATE",
    "SELECT 1 FOR SHARE",
    "SELECT 1 INTO new_table",
    "WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d",
    "WITH i AS (INSERT INTO users (id) VALUES (1) RETURNING id) SELECT 1",
    "EXPLAIN DELETE FROM users",
    "EXPLAIN ANALYZE INSERT INTO users (id) VALUES (1)",
    "SET search_path = public",
    "SET transaction_read_only = off",
    "RESET ALL",
    "LOAD 'auto_explain'",
    "LISTEN chan",
    "DECLARE c CURSOR FOR SELECT 1",
    "FETCH ALL FROM c",
    "PREPARE p AS SELECT 1",
    "EXECUTE p",
    "SELECT 1; SELECT 2",
]


@pytest.mark.security
@pytest.mark.parametrize("query", ALLOWED_QUERIES)
def test_allows_read_only_queries(query):
    assert sanitize_postgresql_read_only_sql(query) == query.strip()


@pytest.mark.security
@pytest.mark.parametrize("query", BLOCKED_READ_ONLY_TRANSACTION_ESCAPES)
def test_blocks_read_only_transaction_escapes(query):
    with pytest.raises(ReadOnlyQueryError) as exc_info:
        sanitize_postgresql_read_only_sql(query)
    assert "read-only" in str(exc_info.value).lower()


@pytest.mark.security
@pytest.mark.parametrize(
    "query",
    POSTGRESQL_DML_STATEMENTS
    + POSTGRESQL_TRANSACTION_STATEMENTS
    + POSTGRESQL_LOCK_STATEMENTS
    + POSTGRESQL_DDL_CREATE_STATEMENTS
    + POSTGRESQL_DDL_ALTER_STATEMENTS
    + POSTGRESQL_DDL_DROP_STATEMENTS
    + POSTGRESQL_MAINTENANCE_STATEMENTS
    + POSTGRESQL_PROCEDURAL_STATEMENTS,
)
def test_blocks_every_enumerated_write_statement(query):
    with pytest.raises(ReadOnlyQueryError):
        sanitize_postgresql_read_only_sql(query)


@pytest.mark.security
def test_hidden_payloads_in_literals_and_comments_are_inert():
    query = (
        "SELECT 'pg_terminate_backend(1)' AS s, "
        "'COPY x TO PROGRAM ''id''' AS c -- pg_reload_conf()\n"
        "/* DO $$ BEGIN END $$ */"
    )
    assert sanitize_postgresql_read_only_sql(query) == query


@pytest.mark.security
def test_unparseable_sql_is_rejected_before_reaching_the_server():
    with pytest.raises(ReadOnlyQueryError) as exc_info:
        sanitize_postgresql_read_only_sql("SELEC 1")
    assert "parse" in str(exc_info.value).lower()


@pytest.mark.security
def test_allowed_functions_extend_the_policy_per_connection():
    with pytest.raises(ReadOnlyQueryError):
        sanitize_postgresql_read_only_sql("SELECT public.my_helper(1)")
    assert (
        sanitize_postgresql_read_only_sql(
            "SELECT public.my_helper(1)", ["public.my_helper"]
        )
        == "SELECT public.my_helper(1)"
    )
    assert (
        sanitize_postgresql_read_only_sql("SELECT my_helper(1)", ["my_helper"])
        == "SELECT my_helper(1)"
    )
    # A schema-qualified allowance also covers the usual bare call, which
    # resolves through search_path.
    assert (
        sanitize_postgresql_read_only_sql("SELECT my_helper(1)", ["public.my_helper"])
        == "SELECT my_helper(1)"
    )
    # A bare allowance does not cover a schema-qualified call to another schema,
    # and a qualified allowance does not cover a different schema.
    with pytest.raises(ReadOnlyQueryError):
        sanitize_postgresql_read_only_sql("SELECT other.my_helper(1)", ["my_helper"])
    with pytest.raises(ReadOnlyQueryError):
        sanitize_postgresql_read_only_sql(
            "SELECT other.my_helper(1)", ["public.my_helper"]
        )


@pytest.mark.security
def test_function_rejection_names_the_config_key():
    with pytest.raises(ReadOnlyQueryError) as exc_info:
        sanitize_postgresql_read_only_sql("SELECT pg_reload_conf()")
    message = str(exc_info.value)
    assert "pg_reload_conf()" in message
    assert "allowed_functions" in message


@pytest.mark.security
def test_side_effecting_catalog_functions_are_absent_from_the_allow_list():
    for name in (
        "pg_terminate_backend",
        "pg_cancel_backend",
        "pg_reload_conf",
        "pg_rotate_logfile",
        "lo_export",
        "lo_import",
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "set_config",
        "pg_create_physical_replication_slot",
        "pg_create_logical_replication_slot",
        "pg_drop_replication_slot",
        "query_to_xml",
        "cursor_to_xml",
        "ts_rewrite",
        "ts_stat",
        "txid_current",
        "pg_current_xact_id",
        "pg_advisory_lock",
        "pg_try_advisory_lock",
        "nextval",
        "setval",
        "pg_notify",
        "pg_switch_wal",
        "pg_promote",
        "pg_stat_reset",
        "pg_export_snapshot",
        "pg_logical_emit_message",
    ):
        assert name not in PG_CATALOG_ALLOWED_FUNCTIONS, name
    for name in ("length", "count", "now", "current_setting", "pg_relation_size"):
        assert name in PG_CATALOG_ALLOWED_FUNCTIONS, name


@pytest.mark.security
def test_shadow_query_is_skipped_when_nothing_resolves_through_search_path():
    assert postgresql_shadow_query("SELECT 1") is None
    assert postgresql_shadow_query("SELECT pg_catalog.length('x')") is None
    assert postgresql_shadow_query("SELECT 1 OPERATOR(pg_catalog.+) 2") is None
    assert postgresql_shadow_query("SELECT * FROM t WHERE a BETWEEN 1 AND 2") is None


@pytest.mark.security
def test_shadow_query_lists_bare_functions_and_operators():
    sql = postgresql_shadow_query(
        "SELECT length(a), count(*) FROM t WHERE 1 @@@ 2 AND a ~~ 'x' "
        "ORDER BY a USING <<<"
    )
    assert sql is not None
    assert "ARRAY['count', 'length']::pg_catalog.name[]" in sql
    assert "ARRAY['<<<', '@@@', '~~']::pg_catalog.name[]" in sql
    assert "pg_catalog.current_schemas(true)" in sql
    # The check itself must not depend on search_path resolution.
    assert " = " not in sql
    assert "OPERATOR(pg_catalog.=)" in sql


@pytest.mark.security
def test_shadow_query_exempts_allowed_functions_and_quotes_names():
    sql = postgresql_shadow_query(
        "SELECT my_helper(1), length('x')", ["public.my_helper"]
    )
    assert sql is not None
    assert "'my_helper'" not in sql
    assert "'length'" in sql
    sql = postgresql_shadow_query("SELECT \"it's\"(1)", ["it's"])
    assert sql is None
    sql = postgresql_shadow_query("SELECT length(1) AS x, \"o'k\"(1)", ["o'k", "zzz"])
    assert sql is not None and "'length'" in sql and "o''k" not in sql


@pytest.mark.security
def test_shadow_guard_block_raises_with_the_marker():
    block = postgresql_shadow_guard_block("SELECT 'x' AS shadow")
    assert block.startswith("DO $readonly_guard$")
    assert f"RAISE EXCEPTION '{SHADOW_GUARD_PREFIX} %" in block
