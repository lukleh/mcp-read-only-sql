# Read-Only Enforcement Matrix

This document maps every SQL statement in PostgreSQL and ClickHouse that can
modify data, schema, roles, configuration, or transaction state to the
mechanisms that block it in the MCP Read-Only SQL server. For each database and
connector implementation we list the relevant safeguards and available unit
tests.

Legend:

- **CLI enforcement** – Guards applied by `PostgreSQLCLIConnector` or
  `ClickHouseCLIConnector` before invoking the database CLI.
- **Python enforcement** – Guards applied by `PostgreSQLPythonConnector` or
  `ClickHousePythonConnector` via driver settings.
- **Tests** – Current automated coverage. `Needs coverage` highlights gaps.
- Unless otherwise stated, PostgreSQL failures surface with SQLSTATE `25006`
  (`cannot execute <statement> in a read-only transaction`) and ClickHouse with
  `READONLY`/`UNSUPPORTED` style errors emitted by the server when
  `readonly=1` is active.

---

## PostgreSQL

**Connector recap**

- Both: `sanitize_postgresql_read_only_sql` parses the query with PostgreSQL's
  own grammar (`pglast`) before anything is sent. Every statement node in the
  tree must be `SELECT`, `EXPLAIN` or `SHOW`; every function call must be on
  the allow-list of `pg_catalog` functions PostgreSQL declares `IMMUTABLE` or
  `STABLE` (plus a short reviewed list of read-only volatile ones, minus
  `txid_current`, `pg_current_xact_id` and `ts_rewrite`), extended per
  connection by `allowed_functions`. Operators and `TABLESAMPLE` methods
  outside `pg_catalog` are refused too. This is the layer that stops what a
  read-only transaction does not.
- CLI: the sanitized single statement is wrapped in `BEGIN; SET TRANSACTION
  READ ONLY; ... COMMIT;`. `psql` always runs with `--single-transaction`,
  `-v ON_ERROR_STOP=1`, and `PGOPTIONS=-c default_transaction_read_only=on`.
- Python: `psycopg2.connect(..., options='-c default_transaction_read_only=on')`
  plus `conn.set_session(readonly=True, autocommit=True)` create a database
  session that refuses writes at the protocol level.

**What the read-only transaction alone does not stop.** For a role with the
matching privilege (a superuser has all of them), `COPY ... TO PROGRAM`,
`COPY ... TO '/path'`, `lo_export()`, `pg_read_file()`, `pg_terminate_backend()`,
`pg_reload_conf()`, `set_config()`, replication-slot functions and `DO` blocks
all succeed inside `SET TRANSACTION READ ONLY`. The AST guard refuses each of
them before execution (`tests/test_sql_guard_postgresql.py`). The guard is a
filter, not a privilege boundary: it cannot see inside views, user-defined
types or casts that already exist in the database, and it does not reduce what
the configured login may do. Log in with a role that can only
read.

### Data Manipulation Language (DML)

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `INSERT`, `INSERT ... RETURNING`, `INSERT ... ON CONFLICT`, `INSERT ... SELECT`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`, `SELECT INTO`, data-modifying CTEs | AST guard refuses every statement class except `SELECT`/`EXPLAIN`/`SHOW` before execution; the read-only transaction and `PGOPTIONS` remain as the second layer (SQLSTATE 25006) | Same guard; read-only session at server level as the second layer | Covered by `test_sql_guard_postgresql.py`, `test_postgresql_cli_blocks_write_statements`, `test_postgresql_python_blocks_write_statements`, and `test_postgres_real_server_rejects_mutations_without_client_guard` for the server layer alone |
| `COPY` in every form: `FROM`, `TO STDOUT`, `TO '/path'`, `TO PROGRAM`, `FROM PROGRAM` | AST guard refuses `COPY` outright. Note: a read-only transaction does **not** block `COPY ... TO PROGRAM` or `COPY ... TO '/path'` for a role holding `pg_execute_server_program` / `pg_write_server_files` (or a superuser) | Same | Covered by `test_sql_guard_postgresql.py` |

### Transaction Control & Session State

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `BEGIN`, `START TRANSACTION`, `SET TRANSACTION`, `SET SESSION CHARACTERISTICS AS TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED` | Sanitizer rejects transaction-control keywords; even if it reached `psql`, wrapper with `--single-transaction` keeps session read-only | Driver-created session already in autocommit read-only mode; attempts to switch raise SQLSTATE 25001/25006 | Covered by `test_postgresql_cli_blocks_transaction_control` and `test_postgresql_python_blocks_write_statements` |
| `LOCK TABLE`, `UNLOCK` (via `LOCK TABLE ...`) | Read-only transaction prevents locks that imply write access | Same | Covered by `test_postgresql_cli_blocks_lock_statements` and `test_postgresql_python_blocks_write_statements` |

### Data Definition Language (DDL)

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `CREATE TABLE`, `CREATE TABLE AS`, `CREATE TEMP TABLE`, `CREATE UNLOGGED TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE VIEW`, `CREATE SEQUENCE`, `CREATE TYPE`, `CREATE DOMAIN`, `CREATE EXTENSION`, `CREATE SCHEMA`, `CREATE TABLESPACE`, `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE INDEX CONCURRENTLY`, `CREATE POLICY`, `CREATE RULE`, `CREATE TRIGGER`, `CREATE FUNCTION`, `CREATE PROCEDURE`, `CREATE AGGREGATE`, `CREATE OPERATOR`, `CREATE OPERATOR CLASS`, `CREATE OPERATOR FAMILY`, `CREATE TRANSFORM`, `CREATE CAST`, `CREATE COLLATION`, `CREATE TEXT SEARCH CONFIGURATION/PARSER/DICTIONARY/TEMPLATE`, `CREATE LANGUAGE`, `CREATE ROLE`, `CREATE USER`, `CREATE GROUP`, `CREATE PUBLICATION`, `CREATE SUBSCRIPTION`, `CREATE SERVER`, `CREATE USER MAPPING`, `CREATE FOREIGN DATA WRAPPER`, `CREATE FOREIGN TABLE`, `CREATE STATISTICS`, `CREATE EVENT TRIGGER`, `CREATE ACCESS METHOD`, `CREATE MATERIALIZED VIEW`, `CREATE POLICY`, `CREATE DATABASE`, `CREATE ROUTINE` (alias) | Single-statement enforcement; read-only transaction blocks catalog changes | Read-only session prevents all catalog mutations, including `CREATE DATABASE` and role creation | Covered by `test_postgresql_cli_blocks_create_statements` and `test_postgresql_python_blocks_write_statements` |
| `ALTER TABLE`, `ALTER TABLE ALL IN TABLESPACE`, `ALTER TABLESPACE`, `ALTER DATABASE`, `ALTER INDEX`, `ALTER MATERIALIZED VIEW`, `ALTER VIEW`, `ALTER SEQUENCE`, `ALTER TYPE`, `ALTER DOMAIN`, `ALTER FUNCTION`, `ALTER PROCEDURE`, `ALTER ROUTINE`, `ALTER RULE`, `ALTER TRIGGER`, `ALTER POLICY`, `ALTER TABLE ... RENAME`, `ALTER TABLE ... SET SCHEMA`, `ALTER STATISTICS`, `ALTER TEXT SEARCH CONFIGURATION/PARSER/DICTIONARY/TEMPLATE`, `ALTER LANGUAGE`, `ALTER AGGREGATE`, `ALTER COLLATION`, `ALTER CONVERSION`, `ALTER EVENT TRIGGER`, `ALTER EXTENSION`, `ALTER FOREIGN DATA WRAPPER`, `ALTER FOREIGN TABLE`, `ALTER LARGE OBJECT`, `ALTER SERVER`, `ALTER USER MAPPING`, `ALTER ROLE`, `ALTER USER`, `ALTER GROUP`, `ALTER SUBSCRIPTION`, `ALTER PUBLICATION`, `ALTER DEFAULT PRIVILEGES`, `ALTER SYSTEM` | Transaction remains read-only; attempts to alter raise SQLSTATE 25006 | Same | Covered by `test_postgresql_cli_blocks_alter_statements` and `test_postgresql_python_blocks_write_statements` |
| `DROP TABLE`, `DROP TABLE ... CASCADE`, `DROP SCHEMA`, `DROP DATABASE`, `DROP VIEW`, `DROP MATERIALIZED VIEW`, `DROP INDEX`, `DROP SEQUENCE`, `DROP TYPE`, `DROP DOMAIN`, `DROP TRIGGER`, `DROP RULE`, `DROP POLICY`, `DROP FUNCTION`, `DROP PROCEDURE`, `DROP ROUTINE`, `DROP AGGREGATE`, `DROP OPERATOR`, `DROP OPERATOR CLASS`, `DROP OPERATOR FAMILY`, `DROP CAST`, `DROP COLLATION`, `DROP CONVERSION`, `DROP EXTENSION`, `DROP EVENT TRIGGER`, `DROP TEXT SEARCH CONFIGURATION/PARSER/DICTIONARY/TEMPLATE`, `DROP TRANSFORM`, `DROP TABLESPACE`, `DROP SERVER`, `DROP USER MAPPING`, `DROP FOREIGN DATA WRAPPER`, `DROP FOREIGN TABLE`, `DROP STATISTICS`, `DROP ACCESS METHOD`, `DROP PUBLICATION`, `DROP SUBSCRIPTION`, `DROP ROLE`, `DROP USER`, `DROP GROUP`, `DROP OWNED` | Read-only transaction blocks drops | Same | Covered by `test_postgresql_cli_blocks_drop_statements` and `test_postgresql_python_blocks_write_statements` |
| `RENAME` variants (`ALTER ... RENAME`, `RENAME COLUMN`, `ALTER INDEX ... RENAME`) | Blocked by read-only transaction | Same | Covered by `test_postgresql_cli_blocks_alter_statements` and `test_postgresql_python_blocks_write_statements` |

### Maintenance & Utility Commands

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `ANALYZE`, `VACUUM`, `VACUUM FULL`, `CLUSTER`, `REINDEX`, `REFRESH MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW CONCURRENTLY`, `CHECKPOINT`, `DISCARD`, `LOAD`, `COMMENT`, `SECURITY LABEL`, `GRANT`, `REVOKE`, `GRANT ... WITH ADMIN OPTION`, `REASSIGN OWNED`, `IMPORT FOREIGN SCHEMA`, `NOTIFY` (writes to WAL) | Read-only transaction / `default_transaction_read_only=on` rejects maintenance that writes catalog or data; configuration-changing `SET` commands restricted by read-only transaction (only safe `SET` allowed) | Same | Covered by `test_postgresql_cli_blocks_maintenance_statements` and `test_postgresql_python_blocks_write_statements` |
| `DO`, `CALL`, `EXECUTE`, `PREPARE`, `DECLARE`/`FETCH`, `LISTEN`, `LOAD`, `SET`, `RESET` | AST guard refuses by statement class; `DO` in particular can run `COPY ... TO PROGRAM` inside a read-only transaction and is never sent | Same | Covered by `test_sql_guard_postgresql.py` and `test_postgresql_cli_blocks_procedural_statements` |

### Function Calls Inside SELECT

| Functions | CLI enforcement | Python enforcement | Tests |
|-----------|-----------------|--------------------|-------|
| Signal and admin: `pg_terminate_backend`, `pg_cancel_backend`, `pg_reload_conf`, `pg_rotate_logfile`, `pg_promote`, `pg_switch_wal`, `pg_stat_reset*` | Not on the allow-list (PostgreSQL marks them `VOLATILE`); refused before execution | Same | Covered by `test_sql_guard_postgresql.py` |
| Server files and large objects: `pg_read_file`, `pg_read_binary_file`, `pg_ls_dir`, `pg_stat_file`, `lo_export`, `lo_import` | Same | Same | Same |
| Session and transaction state: `set_config`, `txid_current`, `pg_current_xact_id`, `pg_export_snapshot`, advisory locks, `nextval`/`setval`, `pg_notify` | Same (`txid_current`/`pg_current_xact_id` are `STABLE` and excluded by hand) | Same | Same |
| SQL-executing helpers: `query_to_xml*`, `cursor_to_xml*`, `ts_rewrite(tsquery, text)`, `ts_stat`, `dblink*` | Same (`ts_rewrite` is excluded by hand; `dblink` lives outside `pg_catalog`) | Same | Same |
| User-defined and extension functions (`public.f()`, `f()` not in `pg_catalog`) | Refused unless listed in the connection's `allowed_functions` | Same | Covered by `test_allowed_functions_extend_the_policy_per_connection` |
| Shadowing through `search_path`: a `public.length(text)` or `public.@@@` planted by another database user, invoked as the bare `length(...)` / `1 @@@ 2` | `postgresql_shadow_query` lists the query's bare names; a `DO` block in the psql transaction asks `pg_proc`/`pg_operator` for non-`pg_catalog` definitions visible via `current_schemas(true)` and raises before the query runs. Name-based on purpose: a visible `public.upper(integer)` refuses `upper('x')` although PostgreSQL would pick the catalog overload; the error names the offender and the qualified spelling. A `schema.name` entry in `allowed_functions` pins the bare call to that schema | Same check as a plain `SELECT` before the query | Covered by `test_shadow_query_*`, `test_postgresql_cli_runs_shadow_guard_and_surfaces_it`, `test_postgres_real_refuses_names_shadowed_on_search_path`, `test_postgres_real_overload_of_catalog_name_is_refused_conservatively` and `test_postgres_real_qualified_allowance_pins_bare_call_to_its_schema` against planted fixtures |

### Additional Notes

- Stored procedures declared `VOLATILE` may attempt writes; both connectors rely on
  PostgreSQL’s read-only transaction enforcement to reject such actions.
- `SET`, `SET ROLE` and `SET SESSION AUTHORIZATION` are refused by statement
  class. They would be pointless as a single statement anyway, and
  `set_config()` is off the function allow-list for the same reason.
- `EXPLAIN ANALYZE` is accepted: it executes only the (already validated)
  statement it wraps.
- Queries the PostgreSQL 18 grammar bundled with `pglast` cannot parse are
  refused client-side; the error carries the parser's position.

---

## ClickHouse

**Connector recap**

- CLI: executes `clickhouse-client` with `--readonly=1`, `--max_execution_time`,
  and `--connect_timeout`. SSH tunnelling adjusts ports but leaves the server in
  read-only mode.
- Python: uses `clickhouse_connect.get_client(..., settings={'readonly': 1,
  'max_execution_time': query_timeout})`. Requests are executed via HTTP/HTTPS
  (or tunneled) and ClickHouse enforces read-only semantics.
- Both: a login whose profile already sets `readonly` (1 or 2) refuses these
  client-side settings by name (`Cannot modify '<setting>' setting in
  readonly mode`, or clickhouse-connect's `Setting <name> is readonly`).
  Each connector probes once per connector instance, without the caller's
  statement (`SELECT 1` for the CLI, the client construction for
  clickhouse-connect), drops only the refused setting and remembers the
  result. A statement is never re-run with weaker settings: its own
  `SETTINGS` clause produces the same refusal text, and re-running it
  without `readonly=1` would run it unguarded. Covered by
  `tests/test_clickhouse_readonly_profiles.py` against fixture users
  `readonly_user` (readonly=1, with the `URL`, `CREATE TEMPORARY TABLE` and
  `INSERT` grants so the profile alone is what refuses `url()` and writes)
  and `readonly2_user`, plus a write with a `SETTINGS` clause on the
  full-privilege login.

### Data Manipulation & Mutations

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `INSERT`, `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT`, `INSERT INTO ... FORMAT`, `INSERT WITH PARTITION`, `OPTIMIZE TABLE ... FINAL` (mutating), `ALTER TABLE ... UPDATE`, `ALTER TABLE ... DELETE`, `ALTER TABLE ... MATERIALIZE COLUMN`, `ALTER TABLE ... MATERIALIZE STATISTICS`, `ALTER TABLE ... MATERIALIZE INDEX`, `ALTER TABLE ... CLEAR COLUMN`, `ALTER TABLE ... CLEAR INDEX`, `ALTER TABLE ... CLEAR PROJECTION`, `ALTER TABLE ... FREEZE` (writes snapshot), `ALTER TABLE ... UNFREEZE`, `ALTER TABLE ... MOVE PART`, `ALTER TABLE ... MOVE PARTITION`, `ALTER TABLE ... ATTACH PART`, `ALTER TABLE ... DETACH PART`, `ALTER TABLE ... FETCH PART`, `ALTER TABLE ... REPLACE PARTITION`, `ALTER TABLE ... DROP PART`, `ALTER TABLE ... DROP PARTITION`, `ALTER TABLE ... ATTACH PARTITION`, `ALTER TABLE ... DETACH PARTITION`, `ALTER TABLE ... CLEAR COLUMN`, `ALTER TABLE ... MODIFY TTL`, `ALTER TABLE ... RENAME`, `ALTER TABLE ... ADD COLUMN`, `ALTER TABLE ... DROP COLUMN`, `ALTER TABLE ... MODIFY COLUMN`, `ALTER TABLE ... COMMENT COLUMN`, `ALTER TABLE ... ORDER BY`, `ALTER TABLE ... SAMPLE BY`, `ALTER TABLE ... ADD|DROP CLUSTER BY`, `ALTER TABLE ... SETTINGS`, `ALTER TABLE ... RESET SETTINGS`, `ALTER TABLE ... REMOVE TTL`, `ALTER TABLE ... MATERIALIZE TTL`, `ALTER TABLE ... DELETE WHERE`, `ALTER TABLE ... UPDATE ... WHERE`, `ALTER TABLE ... MATERIALIZE ORDER BY`, `ALTER TABLE ... MATERIALIZE COLUMN`, `ALTER TABLE ... CONVERT`, `ALTER TABLE ... FREEZE PARTITION`, `ALTER TABLE ... UNFREEZE PARTITION` | `--readonly=1` makes `clickhouse-client` reject every mutation with error `READONLY` 164; mutations never run | `settings={'readonly': 1}` blocks mutations identically | Covered by `test_clickhouse_cli_blocks_mutations` and `test_clickhouse_python_blocks_mutations` |

### Data Definition & Dictionary Statements

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `CREATE DATABASE`, `ATTACH DATABASE`, `DETACH DATABASE`, `DROP DATABASE`, `ALTER DATABASE`, `RENAME DATABASE`, `CREATE TABLE`, `CREATE TABLE ... AS`, `CREATE TABLE ... ENGINE`, `CREATE TABLE ... TTL`, `CREATE TABLE ... SELECT`, `CREATE TABLE ... LIKE`, `CREATE TABLE ... ON CLUSTER`, `RENAME TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, `DETACH TABLE`, `ATTACH TABLE`, `EXCHANGE TABLES`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `CREATE LIVE VIEW`, `DROP VIEW`, `DROP MATERIALIZED VIEW`, `DROP LIVE VIEW`, `CREATE DICTIONARY`, `ALTER DICTIONARY`, `DROP DICTIONARY`, `RENAME DICTIONARY`, `CREATE FUNCTION`, `DROP FUNCTION`, `CREATE ROW POLICY`, `ALTER ROW POLICY`, `DROP ROW POLICY`, `CREATE ROLE`, `ALTER ROLE`, `DROP ROLE`, `GRANT`, `REVOKE`, `SET ROLE`, `SET DEFAULT ROLE`, `CREATE USER`, `ALTER USER`, `DROP USER`, `CREATE QUOTA`, `ALTER QUOTA`, `DROP QUOTA`, `CREATE SETTINGS PROFILE`, `ALTER SETTINGS PROFILE`, `DROP SETTINGS PROFILE`, `CREATE RESOURCE GROUP`, `ALTER RESOURCE GROUP`, `DROP RESOURCE GROUP`, `CREATE NAMED COLLECTION`, `ALTER NAMED COLLECTION`, `DROP NAMED COLLECTION`, `CREATE LOG`, `DROP LOG` | `--readonly=1` disallows DDL; ClickHouse returns `Cannot execute <command> in readonly mode` | Same via driver settings | Covered by `test_clickhouse_cli_blocks_ddl` and `test_clickhouse_python_blocks_mutations` |

### System & Administrative Commands

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `SYSTEM RELOAD CONFIG`, `SYSTEM DROP QUERY CACHE`, `SYSTEM DROP DNS CACHE`, `SYSTEM RELOAD DICTIONARIES`, `SYSTEM RELOAD MODELS`, `SYSTEM RELOAD SIZES`, `SYSTEM START/STOP MERGES`, `SYSTEM START/STOP TTL MERGES`, `SYSTEM DROP REPLICA`, `SYSTEM SYNC REPLICA`, `SYSTEM FLUSH LOGS`, `SYSTEM FLUSH DISTRIBUTED`, `SYSTEM START DISTRIBUTED SENDS`, `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START FETCHES`, `SYSTEM STOP FETCHES`, `SYSTEM START REPLICATION QUEUES`, `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM UNFREEZE`, `SYSTEM DELETE OLD TEMPORARY DIRECTORIES`, `SYSTEM SHUTDOWN`, `SYSTEM KILL`, `SYSTEM SUSPEND`, `SYSTEM RESUME`, `SYSTEM DROP FILESYSTEM CACHE`, `SYSTEM DROP MARK CACHE`, `SYSTEM DROP UNCOMPRESSED CACHE`, `SYSTEM DROP COMPILED EXPRESSION CACHE`, `SYSTEM DROP MMSCACHE`, `SYSTEM DROP QUERY CACHE`, `SYSTEM DROP MUTATION` | `--readonly=1` rejects `SYSTEM` commands impacting state | Same | Covered by `test_clickhouse_cli_blocks_system_commands` and `test_clickhouse_python_blocks_mutations` |
| `KILL QUERY`, `KILL MUTATION` | Rejected because readonly | Same | Covered by `test_clickhouse_cli_blocks_kill_statements` and `test_clickhouse_python_blocks_mutations` |
| `SET` (session), `RESET`, `USE`, `SET SETTINGS PROFILE`, `SET ROLE`, `SET QUOTA`, `SET DEFAULT ROLE`, `SET SESSION`, `SET PROFILE` | Session-affecting statements allowed unless ClickHouse forbids in readonly; no data mutation | Allowed | **Optional** |

### Miscellaneous

| Statements | CLI enforcement | Python enforcement | Tests |
|------------|-----------------|--------------------|-------|
| `ALTER LIVE VIEW`, `REFRESH VIEW`, `WATCH` (streaming), `CHECK TABLE`, `CHECK PARTITION`, `EXISTS`, `SHOW TABLES`, `DESCRIBE`, `EXPLAIN`, `EXCHANGE`, `CALL` (table functions) | Mutating variants blocked; read-only mode allows introspection commands | Same | `WATCH`, `CHECK`, etc. **Needs coverage** |

### Additional Notes

- ClickHouse’s `readonly=1` prohibits any statement that changes data, metadata,
  or configuration except for session-level `SET`. Even commands not explicitly
  listed above (e.g., new DDL syntax) are covered by the server setting.
- Mutations queued via `ALTER TABLE ... UPDATE/DELETE` never start because the
  server rejects the initial statement; no background processing occurs.
- Advanced PostgreSQL attack patterns such as multi-statement payloads (`COMMIT; INSERT ...`),
  `COPY ... PROGRAM`, `MERGE`, procedural `DO`/`CALL`, transaction
  manipulation (`PREPARE TRANSACTION`, `ROLLBACK PREPARED`), and
  side-effecting function calls inside a plain `SELECT` are enumerated in
  the tables above and asserted by `tests/test_sql_guard_postgresql.py`,
  `test_postgresql_cli_blocks_write_statements`,
  `test_postgresql_cli_blocks_transaction_control`, and
  `test_postgresql_cli_blocks_procedural_statements` (with matching Python
  connector tests and live Docker coverage). Similar high-impact ClickHouse
  commands—including `ALTER ... UPDATE/DELETE`, partition management, and
  `SYSTEM`/`KILL` operations—are checked by the ClickHouse suites.

---

## Test Coverage Summary

- PostgreSQL coverage is provided by `tests/test_sql_guard_postgresql.py` (the AST policy: allowed shapes, every enumerated write statement, read-only-transaction escapes, `allowed_functions`) and the shared parameterized connector suites (`test_postgresql_cli_blocks_*` and `test_postgresql_python_blocks_write_statements`). `test_postgres_real_server_rejects_mutations_without_client_guard` bypasses the guard against Docker PostgreSQL to prove the read-only transaction still holds on its own.
- ClickHouse coverage mirrors this approach through `test_clickhouse_cli_blocks_*` and `test_clickhouse_python_blocks_mutations`, asserting the `readonly=1` guard across DML, DDL, and `SYSTEM`/`KILL` commands.
- Integration tests with live databases remain valuable for end-to-end validation, but unit tests now ensure each statement category is wired to fail fast in read-only mode.
