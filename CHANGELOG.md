# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Both ClickHouse connectors now decide the client-side `readonly=1` and
  `max_execution_time` settings from `system.settings`, read once per server
  with no client-side settings, instead of sending them and dropping
  whichever one the server refused. `system.settings` is readable by every
  login; a setting the profile locks is left out, and a login whose profile
  locks `readonly` at 0 is refused outright with a clear error. The
  clickhouse-connect client is created without settings and receives the
  decided ones with each query.
- The psycopg2 PostgreSQL connector runs every statement inside one explicit
  transaction that starts with `SET TRANSACTION READ ONLY; SET LOCAL
  statement_timeout = ...`, the shape the psql connector already used,
  instead of session-level `set_session(readonly=True, autocommit=True)` and
  `SET statement_timeout`. Behind a transaction pooler such as PgBouncer the
  session-level guards could land on a different server connection than the
  query; the transaction-scoped ones cannot. The startup option
  `default_transaction_read_only=on` is still requested and, as in the CLI,
  dropped only when the server rejects the startup parameter.
- SELECT-shaped statements on that connector are read through a server-side
  cursor in batches of 1000 rows, so a large result streams to the result
  file instead of being loaded into memory first. EXPLAIN and SHOW, which
  cannot be declared as cursors, run on a plain cursor.
- When a query on that connector times out, the worker is told to stop at
  its next fetch or row and is waited for, so the result file is not written
  to after the caller has removed it. The wait is bounded by the connection
  timeout, so a server that stops answering cannot hold the caller. A
  statement already running is left to its server-side `statement_timeout`,
  which is now capped at the hard timeout so the server ends it by the
  caller's deadline.
- The Docker test fixtures run PostgreSQL 17 and ClickHouse 26.3, the
  versions the connectors are used against, instead of PostgreSQL 16 and
  ClickHouse 24.8. The test profile keeps its data on tmpfs, so nothing
  carries over. The dev and prod profiles keep a `postgres_data` volume,
  and a PostgreSQL 16 data directory does not start on 17. That volume
  holds the seeded sample data, which the init scripts recreate on a
  fresh volume; if you added data of your own, carry it over like this.
  With the old image still running, take a dump that drops and recreates
  every object, since the init scripts will have seeded the new database
  before the dump is restored:
  `docker compose --profile dev exec -T postgres pg_dump -U testuser --clean --if-exists testdb > dump.sql`.
  Remove the old container without deleting its volume
  (`docker compose --profile dev rm --stop --force postgres`), then remove
  the volume (`docker volume rm <project>_postgres_data`). Start the new
  image and restore in one transaction with errors fatal:
  `docker compose --profile dev exec -T postgres psql -U testuser -d testdb -v ON_ERROR_STOP=1 --single-transaction < dump.sql`.
  A plain dump restored into the seeded database fails on the existing
  tables and rows and leaves your data out. ClickHouse upgrades its
  `clickhouse_data` volume in place.
- The psql connector's `statement_timeout` is capped at the hard timeout, so
  a statement psql is killed away from at the hard timeout is ended by the
  server by then as well.
- Both CLI connectors now kill and reap their client process when the call
  is cancelled, which is what the hard timeout does. Before, psql or
  clickhouse-client kept running the statement after the caller had
  already returned an error.

## [0.6.0] - 2026-09-29

### Security

- SSH tunnels now verify the bastion's host key. Both implementations
  trusted any key (`StrictHostKeyChecking=no` with no known_hosts file for
  system `ssh`, Paramiko `AutoAddPolicy` with no loaded keys), so a spoofed
  bastion would have received the database credentials. The new
  `ssh_tunnel.host_key_checking` field takes the OpenSSH values and defaults
  to `yes`: the bastion's key must already be in `/etc/ssh/ssh_known_hosts`,
  `~/.ssh/known_hosts`, or the file `ssh_tunnel.known_hosts_file` names.
  `accept-new` is the explicit trust-on-first-use mode: the bastion is
  recorded on first use and refused if its key changes. Since `ssh` only
  warns when it cannot save the key, the tunnel checks the chosen files
  before and after connecting and fails if a new key was not recorded.
  It passes those files to `ssh` explicitly; configure `known_hosts_file`
  to use a custom user file in this mode. A bastion covered only by an
  `@cert-authority` entry is always connected to strictly. `no` restores
  the previous behaviour and records nothing.

### Changed

- Every connector tunnels through the system `ssh`. The Python
  implementations used a Paramiko tunnel, whose host-key handling matched
  names literally, knew no `@revoked` or `@cert-authority` markers, held one
  key per host and type, and could not verify host certificates; each of
  those had to be re-implemented by hand. `ssh` does all of it. Consequences
  for the Python implementations: `ssh` must be installed,
  `ssh_tunnel.password` needs `sshpass` as it already did for the CLI
  implementations, tunnel startup allows 30 seconds instead of 5 for
  interactive prompts, and host certificates and `~/.ssh/config` now apply.
  Paramiko is no longer a dependency.
- README describes the actual enforcement layers (client-side statement
  policy, database-level read-only, timeouts) and no longer counts result
  files as one. The implementation matrix says that the Python PostgreSQL
  path does not stream: psycopg2 loads the result before it is written.
- CI also runs weekly, so drift in unpinned dependencies surfaces without a
  push, and the uv cache key hashes `pyproject.toml` instead of the
  gitignored `uv.lock`.

### Removed

- The `mcp_read_only_sql.utils.ssh_tunnel` module and its `SSHTunnel`
  class (the Paramiko tunnel). `mcp_read_only_sql.utils.ssh_tunnel_cli`
  is the one tunnel.
- An SSH tunnel to a bastion whose host key is not in a known_hosts file
  is refused with an `SSH:` error, as is one whose key changed. Bastions you
  have connected to with `ssh` before are already known; for the others,
  fetch the key with `ssh-keyscan`, check its fingerprint against a trusted
  source, and append it, or set `host_key_checking: accept-new` to trust
  them on first use.
- Dead code: the `json_serializer` module, `format_as_tsv`,
  `HardTimeoutMixin` and the `hard_timeout` decorator,
  `ConnectionTimeoutError`, the connectors' unused `_get_default_port`
  hooks, `ConfigParser.save_config`, two write-only DBeaver importer
  attributes, and two `list_connections` fields that were computed but never
  rendered. None of it was reachable from the server or the CLI.
- Test leftovers: the root `conftest.py` re-export shim (tests import
  `tests.conftest` directly), `tests/conftest_new.py`, the never-registered
  `tests/pytest_plugins.py`, `tests/KNOWN_ISSUES.md` (it described anyio
  teardown errors the suite no longer produces, and the plugin written to
  suppress them), and stale references to a `test_concurrent_queries.py`
  that no longer exists. The test README's Docker instructions name the
  `test` profile, without which no service starts.

### Fixed

- The psql connector no longer drops rows. It parsed psql's output and
  skipped any line equal to `BEGIN`, `SET`, `DO`, `COMMIT` or `ROLLBACK`,
  any line shaped like `(N rows)`, and a trailing empty line, so a row with
  one of those values disappeared and a single-column result whose last row
  was NULL or empty lost that row. psql now runs with `-q` and `--csv` with
  a tab separator, which keeps command tags and the row count off stdout, and
  every line is returned as data. The command string no longer carries its
  own `BEGIN`/`COMMIT` inside `--single-transaction`, which removed two
  warnings per query. psql 12 or newer is required for the CSV mode.
- PostgreSQL values containing a tab or a line break no longer corrupt the
  result. psql's unaligned mode printed them raw, so a tab shifted the
  following columns and a line break split the row; CSV mode quotes such
  values, and the Python connector's formatter now follows the same rules
  (NULL and the empty string are empty, a value containing a tab, a double
  quote or a line break is double-quoted with inner quotes doubled), so both
  implementations produce the same bytes for the same row. The connector
  also strips only the record terminator, so a value ending in a carriage
  return keeps it.
- The Python PostgreSQL connector returns duplicate column names correctly.
  It used a dict cursor, so `SELECT 1 AS a, 2 AS a` came back as `2 2`; it
  now uses a plain tuple cursor like psql does.
- `statement_timeout` is sent as an integer number of milliseconds. A
  fractional `query_timeout` rendered as `2500.0`, which PostgreSQL 11 and
  older reject.
- ClickHouse logins whose profile already enforces `readonly` work again.
  Both connectors send `readonly=1` and `max_execution_time` with every
  query, and such a profile refuses them (`Cannot modify '<setting>' setting
  in readonly mode`; clickhouse-connect refuses them client-side as
  `Setting <name> is readonly`), so every query failed for exactly the logins
  the README recommends. Each connector now finds out once, with a probe that
  does not involve the caller's statement, which settings the login accepts,
  drops only the refused ones, logs that, and remembers the answer. A
  statement is never re-run with weaker settings: its own `SETTINGS` clause
  produces the same refusal text, and re-running it without `readonly=1`
  would run it unguarded. Fixture users `readonly_user` and `readonly2_user`
  cover both profile values.
- The clickhouse-client connector no longer drops a last row that renders
  empty (an empty string in the only column). It held back each line until
  the next arrived and discarded the final one when it was empty.
- clickhouse-client errors no longer start with the `Password for user (x):`
  prompt that `--ask-password` prints to stderr.

## [0.5.1] - 2026-09-25

### Changed

- Narrowed the SDK dependency from `mcp>=2.0.0,<3` to `mcp>=2.2.0,<2.3`.
  Installs resolve the newest version allowed, so mcp 2.1.0 reached users
  untested and hid tool error messages until 0.5.0. The cap now admits only
  the minor the test suite runs against; raise it deliberately after
  testing the next one.

## [0.5.0] - 2026-09-25

### Security

- PostgreSQL queries are now parsed with PostgreSQL's own grammar (`pglast`)
  and checked against an allow-list before execution: only `SELECT`, `EXPLAIN`
  and `SHOW` statement shapes are accepted, `COPY` is refused in every form,
  and every function call must be a `pg_catalog` function PostgreSQL declares
  `IMMUTABLE` or `STABLE` (plus a short reviewed list of read-only volatile
  ones). A read-only transaction alone does not stop `COPY ... TO PROGRAM`,
  `DO` blocks, or calls such as `pg_terminate_backend()`, `pg_read_file()`,
  `lo_export()` or `set_config()`; with a superuser login those reach the
  host. Rejections happen client-side with a message naming the function or
  statement. Both implementations share the guard; ClickHouse is unchanged.
- Bare function and operator names resolve through `search_path`, so a
  `public.length(text)` or `public.@@@` planted by another database user
  would run in place of the catalog one. Before each PostgreSQL query the
  connectors now ask the server whether any bare name the query uses has a
  non-`pg_catalog` definition visible to the session, and refuse the query
  if so. The check is by name, so a visible overload such as
  `public.length(integer)` refuses `length('abc')` too; the error says to
  qualify the call or list the function.
- New per-connection `allowed_functions` list (PostgreSQL only) extends the
  allow-list with bare or schema-qualified function names. A `schema.name`
  entry also permits the bare call, pinned to that schema by the shadow
  check; a bare entry trusts whatever the name resolves to.
- The enforcement matrix no longer claims `COPY ... PROGRAM` was blocked by
  the read-only session; it is blocked by the new guard.

### Fixed

- Tool errors reach the caller again. Since mcp SDK 2.x, any exception other
  than `ToolError` raised inside a tool is treated as a crash and reported as
  the generic `Error executing tool <name>`, hiding the actual reason (unknown
  connection, unreachable host, rejected statement). The operational failure
  types (`ValueError`, the new `ConnectorError`, `TimeoutError`, `OSError`,
  `HardTimeoutError`) are now re-raised as `ToolError`, so the result carries
  the underlying message after the SDK prefix. Programming errors keep the
  SDK's crash handling: generic text to the caller, traceback in the server
  log.
- Connectors and the SSH tunnels raise `ConnectorError` (a `RuntimeError`
  subclass) for driver errors, non-zero client exits, and SSH failures, and
  no longer wrap arbitrary exceptions with a `psql:`/`clickhouse-client:`/
  `SSH:` prefix. Only process-spawn and socket failures (`OSError`) are wrapped;
  anything else propagates as the bug it is.
- Non-string `connection_name`, `username`, `description`, or server `host`
  values in `connections.yaml` are rejected when the config loads, with the
  connection named, instead of failing `list_connections` with a generic
  error.

### Changed

- Dev tooling: pinned `ruff>=0.16,<0.17` in the dev extra (uv.lock is
  gitignored, so CI previously linted with whatever ruff was latest), widened
  the CI lint scope from `src/ tests/` to `ruff check .` to match the
  RELEASING.md gate, and removed the unused `black` dev dependency.
- Adopted ruff 0.16's default rule set (the minor-version pin keeps that
  implicit set deterministic) with two documented opt-outs: `BLE001` (broad
  `except Exception` at MCP tool boundaries is this server's documented
  design) and `TRY004` (raised exception types are observable API behavior,
  out of scope for a lint pass). The code was modernized accordingly, with no
  behavior change: PEP 585/604 annotations (`list[str]`, `X | None`), sorted
  imports, `TimeoutError`/`OSError` instead of their pre-3.11 aliases,
  `contextlib.suppress` for intentional swallow-and-continue cleanup,
  explicit `check=False` on `subprocess.run` calls, combined nested `with`
  statements, and removal of stray shebang lines from modules that are only
  ever imported or invoked via the console script. A dead duplicate
  `except OSError` handler in `utils/ssh_tunnel.py` (unreachable since
  `socket.error`/`IOError` are `OSError` aliases) was dropped. Intentional
  naive-`datetime` uses (backup-filename timestamps, serialization tests) and
  the connectors' local blocking file writes carry per-line `noqa` markers
  instead of blanket ignores.
- Fixed the five `ty check` diagnostics so the documented pre-release type
  gate passes again: typed `list_connections`' row-building dict in
  `server.py`, and the DBeaver dry-run diff preview in
  `config/dbeaver_import.py` now keys its comparison maps only by string
  `connection_name` values (a non-string name could previously crash the
  preview's `sorted()` call).

## [0.4.0] - 2026-08-03

### Changed

- Ported the server from the MCP Python SDK 1.x `FastMCP` API to the 2.x
  `MCPServer` API. `mcp.server.fastmcp.FastMCP` was replaced by
  `mcp.server.mcpserver.MCPServer`; the SDK 2.0.0 release removed
  `mcp.server.fastmcp` outright and ships no compatibility shim, so this is a
  hard cutover rather than an optional upgrade. The tool bodies, their
  signatures, and their docstrings are unchanged.
- Raised the SDK dependency to `mcp>=2.0.0,<3`, replacing the temporary
  `mcp>=1.10.0,<2` pin added in 0.3.1. The upper cap is kept so that the next
  major SDK rewrite cannot silently break fresh installs the way 2.0.0 did when
  it removed `mcp.server.fastmcp`.
- `serverInfo.version` in the `initialize` response now reports this package's
  version (`0.4.0`) instead of the MCP SDK's version. Under 1.x `FastMCP` filled
  that field with the SDK version (for example `1.29.0`), which was never the
  intent; SDK 2 leaves it empty unless the server passes its own version, and it
  is now passed explicitly.
- The advertised tool surface is unchanged. `tools/list` output from the built
  wheel is byte-identical to the 0.3.1 output: the same two tools
  (`list_connections`, `run_query_read_only`) with identical descriptions,
  `inputSchema`, and `outputSchema` — including the `*Arguments` / `*Output`
  schema titles.
- Test-only: the in-process tool-manager helpers now pass the `Context` argument
  that `ToolManager.call_tool` requires in SDK 2 (it was optional in 1.x), and
  the `CallToolResult` error flag is read as `is_error`, the SDK 2 attribute name
  for the unchanged `isError` wire field.

## [0.3.1] - 2026-08-03

### Fixed

- Constrained the MCP Python SDK dependency to `mcp>=1.10.0,<2`. The SDK's 2.0.0 release (2026-07-28) removed `mcp.server.fastmcp`, which this server imports, so any fresh install resolving to 2.x crashed on startup with `ModuleNotFoundError: No module named 'mcp.server.fastmcp'` and the server never connected. The previous floor of `>=1.0.0` was also wrong in the other direction: `mcp.server.fastmcp` only appeared in 1.2.0, and FastMCP only emits `outputSchema` / structured content from 1.10.0, so older 1.x resolves either crashed identically or started with the tools' declared output schemas silently missing. The cap stays until the server is ported to the 2.x API.

### Changed

- Declared the ruff rule set explicitly as `select = ["E4", "E7", "E9", "F"]`, the set this tree is written against. `uv.lock` is not committed here and the dev extra tracks the latest ruff, so ruff 0.16 widening its implicit defaults would otherwise fail the CI lint step on unchanged code. Development-only; no runtime effect.

## [0.3.0] - 2026-06-08

### Added

- SSH tunnels accept configurations without `private_key` or `password`. When
  neither is supplied the Python implementation lets paramiko fall back to
  ssh-agent and `~/.ssh/*` discovery, and the CLI implementation invokes
  system `ssh` with no `-i` flag, so agent-loaded identities and matching
  identity options can be used. The configured SSH host, user, and port are
  still passed explicitly.
- CLI SSH tunnel startup now defaults to 30 seconds to accommodate system
  `ssh` interactive approval flows. Python/Paramiko startup keeps its 5 second
  default; set `ssh_tunnel.ssh_timeout` lower when fail-fast behavior is
  preferred for unreachable bastions.

## [0.2.6] - 2026-06-08

### Fixed

- Resolved the `psql` / `clickhouse-client` CLI binaries through an explicit lookup (env override → `PATH` → OS-aware fallback) instead of relying solely on `PATH`. Homebrew keg-only `libpq` installs on macOS, where `psql` is not symlinked onto `PATH`, now work without manual `PATH` setup. The resolved path can be pinned with `MCP_READ_ONLY_SQL_PSQL_PATH` / `MCP_READ_ONLY_SQL_CLICKHOUSE_CLIENT_PATH`, and is cached per connector.

## [0.2.5] - 2026-04-21

### Added

- Added hot-reload regression tests covering connection add/change/remove flows, invalid live edits, and config changes that happen during a reload attempt.

### Fixed

- Reloaded `connections.yaml` automatically before both `list_connections` and `run_query_read_only`, without requiring an MCP server restart.
- Kept hot-reload state atomic by building connectors from a single file snapshot and only storing a config marker for the exact snapshot that was actually loaded.
- Preserved the last known good connections when live config edits are invalid or the config file is temporarily missing, while continuing to retry reloads on later tool calls.

## [0.2.2] - 2026-04-03

### Added

- Added `ty` as a supported development check for the full packaged `src/` tree.

### Changed

- Added repo-specific `AGENTS.md` guidance covering connector layout, shared timeout and SSH helpers, and the typed development workflow.
- Reworked `RELEASING.md` into an evergreen release checklist with explicit validation, tagging, and publish steps.

### Fixed

- Flushed the final buffered TSV line when PostgreSQL and ClickHouse CLI queries stream results to an output file.
- Hardened DBeaver credential import so missing or non-dictionary decrypted sections are ignored cleanly instead of being treated as valid connection data.

## [0.2.1] - 2026-04-02

### Added

- Root `CHANGELOG.md` using the Keep a Changelog format and seeded package history.

### Changed

- `project.urls.Changelog` now points to the in-repo changelog instead of the generic GitHub releases page.
- The release flow now treats changelog maintenance as a required step and reuses changelog sections for GitHub release notes.
- Breaking: `run_query_read_only` now always writes successful query results under the managed state directory and returns the TSV file path instead of inline query output.
- Breaking: removed the `file_path` tool parameter and `max_result_bytes` configuration/result-size limit behavior.

### Fixed

- Restored Python connector executor compatibility so non-file query execution no longer passes unexpected positional arguments to synchronous workers or test stubs after the managed result-file refactor.

## [0.1.0] - 2026-03-29

### Added

- Initial PyPI release for `uvx mcp-read-only-sql`.
- Canonical `src/mcp_read_only_sql` package layout and metadata-backed `__version__`.
- Root CLI subcommands for `import-dbeaver`, `validate-config`, `test-connection`, and `test-ssh-tunnel`.
- Package-native bootstrap commands for `--write-sample-config`, `--overwrite`, and `--print-paths`.
- Trusted PyPI publishing with a gated GitHub Actions release workflow and manual `pypi` approval.

### Changed

- Standardized the public CLI around the single `mcp-read-only-sql` command instead of separate top-level helper scripts.
- Kept both Python and external CLI connector modes as supported public workflows.
