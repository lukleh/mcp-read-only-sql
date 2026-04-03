# Repository Guidelines

## Project Structure & Module Organization
`src/mcp_read_only_sql/server.py` exposes the MCP entry point, while `src/mcp_read_only_sql/connectors/` hosts the PostgreSQL and ClickHouse adapters for both CLI and Python implementations. Cross-cutting helpers (`ssh_tunnel.py`, `ssh_tunnel_cli.py`, `sql_guard.py`, TSV formatting, timeout handling) live in `src/mcp_read_only_sql/utils/`. Configuration tooling, including DBeaver import and manifest validation, sits in `src/mcp_read_only_sql/config/` and `src/mcp_read_only_sql/tools/`. Tests are grouped in `tests/`, and Docker fixtures for integration runs reside in `docker/`. The package ships a sample `connections.yaml`, and runtime config lives under `~/.config/lukleh/mcp-read-only-sql/`.

## Build, Test, and Development Commands
- `uv sync --extra dev` — install runtime and development dependencies.
- `uv run mcp-read-only-sql` or `just run` — start the MCP server with the resolved runtime config directory.
- `uv run mcp-read-only-sql --write-sample-config` or `just write-sample-config` — bootstrap `connections.yaml` without cloning-path assumptions.
- `uv run mcp-read-only-sql import-dbeaver ...` or `just import-dbeaver` — convert a DBeaver workspace into `connections.yaml`.
- `just validate` — lint `connections.yaml` against the schema and safety checks.
- `just test` — spin up Dockerized fixtures and execute the full pytest suite via `./run_tests.sh`.
- `uv run python -m pytest tests/test_sql_guard.py` — run an individual module when iterating quickly.
- `uv run ruff check .` and `uv run black .` — run linting and formatting for the Python tree.
- `uv run ty check` — type-check the full `src/` tree; there are no remaining package excludes.

## Coding Style & Naming Conventions
Target Python 3.11+, four-space indentation, and Unix newlines. Format with `uv run black .`, lint via `uv run ruff check .`, and keep `uv run ty check` passing for `src/`. Modules and callables use snake_case, classes PascalCase (e.g., `TestReadOnlyGuards`), and immutable settings uppercase (`DEFAULT_QUERY_TIMEOUT`). Apply type hints on public surfaces and keep docstrings brief, emphasizing read-only guarantees and connector behavior.

## Testing Guidelines
Pytest discovers `test_*.py` modules, `Test*` classes, and `test_*` functions per `pytest.ini`. Use the built-in markers (`security`, `cli`, `python`, `ssh`, `slow`) to scope runs, e.g. `pytest -m "security and not slow"`. A 30s default timeout applies, so tear down tunnels and subprocesses explicitly. `just test` emits JUnit XML at `test-results/pytest.xml` for CI uploads.

## Commit & Pull Request Guidelines
Recent commits favour imperative, concise subjects (“Refactor PostgreSQL read-only guard into shared utility”). Keep changes focused and explain security or connector impacts in the body when needed. Pull requests should describe motivation, list affected modules or scripts, link issues, and attach logs or screenshots for operational updates.

## Security & Configuration Tips
Do not relax safeguards in `src/mcp_read_only_sql/utils/sql_guard.py` or connector factories without matching tests. Treat `connections.yaml` as sensitive; the server stores database and SSH passwords directly in that file, so keep it private and user-readable only. Use the shared SSH tunnel and timeout helpers instead of bespoke subprocesses so read-only enforcement, cleanup, and managed result-file behavior stay consistent.
