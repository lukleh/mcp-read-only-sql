# Repository Guidelines

## Project Structure & Module Organization
`src/server.py` exposes the MCP entry point, while `src/connectors/` hosts the PostgreSQL and ClickHouse adapters. Cross-cutting helpers (`ssh_tunnel.py`, `sql_guard.py`, TSV formatting) live in `src/utils/`. Configuration tooling, including DBeaver import and manifest validation, sits in `src/config/` and `src/tools/`. Tests are grouped in `tests/`, and Docker fixtures for integration runs reside in `docker/`. Connection manifests stay at the repo root (`connections.yaml`, with a scrubbed sample alongside).

## Build, Test, and Development Commands
- `uv sync` — install runtime and development dependencies.
- `uv run -- python -m src.server connections.yaml` or `just run` — start the MCP server with the selected manifest.
- `just validate` — lint `connections.yaml` against the schema and safety checks.
- `just test` — spin up Dockerized fixtures and execute the full pytest suite via `./run_tests.sh`.
- `uv run -- python -m pytest tests/test_sql_guard.py` — run an individual module when iterating quickly.

## Coding Style & Naming Conventions
Target Python 3.10+, four-space indentation, and Unix newlines. Format with `uv run -- black .` and lint via `uv run -- ruff check .`; CI mirrors these checks. Modules and callables use snake_case, classes PascalCase (e.g., `TestReadOnlyGuards`), and immutable settings uppercase (`DEFAULT_QUERY_TIMEOUT`). Apply type hints on public surfaces and keep docstrings brief, emphasising read-only guarantees.

## Testing Guidelines
Pytest discovers `test_*.py` modules, `Test*` classes, and `test_*` functions per `pytest.ini`. Use the built-in markers (`security`, `cli`, `python`, `ssh`, `slow`) to scope runs, e.g. `pytest -m "security and not slow"`. A 30s default timeout applies, so tear down tunnels and subprocesses explicitly. `just test` emits JUnit XML at `test-results/pytest.xml` for CI uploads.

## Commit & Pull Request Guidelines
Recent commits favour imperative, concise subjects (“Refactor PostgreSQL read-only guard into shared utility”). Keep changes focused and explain security or connector impacts in the body when needed. Pull requests should describe motivation, list affected modules or scripts, link issues, and attach logs or screenshots for operational updates.

## Security & Configuration Tips
Do not relax safeguards in `src/utils/sql_guard.py` or connector factories without matching tests. Treat `connections.yaml` as sensitive; ship only the redacted sample and load secrets via `.env`. Use the SSH tunnel helpers instead of bespoke subprocesses to preserve timeout and read-only enforcement.
