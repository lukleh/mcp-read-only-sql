# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
