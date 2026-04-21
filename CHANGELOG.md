# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.3] - 2026-04-21

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
