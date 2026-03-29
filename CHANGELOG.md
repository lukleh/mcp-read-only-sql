# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
