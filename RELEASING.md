# Releasing `mcp-read-only-sql`

This repo follows the same package and release model as the other published MCP servers:

- package metadata in `pyproject.toml`
- tag-driven GitHub Actions publish workflow
- PyPI trusted publishing via the GitHub `pypi` environment
- manual approval at the `pypi` environment before the final upload job

## CLI convention

- The public interface starts with the package command: `mcp-read-only-sql`.
- Repository-facing docs should prefer the root command plus flags or subcommands, not extra top-level helper scripts.
- This repo already uses subcommands for auxiliary operations:
  - `mcp-read-only-sql import-dbeaver`
  - `mcp-read-only-sql validate-config`
  - `mcp-read-only-sql test-connection`
  - `mcp-read-only-sql test-ssh-tunnel`
- Future auxiliary operations should extend that subcommand surface instead of adding new public console entry points.

## Current package status

- PyPI package: published as `0.1.0`
- Publish workflow: active on `main`
- GitHub `pypi` environment: configured
- Required reviewer: `lukleh`
- Self-review: allowed

## Changelog policy

- Keep upcoming user-visible changes under `## [Unreleased]` in `CHANGELOG.md`.
- On release, move those entries into a dated version section such as `## [0.1.1] - 2026-03-29`.
- Prefer concise bullets grouped under `Added`, `Changed`, and `Fixed`.
- When creating GitHub release notes, reuse the matching `CHANGELOG.md` section instead of writing a second summary from scratch.

## One-time setup

### PyPI

1. Log in to `https://pypi.org`.
2. Add a trusted publisher for this repository.
   If the project already exists, use the project's `Manage -> Publishing` page instead of the account-level `Publishing` page.
   - Project name: `mcp-read-only-sql`
   - Owner: `lukleh`
   - Repository: `mcp-read-only-sql`
   - Workflow filename: `publish.yml`
   - Environment name: `pypi`

### GitHub

1. Create an environment named `pypi`.
2. Add at least one required reviewer.
3. Decide whether self-review is allowed.

Current repo configuration:

- Environment: `pypi`
- Required reviewer: `lukleh`
- Self-review: allowed

## Release flow

1. Update `CHANGELOG.md` for the release.
2. Confirm `version` in `pyproject.toml` on `main`.
3. Commit the release changes to `main`.
4. Push a matching tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

5. GitHub Actions starts the `Publish` workflow automatically.
6. The workflow runs the full release gate before approval:
   - test matrix
   - package build
   - wheel smoke tests
   - sdist smoke tests
7. The final `publish` job pauses on the GitHub `pypi` environment.
8. Approve the deployment.
9. After approval, GitHub uploads the built package to PyPI.

## Prereleases

Use normal PEP 440 prerelease versions in `pyproject.toml`, for example:

- `0.2.0a1`
- `0.2.0b1`
- `0.2.0rc1`

Push the matching tag:

```bash
git tag v0.2.0a1
git push origin v0.2.0a1
```

The same workflow and approval gate handle prereleases.

## SQL-specific notes

- Keep both `implementation: cli` and `implementation: python` clearly supported in public docs.
- `uvx` installs the MCP server package, not system database clients.
- If users choose CLI mode, the docs should stay explicit about external requirements:
  - PostgreSQL CLI mode needs `psql`
  - ClickHouse CLI mode needs `clickhouse-client`
  - CLI SSH password auth needs `sshpass`
- Package smoke tests should cover the main server entry point and the management subcommands.
