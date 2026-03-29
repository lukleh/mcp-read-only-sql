# Releasing `mcp-read-only-sql`

This repo follows the same package and release model as the other published MCP servers:

- package metadata in `pyproject.toml`
- tag-driven GitHub Actions publish workflow
- PyPI trusted publishing via the GitHub `pypi` environment
- manual approval at the `pypi` environment before the final upload job

## Current package status

- PyPI package: not published yet
- Publish workflow: pending merge on the public-release branch
- GitHub `pypi` environment: not configured yet
- Required reviewer: not configured yet
- Self-review: not configured yet

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

## Release flow

1. Merge the release-prep branch into `main`.
2. Confirm `version` in `pyproject.toml`.
3. Push a matching tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

4. GitHub Actions starts the `Publish` workflow automatically.
5. The workflow runs the full release gate before approval:
   - test matrix
   - package build
   - wheel smoke tests
   - sdist smoke tests
6. The final `publish` job pauses on the GitHub `pypi` environment.
7. Approve the deployment.
8. After approval, GitHub uploads the built package to PyPI.

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
- Package smoke tests should cover both the main server entry point and the DBeaver import helper entry point.
