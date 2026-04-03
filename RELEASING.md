# Releasing `mcp-read-only-sql`

This repository publishes to PyPI from Git tags through GitHub Actions.

Release automation lives in:
- `.github/workflows/publish.yml`
- `.github/workflows/test.yml`
- the GitHub environment named `pypi`
- the PyPI trusted publisher for `lukleh/mcp-read-only-sql`

## What To Change For A Release

Update these files in the release commit:

1. `CHANGELOG.md`
   Move the user-visible items from `## [Unreleased]` into a new section:
   `## [X.Y.Z] - YYYY-MM-DD`
2. `pyproject.toml`
   Update `[project].version` to `X.Y.Z`

Do not expect a tracked `uv.lock` change in this repository. `uv.lock` is
gitignored here, so it is not part of the release diff.

`RELEASING.md` should stay evergreen. It should explain the process, not carry a
release-specific version number.

## How To Update The Version

1. Edit `pyproject.toml`
2. Refresh the local environment if needed:

```bash
uv sync --extra dev
```

3. Confirm the installed package metadata and CLI version output match:

```bash
uv run --extra dev pytest tests/test_server.py tests/test_cli_versions.py -q -k 'package_version_matches_distribution_metadata or support_version'
```

## Pre-Release Validation

Run the normal local checks before tagging:

```bash
uv run --extra dev ruff check .
uv run --extra dev ty check
./run_tests.sh
```

If you are iterating on release metadata only, the focused version tests above
are the minimum sanity check.

## How To Publish

1. Make the release commit on `main`
2. Create and push the matching tag:

```bash
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

3. GitHub Actions starts `.github/workflows/publish.yml`
4. The workflow runs the test matrix, builds the wheel and sdist, and smoke-tests the built artifacts
5. The final publish job pauses on the GitHub `pypi` environment
6. Approve the deployment in GitHub Actions
7. GitHub publishes the package to PyPI

## Notes

- Keep both `implementation: cli` and `implementation: python` clearly supported in release notes and docs
- `uvx` installs the Python package only; it does not install `psql`, `clickhouse-client`, or `sshpass`
- If the public CLI or result-file behavior changes, keep the package smoke tests and README aligned with `mcp-read-only-sql`
