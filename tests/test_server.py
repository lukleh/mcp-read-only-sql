"""Tests for MCP Read-Only SQL server bootstrap behavior."""

from importlib.metadata import version
from pathlib import Path
from stat import S_IMODE

import pytest


def test_package_version_matches_distribution_metadata():
    """The module should expose the installed distribution version."""
    from mcp_read_only_sql import __version__

    assert __version__ == version("mcp-read-only-sql")


def test_write_sample_config_creates_runtime_dirs_and_file(tmp_path, monkeypatch):
    """Sample config bootstrap should create package runtime directories."""
    from mcp_read_only_sql.runtime_paths import resolve_runtime_paths
    from mcp_read_only_sql.server import (
        SAMPLE_CONNECTIONS_YAML,
        write_sample_config,
    )

    config_dir = tmp_path / "config"
    state_dir = tmp_path / "state"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("MCP_READ_ONLY_SQL_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("MCP_READ_ONLY_SQL_STATE_DIR", str(state_dir))
    monkeypatch.setenv("MCP_READ_ONLY_SQL_CACHE_DIR", str(cache_dir))

    runtime_paths = resolve_runtime_paths()
    written_path = write_sample_config(runtime_paths)

    assert written_path == runtime_paths.connections_file
    assert runtime_paths.config_dir.is_dir()
    assert runtime_paths.state_dir.is_dir()
    assert runtime_paths.cache_dir.is_dir()
    assert written_path.read_text(encoding="utf-8") == SAMPLE_CONNECTIONS_YAML
    assert S_IMODE(written_path.stat().st_mode) == 0o600


def test_sample_config_matches_example_file():
    """The packaged sample config should stay in sync with connections.yaml.sample."""
    from mcp_read_only_sql.server import SAMPLE_CONNECTIONS_YAML

    example_path = Path(__file__).resolve().parents[1] / "connections.yaml.sample"

    assert SAMPLE_CONNECTIONS_YAML == example_path.read_text(encoding="utf-8")


def test_write_sample_config_requires_overwrite_to_replace(tmp_path):
    """Existing config files should be preserved unless overwrite is requested."""
    from mcp_read_only_sql.runtime_paths import RuntimePaths
    from mcp_read_only_sql.server import write_sample_config

    runtime_paths = RuntimePaths(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()
    runtime_paths.connections_file.write_text(
        "- connection_name: existing\n", encoding="utf-8"
    )

    with pytest.raises(FileExistsError, match="already exists"):
        write_sample_config(runtime_paths)


def test_write_sample_config_overwrite_replaces_existing_file(tmp_path):
    """Overwrite mode should replace an existing file with the sample config."""
    from mcp_read_only_sql.runtime_paths import RuntimePaths
    from mcp_read_only_sql.server import (
        SAMPLE_CONNECTIONS_YAML,
        write_sample_config,
    )

    runtime_paths = RuntimePaths(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        cache_dir=tmp_path / "cache",
    )
    runtime_paths.ensure_directories()
    runtime_paths.connections_file.write_text(
        "- connection_name: existing\n", encoding="utf-8"
    )

    write_sample_config(runtime_paths, overwrite=True)

    assert (
        runtime_paths.connections_file.read_text(encoding="utf-8")
        == SAMPLE_CONNECTIONS_YAML
    )
    assert S_IMODE(runtime_paths.connections_file.stat().st_mode) == 0o600


def test_main_write_sample_config_and_print_paths_together(
    monkeypatch, tmp_path, capsys
):
    """The CLI should support bootstrapping config and printing paths in one run."""
    import sys

    from mcp_read_only_sql import server

    config_dir = tmp_path / "config"
    state_dir = tmp_path / "state"
    cache_dir = tmp_path / "cache"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-read-only-sql",
            "--config-dir",
            str(config_dir),
            "--state-dir",
            str(state_dir),
            "--cache-dir",
            str(cache_dir),
            "--write-sample-config",
            "--print-paths",
        ],
    )
    monkeypatch.setattr(
        server.ReadOnlySQLServer,
        "__init__",
        lambda *args, **kwargs: pytest.fail(
            "ReadOnlySQLServer should not be constructed when only printing setup info"
        ),
    )

    server.main()

    output = capsys.readouterr().out

    assert f"Wrote sample config to {config_dir / 'connections.yaml'}" in output
    assert f"config_dir={config_dir}" in output
    assert f"state_dir={state_dir}" in output
    assert f"cache_dir={cache_dir}" in output
    assert f"connections_file={config_dir / 'connections.yaml'}" in output


def test_main_rejects_overwrite_without_write_sample_config(monkeypatch):
    """Overwrite should only be accepted together with sample-config bootstrap."""
    import sys

    from mcp_read_only_sql import server

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-read-only-sql",
            "--overwrite",
        ],
    )

    with pytest.raises(SystemExit):
        server.main()


def test_main_dispatches_validate_config_subcommand_with_runtime_flags(
    monkeypatch, tmp_path, capsys
):
    """Subcommands should run through the root CLI and honor shared runtime paths."""
    import sys

    from mcp_read_only_sql import server

    config_dir = tmp_path / "config"
    state_dir = tmp_path / "state"
    cache_dir = tmp_path / "cache"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-read-only-sql",
            "--config-dir",
            str(config_dir),
            "--state-dir",
            str(state_dir),
            "--cache-dir",
            str(cache_dir),
            "validate-config",
            "--print-paths",
        ],
    )
    monkeypatch.setattr(
        server.ReadOnlySQLServer,
        "__init__",
        lambda *args, **kwargs: pytest.fail(
            "ReadOnlySQLServer should not be constructed for management subcommands"
        ),
    )

    server.main()

    output = capsys.readouterr().out

    assert f"config_dir={config_dir}" in output
    assert f"state_dir={state_dir}" in output
    assert f"cache_dir={cache_dir}" in output
    assert f"connections_file={config_dir / 'connections.yaml'}" in output


def test_main_rejects_bootstrap_flags_with_subcommands(monkeypatch):
    """Root bootstrap flags should not be combined with management subcommands."""
    import sys

    from mcp_read_only_sql import server

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-read-only-sql",
            "--write-sample-config",
            "validate-config",
        ],
    )

    with pytest.raises(SystemExit):
        server.main()
