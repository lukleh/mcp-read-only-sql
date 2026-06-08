import stat

import pytest

from mcp_read_only_sql import cli_binaries
from mcp_read_only_sql.cli_binaries import env_var_for, resolve_cli_binary


def _make_executable(path):
    path.write_text("#!/bin/sh\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return str(path)


def test_env_var_for_naming():
    assert env_var_for("psql") == "MCP_READ_ONLY_SQL_PSQL_PATH"
    assert (
        env_var_for("clickhouse-client") == "MCP_READ_ONLY_SQL_CLICKHOUSE_CLIENT_PATH"
    )


def test_env_override_returns_pinned_path(monkeypatch, tmp_path):
    binary = _make_executable(tmp_path / "psql")
    monkeypatch.setenv("MCP_READ_ONLY_SQL_PSQL_PATH", binary)

    assert resolve_cli_binary("psql") == binary


def test_env_override_expands_user(monkeypatch, tmp_path):
    binary = _make_executable(tmp_path / "psql")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("MCP_READ_ONLY_SQL_PSQL_PATH", "~/psql")

    assert resolve_cli_binary("psql") == binary


def test_env_override_non_executable_raises(monkeypatch, tmp_path):
    bogus = tmp_path / "not-exec"
    bogus.write_text("")
    monkeypatch.setenv("MCP_READ_ONLY_SQL_PSQL_PATH", str(bogus))

    with pytest.raises(FileNotFoundError) as exc:
        resolve_cli_binary("psql")
    assert "MCP_READ_ONLY_SQL_PSQL_PATH" in str(exc.value)


def test_resolves_via_which(monkeypatch):
    monkeypatch.delenv("MCP_READ_ONLY_SQL_PSQL_PATH", raising=False)
    monkeypatch.setattr(
        cli_binaries.shutil,
        "which",
        lambda name: "/usr/bin/psql" if name == "psql" else None,
    )

    assert resolve_cli_binary("psql") == "/usr/bin/psql"


def test_falls_back_to_candidate_dir(monkeypatch, tmp_path):
    binary = _make_executable(tmp_path / "psql")
    monkeypatch.delenv("MCP_READ_ONLY_SQL_PSQL_PATH", raising=False)
    monkeypatch.setattr(cli_binaries.shutil, "which", lambda name: None)
    monkeypatch.setattr(cli_binaries, "_candidate_dirs", lambda b: [str(tmp_path)])

    assert resolve_cli_binary("psql") == binary


def test_not_found_raises_with_env_hint(monkeypatch):
    monkeypatch.delenv("MCP_READ_ONLY_SQL_PSQL_PATH", raising=False)
    monkeypatch.setattr(cli_binaries.shutil, "which", lambda name: None)
    monkeypatch.setattr(cli_binaries, "_candidate_dirs", lambda b: [])

    with pytest.raises(FileNotFoundError) as exc:
        resolve_cli_binary("psql")
    message = str(exc.value)
    assert "psql: command not found" in message
    assert "MCP_READ_ONLY_SQL_PSQL_PATH" in message


def test_brew_prefix_used_for_candidate_dirs(monkeypatch, tmp_path):
    """On macOS the libpq Homebrew prefix should be probed for psql."""
    prefix_bin = tmp_path / "opt" / "libpq" / "bin"
    prefix_bin.mkdir(parents=True)
    _make_executable(prefix_bin / "psql")

    monkeypatch.setattr(cli_binaries.sys, "platform", "darwin")
    monkeypatch.setattr(
        cli_binaries,
        "_brew_prefix",
        lambda formula: str(tmp_path / "opt" / "libpq") if formula == "libpq" else None,
    )

    dirs = cli_binaries._candidate_dirs("psql")
    assert str(prefix_bin) in dirs
