"""Regression tests for CLI version flags."""

import importlib
from importlib.metadata import version

import pytest


@pytest.mark.parametrize(
    ("module_name", "prog_name"),
    [
        ("mcp_read_only_sql.server", "mcp-read-only-sql"),
        (
            "mcp_read_only_sql.config.dbeaver_import",
            "mcp-read-only-sql-import-dbeaver",
        ),
        (
            "mcp_read_only_sql.tools.validate_config",
            "mcp-read-only-sql-validate-config",
        ),
        (
            "mcp_read_only_sql.tools.test_connection",
            "mcp-read-only-sql-test-connection",
        ),
        (
            "mcp_read_only_sql.tools.test_ssh_tunnel",
            "mcp-read-only-sql-test-ssh-tunnel",
        ),
    ],
)
def test_cli_entrypoints_support_version(monkeypatch, capsys, module_name, prog_name):
    """Each packaged CLI should expose a standard --version flag."""
    module = importlib.import_module(module_name)

    monkeypatch.setattr("sys.argv", [prog_name, "--version"])

    with pytest.raises(SystemExit) as exc_info:
        module.main()

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == (
        f"{prog_name} {version('mcp-read-only-sql')}"
    )
