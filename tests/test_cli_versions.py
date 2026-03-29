"""Regression tests for CLI version flags on the public root command."""

from importlib.metadata import version

import pytest

from mcp_read_only_sql import server


@pytest.mark.parametrize(
    "argv",
    [
        ["mcp-read-only-sql", "--version"],
        ["mcp-read-only-sql", "import-dbeaver", "--version"],
        ["mcp-read-only-sql", "validate-config", "--version"],
        ["mcp-read-only-sql", "test-connection", "--version"],
        ["mcp-read-only-sql", "test-ssh-tunnel", "--version"],
    ],
)
def test_cli_entrypoints_support_version(monkeypatch, capsys, argv):
    """The public root CLI should expose version flags for every subcommand."""
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit) as exc_info:
        server.main()

    assert exc_info.value.code == 0
    assert (
        capsys.readouterr().out.strip()
        == f"{' '.join(argv[:-1])} {version('mcp-read-only-sql')}"
    )
