import json
import sys
from pathlib import Path

import yaml

from src.config.dbeaver_import import DBeaverImporter, main


def _write_dbeaver_workspace(tmp_path: Path, connections: list[dict]) -> Path:
    """Create a minimal DBeaver workspace with data-sources.json."""
    workspace = tmp_path / ".dbeaver"
    workspace.mkdir()
    data = {
        "connections": {
            conn["id"]: {
                "name": conn["name"],
                "provider": conn.get("provider", "clickhouse"),
                "configuration": conn.get("configuration", {}),
            }
            for conn in connections
        }
    }
    (workspace / "data-sources.json").write_text(json.dumps(data, indent=2))
    return workspace


def _run_import(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["dbeaver_import", *argv])
    main()


def test_dry_run_skips_writes(tmp_path, monkeypatch, capsys):
    workspace = _write_dbeaver_workspace(
        tmp_path,
        [
            {
                "id": "c1",
                "name": "clickhouse-1.example.com grafana",
                "provider": "clickhouse",
                "configuration": {"host": "clickhouse-1.example.com", "port": "8123"},
            }
        ],
    )

    def _fake_decrypt(self):
        return {"c1": {"user": "grafana", "password": "secret"}}, {}

    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", _fake_decrypt)

    output_path = tmp_path / "connections.yaml"
    env_path = tmp_path / ".env"
    output_path.write_text("- connection_name: existing\n  type: clickhouse\n")
    env_path.write_text("DB_PASSWORD_EXISTING=keep\n")

    _run_import(
        monkeypatch,
        [
            str(workspace),
            "--dry-run",
            "--output",
            str(output_path),
            "--env-file",
            str(env_path),
        ],
    )

    captured = capsys.readouterr().out
    assert "Dry run: skipping write" in captured
    assert output_path.read_text() == "- connection_name: existing\n  type: clickhouse\n"
    assert env_path.read_text() == "DB_PASSWORD_EXISTING=keep\n"
    assert not list(tmp_path.glob("connections.yaml.bak.*"))
    assert not list(tmp_path.glob(".env.bak.*"))


def test_only_merges_with_existing(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(
        tmp_path,
        [
            {
                "id": "c1",
                "name": "clickhouse-1.example.com grafana",
                "provider": "clickhouse",
                "configuration": {"host": "clickhouse-1.example.com", "port": "8123"},
            }
        ],
    )

    def _fake_decrypt(self):
        return {"c1": {"user": "grafana", "password": "secret"}}, {}

    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", _fake_decrypt)

    output_path = tmp_path / "connections.yaml"
    env_path = tmp_path / ".env"
    existing = [
        {
            "connection_name": "existing_conn",
            "type": "clickhouse",
            "servers": ["old-host:8123"],
            "db": "default",
            "username": "old_user",
            "implementation": "cli",
        }
    ]
    output_path.write_text(yaml.safe_dump(existing, sort_keys=False))
    env_path.write_text("DB_PASSWORD_EXISTING=keep\n")

    _run_import(
        monkeypatch,
        [
            str(workspace),
            "--only",
            "clickhouse-1.example.com grafana",
            "--output",
            str(output_path),
            "--env-file",
            str(env_path),
        ],
    )

    updated = yaml.safe_load(output_path.read_text())
    names = {conn["connection_name"] for conn in updated}
    assert "existing_conn" in names
    assert "clickhouse-1_example_com_grafana" in names

    env_contents = env_path.read_text()
    assert "DB_PASSWORD_CLICKHOUSE_1_EXAMPLE_COM_GRAFANA=secret" in env_contents

    assert list(tmp_path.glob("connections.yaml.bak.*"))
    assert list(tmp_path.glob(".env.bak.*"))
