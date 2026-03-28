import json
import stat
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
    output_path.write_text("- connection_name: existing\n  type: clickhouse\n")

    _run_import(
        monkeypatch,
        [
            str(workspace),
            "--dry-run",
            "--output",
            str(output_path),
        ],
    )

    captured = capsys.readouterr().out
    assert "Dry run: skipping write" in captured
    assert "Database passwords imported" in captured
    assert (
        output_path.read_text() == "- connection_name: existing\n  type: clickhouse\n"
    )
    assert not list(tmp_path.glob("connections.yaml.bak.*"))


def test_only_merges_with_existing(tmp_path, monkeypatch, capsys):
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

    _run_import(
        monkeypatch,
        [
            str(workspace),
            "--only",
            "clickhouse-1.example.com grafana",
            "--output",
            str(output_path),
        ],
    )

    updated = yaml.safe_load(output_path.read_text())
    by_name = {conn["connection_name"]: conn for conn in updated}
    names = set(by_name)
    assert "existing_conn" in names
    assert "clickhouse-1_example_com_grafana" in names
    assert by_name["clickhouse-1_example_com_grafana"]["password"] == "secret"
    assert stat.S_IMODE(output_path.stat().st_mode) == 0o600

    captured = capsys.readouterr().out
    assert "Database passwords imported: 1" in captured

    assert list(tmp_path.glob("connections.yaml.bak.*"))
    backup_path = next(tmp_path.glob("connections.yaml.bak.*"))
    assert stat.S_IMODE(backup_path.stat().st_mode) == 0o600


def test_import_does_not_write_credentials_files(tmp_path, monkeypatch, capsys):
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
    output_path.write_text("- connection_name: existing\n  type: clickhouse\n")

    _run_import(
        monkeypatch,
        [
            str(workspace),
            "--output",
            str(output_path),
        ],
    )

    captured = capsys.readouterr().out
    assert "Database passwords imported: 1" in captured

    updated = yaml.safe_load(output_path.read_text())
    assert updated[0]["connection_name"] == "clickhouse-1_example_com_grafana"
    assert updated[0]["password"] == "secret"
    assert stat.S_IMODE(output_path.stat().st_mode) == 0o600


def test_print_paths_without_dbeaver_path(monkeypatch, capsys):
    _run_import(monkeypatch, ["--print-paths"])

    captured = capsys.readouterr().out
    assert "config_dir=" in captured
    assert "state_dir=" in captured
    assert "cache_dir=" in captured
    assert "connections_file=" in captured


def test_merge_clusters_keeps_distinct_password_groups():
    importer = DBeaverImporter("/tmp/nonexistent")

    connections = [
        {
            "connection_name": "cluster_node_1",
            "type": "clickhouse",
            "servers": ["clickhouse-1.example.com:8123"],
            "db": "default",
            "username": "grafana",
            "password": "secret-a",
            "implementation": "cli",
        },
        {
            "connection_name": "cluster_node_2",
            "type": "clickhouse",
            "servers": ["clickhouse-2.example.com:8123"],
            "db": "default",
            "username": "grafana",
            "password": "secret-b",
            "implementation": "cli",
        },
    ]

    merged = importer._merge_cluster_connections(connections)

    assert len(merged) == 2
    assert {conn["password"] for conn in merged} == {"secret-a", "secret-b"}
    assert {tuple(conn["servers"]) for conn in merged} == {
        ("clickhouse-1.example.com:8123",),
        ("clickhouse-2.example.com:8123",),
    }
