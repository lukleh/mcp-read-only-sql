import json
import stat
import sys
from pathlib import Path

import pytest
import yaml

from mcp_read_only_sql.config.dbeaver_import import DBeaverImporter, main


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


def test_subset_import_retains_unselected_cluster_servers(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(
        tmp_path,
        [
            {"id": "c1", "name": "node 1", "configuration": {"host": "node1.example", "port": "8123"}},
            {"id": "c2", "name": "node 2", "configuration": {"host": "node2.example", "port": "8123"}},
        ],
    )
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    initial = output.read_bytes()
    assert initial[-1:] == b"\n" and initial[-2:-1] != b"\n"
    _run_import(monkeypatch, [str(workspace), "--output", str(output), "--only", "node 1"])
    updated = yaml.safe_load(output.read_text())
    assert len(updated) == 1
    assert updated[0]["servers"] == ["node1.example:8123", "node2.example:8123"]
    assert output.read_bytes() == initial


def test_full_import_can_remove_cluster_members(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(
        tmp_path,
        [{"id": "c1", "name": "node 1", "configuration": {"host": "node1.example", "port": "8123"}}],
    )
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([{
        "connection_name": "node_1", "type": "clickhouse", "db": "default", "username": "reader",
        "servers": ["node1.example:8123", "node2.example:8123"],
    }]))
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    assert yaml.safe_load(output.read_text())[0]["servers"] == ["node1.example:8123"]


@pytest.mark.parametrize("change", ["password", "username", "database", "ssh", "implementation"])
def test_subset_cluster_settings_change_requires_full_import(tmp_path, monkeypatch, capsys, change):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "node 1", "configuration": {"host": "node1.example", "port": "8123", "database": "analytics"}},
        {"id": "c2", "name": "node 2", "configuration": {"host": "node2.example", "port": "8123", "database": "analytics"}},
    ])
    credentials = {
        "c1": {"user": "reader", "password": "shared-secret"},
        "c2": {"user": "reader", "password": "shared-secret"},
    }
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (credentials, {}))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    sources_path = workspace / "data-sources.json"
    sources = json.loads(sources_path.read_text())
    if change == "password":
        credentials["c1"]["password"] = "selected-secret"
    elif change == "username":
        credentials["c1"]["user"] = "selected-reader"
    elif change == "database":
        sources["connections"]["c1"]["configuration"]["database"] = "selected_database"
    elif change == "ssh":
        sources["connections"]["c1"]["configuration"]["handlers"] = {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "userName": "tunnel"},
        }}
    else:
        previous = yaml.safe_load(output.read_text())
        previous[0]["implementation"] = "python"
        output.write_text(yaml.safe_dump(previous, sort_keys=False))
    sources_path.write_text(json.dumps(sources))
    original = output.read_bytes()
    assert original[-1:] == b"\n" and original[-2:-1] != b"\n"
    with pytest.raises(SystemExit) as failure:
        _run_import(monkeypatch, [str(workspace), "--only", "node 1", "--output", str(output)])
    assert failure.value.code == 1
    assert "run a full import" in capsys.readouterr().out
    assert output.read_bytes() == original
    assert not list(tmp_path.glob("connections.yaml.bak.*"))
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    updated = yaml.safe_load(output.read_text())
    if change != "implementation":
        node2 = next(conn for conn in updated if conn["connection_name"] == "node_2")
        assert node2["servers"] == ["node2.example:8123"]
        assert node2["username"] == "reader"
        assert node2["password"] == "shared-secret"
    else:
        assert updated[0]["implementation"] == "cli"
