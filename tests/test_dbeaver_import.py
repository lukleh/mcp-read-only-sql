import json
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from mcp_read_only_sql.config import load_connections
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


@pytest.mark.parametrize("mixed_forms", [False, True])
def test_subset_import_retains_mapping_servers_without_duplicates(tmp_path, monkeypatch, mixed_forms):
    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c1", "name": "node 1", "configuration": {"host": "node1.example", "port": "8123"},
    }])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": "reader", "password": "shared-secret"}}, {},
    ))
    servers = [
        {"host": "node1.example", "port": 8123},
        "node2.example:8123" if mixed_forms else {"host": "node2.example", "port": 8123},
    ]
    previous = [{
        "connection_name": "node_1", "type": "clickhouse", "db": "default",
        "username": "reader", "password": "shared-secret", "implementation": "cli",
        "servers": servers, "description": "cluster",
    }]
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump(previous, sort_keys=False))
    original = output.read_bytes()
    assert original[-1:] == b"\n" and original[-2:-1] != b"\n"
    assert len(load_connections(output)["node_1"].servers) == 2
    _run_import(monkeypatch, [str(workspace), "--only", "node 1", "--output", str(output)])
    assert yaml.safe_load(output.read_text())[0]["servers"] == servers
    loaded = load_connections(output)["node_1"].servers
    assert len(loaded) == 2
    assert {(server.host, server.port) for server in loaded} == {
        ("node1.example", 8123), ("node2.example", 8123),
    }
    assert yaml.safe_load(output.read_text()) == previous
    updated = output.read_bytes()
    assert updated[-1:] == b"\n" and updated[-2:-1] != b"\n"


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


@pytest.mark.parametrize("change", ["password", "username", "database", "ssh"])
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
    node2 = next(conn for conn in updated if conn["connection_name"] == "node_2")
    assert node2["servers"] == ["node2.example:8123"]
    assert node2["username"] == "reader"
    assert node2["password"] == "shared-secret"


@pytest.mark.parametrize("selected", ["node 1", "node 2"])
@pytest.mark.parametrize("empty_passwords", [False, True])
def test_partial_cluster_preserves_custom_ssh_options_and_default_port(tmp_path, monkeypatch, selected, empty_passwords):
    handlers = {"ssh_tunnel": {"enabled": True, "properties": {
        "host": "bastion", "userName": "tunnel", "authType": "PASSWORD",
    }}}
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "node 1", "configuration": {"host": "node1.example", "handlers": handlers}},
        {"id": "c2", "name": "node 2", "configuration": {"host": "node2.example", "handlers": handlers}},
    ])
    credentials = {key: {"user": "reader", "password": "database-secret"} for key in ("c1", "c2")}
    ssh_credentials = {key: {"user": "tunnel", "password": "ssh-secret"} for key in ("c1", "c2")}
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (credentials, ssh_credentials))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    previous = yaml.safe_load(output.read_text())
    previous[0]["implementation"] = "python"
    previous[0]["ssh_tunnel"].update({
        "port": 22, "host_key_checking": "accept-new",
        "known_hosts_file": str(tmp_path / "known_hosts"), "ssh_timeout": 45,
    })
    output.write_text(yaml.safe_dump(previous, sort_keys=False))
    original = output.read_bytes()
    if empty_passwords:
        for key in ("c1", "c2"):
            credentials[key]["password"] = ""
            ssh_credentials[key]["password"] = ""
    _run_import(monkeypatch, [str(workspace), "--only", selected, "--output", str(output)])
    assert yaml.safe_load(output.read_text()) == previous
    if not empty_passwords:
        assert output.read_bytes() == original


@pytest.mark.parametrize("implementation", ["cli", "python"])
def test_partial_nonleading_member_updates_unique_existing_cluster(tmp_path, monkeypatch, implementation):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "node 1", "configuration": {"host": "node1.example"}},
        {"id": "c2", "name": "node 2", "configuration": {"host": "node2.example"}},
    ])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {key: {"user": "reader", "password": "shared-secret"} for key in ("c1", "c2")}, {},
    ))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    previous = yaml.safe_load(output.read_text())
    previous[0]["implementation"] = implementation
    output.write_text(yaml.safe_dump(previous, sort_keys=False))
    _run_import(monkeypatch, [str(workspace), "--only", "node 2", "--output", str(output)])
    assert yaml.safe_load(output.read_text()) == previous
    assert len(previous[0]["servers"]) == 2


def test_partial_endpoint_match_refuses_multiple_existing_owners(tmp_path, monkeypatch, capsys):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c2", "name": "node 2", "configuration": {"host": "node2.example"}}])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: ({"c2": {"user": "reader"}}, {}))
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([
        {"connection_name": name, "type": "clickhouse", "servers": ["node1.example:8123", "node2.example:8123"], "db": "default", "username": "reader"}
        for name in ("first", "second")
    ], sort_keys=False))
    original = output.read_bytes()
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--only", "node 2", "--output", str(output)])
    assert error.value.code == 1
    assert "Ambiguous partial import" in capsys.readouterr().out
    assert output.read_bytes() == original
    assert not list(tmp_path.glob("*.bak.*"))


@pytest.mark.parametrize("difference", ["database", "username", "unavailable_username", "ssh_route", "implementation"])
def test_partial_endpoint_does_not_replace_a_different_or_unknown_identity(tmp_path, monkeypatch, capsys, difference):
    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c1", "name": "new name", "configuration": {"host": "shared.example", "database": "analytics"},
    }])
    creds = {} if difference == "unavailable_username" else {"c1": {"user": "reader", "password": "new-secret"}}
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (creds, {}))
    previous = {"connection_name": "existing", "type": "clickhouse", "servers": ["shared.example:8123"],
                "db": "other" if difference == "database" else "analytics",
                "username": "another" if difference == "username" else "reader", "password": "known-secret"}
    if difference == "ssh_route":
        previous["ssh_tunnel"] = {"host": "bastion", "user": "tunnel"}
    if difference == "implementation":
        previous["implementation"] = "python"
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([previous], sort_keys=False))
    original = output.read_bytes()
    if difference == "unavailable_username":
        with pytest.raises(SystemExit) as error:
            _run_import(monkeypatch, [str(workspace), "--only", "new name", "--output", str(output)])
        assert error.value.code == 1
        assert "Cannot identify existing endpoint owner" in capsys.readouterr().out
        assert output.read_bytes() == original
        assert not list(tmp_path.glob("*.bak.*"))
    else:
        _run_import(monkeypatch, [str(workspace), "--only", "new name", "--output", str(output)])
        by_name = {conn["connection_name"]: conn for conn in yaml.safe_load(output.read_text())}
        assert by_name["existing"] == previous
        assert by_name["new_name"]["db"] == "analytics"
        assert by_name["new_name"]["username"] == "reader"


def test_partial_new_name_does_not_replace_single_manual_profile(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c2", "name": "node 2", "configuration": {"host": "node2.example"}}])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: ({"c2": {"user": "reader"}}, {}))
    previous = {"connection_name": "manual_profile", "type": "clickhouse", "servers": ["node2.example:8123"],
                "db": "default", "username": "reader", "query_timeout": 60}
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([previous], sort_keys=False))
    _run_import(monkeypatch, [str(workspace), "--only", "node 2", "--output", str(output)])
    result = yaml.safe_load(output.read_text())
    assert result[0] == previous
    assert result[1]["connection_name"] == "node_2"


@pytest.mark.parametrize("selected", ["node 1", "node 2"])
def test_partial_cluster_keeps_unmanaged_connection_options(tmp_path, monkeypatch, selected):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "node 1", "configuration": {"host": "node1.example"}},
        {"id": "c2", "name": "node 2", "configuration": {"host": "node2.example"}},
    ])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {key: {"user": "reader"} for key in ("c1", "c2")}, {},
    ))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    previous = yaml.safe_load(output.read_text())
    previous[0].update({"query_timeout": 60, "connection_timeout": 15,
                        "allowed_databases": ["default", "analytics"]})
    output.write_text(yaml.safe_dump(previous, sort_keys=False))
    _run_import(monkeypatch, [str(workspace), "--only", selected, "--output", str(output)])
    assert yaml.safe_load(output.read_text()) == previous


@pytest.mark.parametrize("explicit_empty", [False, True])
def test_partial_single_server_preserves_backend_and_unavailable_or_empty_password(tmp_path, monkeypatch, explicit_empty):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "node 1", "configuration": {"host": "node1.example"}}])
    available = {"c1": {"user": "reader", "password": "known-secret"}}
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (available, {}))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    previous = yaml.safe_load(output.read_text())
    previous[0]["implementation"] = "python"
    output.write_text(yaml.safe_dump(previous, sort_keys=False))
    available.clear()
    if explicit_empty:
        available["c1"] = {"password": ""}
    _run_import(monkeypatch, [str(workspace), "--only", "node 1", "--output", str(output)])
    saved = yaml.safe_load(output.read_text())[0]
    assert saved["username"] == "reader"
    assert saved["password"] == "known-secret"
    assert saved["implementation"] == "python"


def test_partial_import_refuses_unreadable_existing_yaml_without_overwriting(tmp_path, monkeypatch, capsys):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "node 1"}])
    output = tmp_path / "connections.yaml"
    output.write_text("invalid: [\n")
    original = output.read_bytes()
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--only", "node 1", "--output", str(output)])
    assert error.value.code == 1
    assert "Cannot read existing" in capsys.readouterr().out
    assert output.read_bytes() == original


@pytest.mark.parametrize("changed", ["database_host", "database_name", "database_user", "ssh_host", "ssh_user", "ssh_key"])
@pytest.mark.parametrize("empty_password", [False, True])
def test_partial_missing_credentials_never_move_to_changed_identity(tmp_path, monkeypatch, capsys, changed, empty_password):
    props = {"host": "old-bastion", "userName": "old-login", "authType": "PASSWORD"}
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "profile", "configuration": {
        "host": "old-database", "database": "analytics", "handlers": {"ssh_tunnel": {"enabled": True, "properties": props}},
    }}])
    credentials = {"c1": {"user": "alice", "password": "old-database-secret"}}
    ssh_credentials = {"c1": {"user": "old-login", "password": "old-ssh-secret"}}
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (credentials, ssh_credentials))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    original = output.read_bytes()
    data = json.loads((workspace / "data-sources.json").read_text())
    config = data["connections"]["c1"]["configuration"]
    if changed == "database_host":
        config["host"] = "new-database"
    elif changed == "database_name":
        config["database"] = "new-database-name"
    elif changed == "database_user":
        credentials["c1"]["user"] = "bob"
    elif changed == "ssh_host":
        config["handlers"]["ssh_tunnel"]["properties"]["host"] = "new-bastion"
    elif changed == "ssh_user":
        ssh_credentials["c1"]["user"] = "new-login"
    else:
        config["handlers"]["ssh_tunnel"]["properties"]["keyPath"] = "/new-key"
        config["handlers"]["ssh_tunnel"]["properties"]["authType"] = "PUBLIC_KEY"
    credentials["c1"].pop("password")
    ssh_credentials["c1"].pop("password")
    if empty_password:
        credentials["c1"]["password"] = ""
        ssh_credentials["c1"]["password"] = ""
    (workspace / "data-sources.json").write_text(json.dumps(data))
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--only", "profile", "--output", str(output)])
    assert error.value.code == 1
    assert "Cannot preserve unavailable" in capsys.readouterr().out
    assert output.read_bytes() == original
    assert not list(tmp_path.glob("*.bak.*"))


def test_partial_missing_ssh_password_is_not_reused_on_new_bastion(tmp_path, monkeypatch, capsys):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "profile", "configuration": {
        "host": "database", "handlers": {"ssh_tunnel": {"enabled": True, "properties": {
            "host": "old-bastion", "userName": "tunnel", "authType": "PASSWORD",
        }}},
    }}])
    ssh = {"c1": {"user": "tunnel", "password": "old-ssh-secret"}}
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": "reader", "password": "available-database-secret"}}, ssh,
    ))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    original = output.read_bytes()
    data = json.loads((workspace / "data-sources.json").read_text())
    data["connections"]["c1"]["configuration"]["handlers"]["ssh_tunnel"]["properties"]["host"] = "new-bastion"
    (workspace / "data-sources.json").write_text(json.dumps(data))
    ssh["c1"].pop("password")
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--only", "profile", "--output", str(output)])
    assert error.value.code == 1
    assert "Cannot preserve unavailable SSH password" in capsys.readouterr().out
    assert output.read_bytes() == original


@pytest.mark.parametrize("only", [[], ["--only", "prod.db"]])
def test_name_collision_never_replaces_existing_config(tmp_path, monkeypatch, only):
    workspace = _write_dbeaver_workspace(
        tmp_path,
        [
            {"id": "c1", "name": "prod.db", "configuration": {"host": "alpha.example", "port": "8123"}},
            {"id": "c2", "name": "prod_db", "configuration": {"host": "beta.example", "port": "8123"}},
        ],
    )
    output = tmp_path / "connections.yaml"
    original = "- connection_name: retained\n"
    output.write_text(original)
    with pytest.raises(SystemExit) as failure:
        _run_import(monkeypatch, [str(workspace), "--output", str(output), *only])
    assert failure.value.code == 1
    assert output.read_bytes() == original.encode()
    assert not list(tmp_path.glob("connections.yaml.bak.*"))


@pytest.mark.parametrize("wanted_first", [False, True])
def test_subset_import_ignores_unselected_name_collisions(tmp_path, monkeypatch, wanted_first):
    collisions = [
        {"id": "c1", "name": "prod.db", "configuration": {"host": "alpha.example", "port": "8123"}},
        {"id": "c2", "name": "prod_db", "configuration": {"host": "beta.example", "port": "8123"}},
    ]
    wanted = {"id": "wanted", "name": "wanted", "configuration": {"host": "wanted.example", "port": "8123"}}
    connections = [wanted, *collisions] if wanted_first else [*collisions, wanted]
    workspace = _write_dbeaver_workspace(tmp_path, connections)
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--only", "wanted", "--output", str(output)])
    result = yaml.safe_load(output.read_text())
    assert [conn["connection_name"] for conn in result] == ["wanted"]


@pytest.mark.parametrize("only", [[], ["--only", "Analytics"]])
def test_identically_named_cluster_members_merge_in_cli(tmp_path, monkeypatch, only):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "Analytics", "configuration": {"host": "ch-1.example", "port": "8123"}},
        {"id": "c2", "name": "Analytics", "configuration": {"host": "ch-2.example", "port": "8123"}},
    ])
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output), *only])
    result = yaml.safe_load(output.read_text())
    assert len(result) == 1
    assert result[0]["connection_name"] == "analytics"
    assert result[0]["servers"] == ["ch-1.example:8123", "ch-2.example:8123"]


@pytest.mark.parametrize("only", [[], ["--only", "prod.db,prod_db"]])
def test_colliding_names_merge_when_both_records_are_imported(tmp_path, monkeypatch, only):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "prod.db", "configuration": {"host": "ch-1.example", "port": "8123"}},
        {"id": "c2", "name": "prod_db", "configuration": {"host": "ch-2.example", "port": "8123"}},
    ])
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output), *only])
    result = yaml.safe_load(output.read_text())
    assert len(result) == 1
    assert result[0]["connection_name"] == "prod_db"
    assert result[0]["servers"] == ["ch-1.example:8123", "ch-2.example:8123"]


@pytest.mark.parametrize("selected_first", [False, True])
def test_partial_same_group_collision_never_replaces_existing_profile(
    tmp_path, monkeypatch, capsys, selected_first
):
    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c2", "name": "prod_db", "configuration": {"host": "ch-2.example", "port": "8123"},
    }])
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--only", "prod_db", "--output", str(output)])
    original = output.read_bytes()
    assert yaml.safe_load(original)[0]["servers"] == ["ch-2.example:8123"]
    data = json.loads((workspace / "data-sources.json").read_text())
    selected = {"c1": {
        "name": "prod.db", "provider": "clickhouse",
        "configuration": {"host": "ch-1.example", "port": "8123"},
    }}
    previous = data["connections"]
    data["connections"] = selected | previous if selected_first else previous | selected
    (workspace / "data-sources.json").write_text(json.dumps(data))

    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--only", "prod.db", "--output", str(output)])

    assert error.value.code == 1
    assert "both map to connection_name" in capsys.readouterr().out
    assert output.read_bytes() == original
    assert not list(tmp_path.glob("connections.yaml.bak.*"))


def test_no_merge_refuses_identical_names_and_reports_display_names(tmp_path, monkeypatch, capsys):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "Analytics", "configuration": {"host": "ch-1.example", "port": "8123"}},
        {"id": "c2", "name": "Analytics", "configuration": {"host": "ch-2.example", "port": "8123"}},
    ])
    output = tmp_path / "connections.yaml"
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--no-merge", "--output", str(output)])
    assert error.value.code == 1
    assert "'Analytics' (c1)" in capsys.readouterr().out
    assert not output.exists()


def test_same_name_distinct_credentials_do_not_merge(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": "c1", "name": "Analytics", "configuration": {"host": "ch-1.example", "port": "8123"}},
        {"id": "c2", "name": "Analytics", "configuration": {"host": "ch-2.example", "port": "8123"}},
    ])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": "reader", "password": "first"}, "c2": {"user": "reader", "password": "second"}}, {},
    ))
    with pytest.raises(ValueError, match="both map to connection_name"):
        DBeaverImporter(str(workspace)).import_connections()


@pytest.mark.parametrize("encrypted", [False, True])
@pytest.mark.parametrize("stale_key_path", [False, True])
def test_import_reads_ssh_and_database_credentials_from_both_formats(tmp_path, monkeypatch, encrypted, stale_key_path):
    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c1", "name": "database", "configuration": {
            "host": "database.internal", "handlers": {"ssh_tunnel": {
                "enabled": True, "properties": {
                    "host": "bastion.example", "userName": "fallback", "authType": "PASSWORD",
                    "keyPath": "/unused-key" if stale_key_path else "",
                },
            }},
        },
    }])
    credentials = {
        "c1": {
            "#connection": {"user": "reader", "password": "database-secret"},
            "network/ssh_tunnel": {"user": "tunnel", "password": "ssh-secret"},
        },
        "invalid": None,
        "invalid-sections": {"#connection": [], "network/ssh_tunnel": "invalid"},
    }
    content = json.dumps(credentials).encode()
    (workspace / "credentials-config.json").write_bytes(content)
    if encrypted:
        monkeypatch.setattr(
            "mcp_read_only_sql.config.dbeaver_import.subprocess.run",
            lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b"0" * 16 + content),
        )
    result = DBeaverImporter(str(workspace)).import_connections()
    assert result[0]["username"] == "reader"
    assert result[0]["password"] == "database-secret"
    assert result[0]["ssh_tunnel"] == {
        "host": "bastion.example", "user": "tunnel", "port": 22, "password": "ssh-secret",
    }


@pytest.mark.parametrize("properties,expected", [
    ({}, 22),
    ({"port": None}, 22),
    ({"port": 22}, 22),
    ({"port": "22"}, 22),
    ({"port": "2222"}, 2222),
])
def test_import_keeps_dbeaver_ssh_port_explicit(properties, expected):
    result = DBeaverImporter("/unused")._convert_connection("c1", {
        "name": "database", "provider": "clickhouse", "configuration": {"handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "userName": "tunnel", **properties},
        }}},
    })
    assert result["ssh_tunnel"]["port"] == expected
    assert type(result["ssh_tunnel"]["port"]) is int


def test_plaintext_credentials_do_not_require_openssl(tmp_path, monkeypatch):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "database"}])
    (workspace / "credentials-config.json").write_text(json.dumps({
        "c1": {"#connection": {"user": "reader", "password": "database-secret"}},
    }))
    def unavailable(*args, **kwargs):
        raise FileNotFoundError("openssl")
    monkeypatch.setattr("mcp_read_only_sql.config.dbeaver_import.subprocess.run", unavailable)
    result = DBeaverImporter(str(workspace)).import_connections()
    assert result[0]["username"] == "reader"
    assert result[0]["password"] == "database-secret"


def test_ssh_only_encrypted_credentials_do_not_attempt_plaintext_fallback(tmp_path, monkeypatch, caplog):
    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c1", "name": "database", "configuration": {"handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "authType": "PASSWORD"},
        }}},
    }])
    (workspace / "credentials-config.json").write_bytes(b"not plaintext JSON")
    content = json.dumps({"c1": {"network/ssh_tunnel": {"user": "tunnel", "password": "ssh-secret"}}}).encode()
    monkeypatch.setattr("mcp_read_only_sql.config.dbeaver_import.subprocess.run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=b"0" * 16 + content))
    result = DBeaverImporter(str(workspace)).import_connections()
    assert result[0]["ssh_tunnel"]["password"] == "ssh-secret"
    assert "Usernames will need" not in caplog.text


@pytest.mark.parametrize("key_path", ["/key", ""])
def test_key_passphrase_is_not_mislabeled_as_account_password(capsys, key_path):
    importer = DBeaverImporter("/unused")
    result = importer._convert_connection("c1", {
        "name": "database", "provider": "clickhouse", "configuration": {"handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "authType": "PUBLIC_KEY", "keyPath": key_path},
        }}},
    }, ssh_creds={"user": "tunnel", "password": "private-key-secret"})
    assert result["ssh_tunnel"].get("private_key", "") == key_path
    assert "password" not in result["ssh_tunnel"]
    output = capsys.readouterr().out
    assert ("key passphrase was not imported" in output) == bool(key_path)
    assert "private-key-secret" not in output


@pytest.mark.parametrize("empty_password", ["", None])
def test_empty_and_missing_passwords_group_without_stored_secret_messages(tmp_path, monkeypatch, capsys, empty_password):
    workspace = _write_dbeaver_workspace(tmp_path, [
        {"id": key, "name": name, "configuration": {"host": host, "handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "userName": "tunnel", "authType": "PASSWORD"},
        }}}}
        for key, name, host in (("c1", "node 1", "node1.example"), ("c2", "node 2", "node2.example"))
    ])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": "reader", "password": empty_password}, "c2": {"user": "reader"}},
        {"c1": {"user": "tunnel", "password": empty_password}, "c2": {"user": "tunnel"}},
    ))
    output = tmp_path / "connections.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    saved = yaml.safe_load(output.read_text())
    assert len(saved) == 1
    assert saved[0]["servers"] == ["node1.example:8123", "node2.example:8123"]
    assert "password" not in saved[0]
    assert "password" not in saved[0]["ssh_tunnel"]
    messages = capsys.readouterr().out
    assert "Password: ********" not in messages
    assert "Database passwords imported: 0" in messages


@pytest.mark.parametrize("empty_password", ["", None])
@pytest.mark.parametrize("changed", ["database_host", "database_name", "database_user", "ssh_host", "ssh_user"])
def test_partial_previous_empty_password_is_not_a_secret_to_preserve(tmp_path, monkeypatch, empty_password, changed):
    database_host = "new-database" if changed == "database_host" else "database"
    database_name = "new-analytics" if changed == "database_name" else "analytics"
    database_user = "new-reader" if changed == "database_user" else "reader"
    ssh_host = "new-bastion" if changed == "ssh_host" else "bastion"
    ssh_user = "new-tunnel" if changed == "ssh_user" else "tunnel"
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "profile", "configuration": {
        "host": database_host, "database": database_name, "handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": ssh_host, "userName": ssh_user, "authType": "PASSWORD"},
        }},
    }}])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": database_user, "password": empty_password}},
        {"c1": {"user": ssh_user, "password": empty_password}},
    ))
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([{
        "connection_name": "profile", "type": "clickhouse", "servers": ["database:8123"],
        "db": "analytics", "username": "reader", "password": empty_password, "implementation": "python",
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", "password": empty_password},
    }], sort_keys=False))
    _run_import(monkeypatch, [str(workspace), "--only", "profile", "--output", str(output)])
    saved = yaml.safe_load(output.read_text())[0]
    assert saved["servers"] == [f"{database_host}:8123"]
    assert saved["db"] == database_name
    assert saved["username"] == database_user
    assert saved["implementation"] == "python"
    assert saved["ssh_tunnel"] == {"host": ssh_host, "user": ssh_user, "port": 22}
    assert "password" not in saved


@pytest.mark.parametrize("empty_password", ["", None])
def test_partial_cluster_normalizes_existing_empty_passwords(tmp_path, monkeypatch, empty_password):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "node 1", "configuration": {
        "host": "node1.example", "handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "userName": "tunnel", "authType": "PASSWORD"},
        }},
    }}])
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": {"user": "reader"}}, {"c1": {"user": "tunnel"}},
    ))
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([{
        "connection_name": "node_1", "type": "clickhouse", "servers": ["node1.example:8123", "node2.example:8123"],
        "db": "default", "username": "reader", "password": empty_password,
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", "port": 22, "password": empty_password},
    }], sort_keys=False))
    _run_import(monkeypatch, [str(workspace), "--only", "node 1", "--output", str(output)])
    saved = yaml.safe_load(output.read_text())[0]
    assert saved["servers"] == ["node1.example:8123", "node2.example:8123"]
    assert "password" not in saved
    assert "password" not in saved["ssh_tunnel"]


@pytest.mark.parametrize("inherited_port", [{}, {"port": None}])
def test_cluster_grouping_separates_inherited_and_explicit_ssh_ports(inherited_port):
    connections = [{
        "connection_name": name, "type": "clickhouse", "servers": [f"{name}.example:8123"],
        "db": "analytics", "username": "reader",
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", **port},
    } for name, port in (("node1", inherited_port), ("node2", {"port": 22}))]
    result = DBeaverImporter("/unused")._merge_cluster_connections(connections)
    assert len(result) == 2
    assert result[0]["servers"] == ["node1.example:8123"]
    assert result[1]["servers"] == ["node2.example:8123"]


@pytest.mark.parametrize("inherited_port", [{}, {"port": None}])
def test_partial_endpoint_match_separates_inherited_and_explicit_ssh_ports(inherited_port):
    previous = {
        "connection_name": "cluster", "type": "clickhouse", "servers": ["node1.example:8123", "node2.example:8123"],
        "db": "analytics", "username": "reader",
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", **inherited_port},
    }
    incoming = {
        **previous, "connection_name": "node2", "servers": ["node2.example:8123"],
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", "port": 22},
    }
    result = DBeaverImporter("/unused")._merge_partial_connections([previous], [incoming])
    assert result == [previous, incoming]


@pytest.mark.parametrize("inherited_port", [{}, {"port": None}])
@pytest.mark.parametrize("unavailable", ["database", "ssh"])
def test_partial_import_never_reuses_credentials_when_inherited_ssh_port_becomes_explicit(
    tmp_path, monkeypatch, capsys, inherited_port, unavailable,
):
    workspace = _write_dbeaver_workspace(tmp_path, [{"id": "c1", "name": "profile", "configuration": {
        "host": "database", "database": "analytics", "handlers": {"ssh_tunnel": {
            "enabled": True, "properties": {"host": "bastion", "userName": "tunnel", "authType": "PASSWORD"},
        }},
    }}])
    credentials = {"user": "reader", "password": "database-secret"}
    ssh_credentials = {"user": "tunnel", "password": "ssh-secret"}
    (credentials if unavailable == "database" else ssh_credentials).pop("password")
    monkeypatch.setattr(DBeaverImporter, "_decrypt_credentials", lambda self: (
        {"c1": credentials}, {"c1": ssh_credentials},
    ))
    output = tmp_path / "connections.yaml"
    output.write_text(yaml.safe_dump([{
        "connection_name": "profile", "type": "clickhouse", "servers": ["database:8123"],
        "db": "analytics", "username": "reader", "password": "database-secret",
        "ssh_tunnel": {"host": "bastion", "user": "tunnel", "password": "ssh-secret", **inherited_port},
    }], sort_keys=False))
    original = output.read_bytes()
    assert original[-1:] == b"\n" and original[-2:-1] != b"\n"
    with pytest.raises(SystemExit) as failure:
        _run_import(monkeypatch, [str(workspace), "--only", "profile", "--output", str(output)])
    assert failure.value.code == 1
    assert "Cannot preserve unavailable" in capsys.readouterr().out
    assert output.read_bytes() == original
    assert not list(tmp_path.glob("connections.yaml.bak.*"))
