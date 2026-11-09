"""Management validation and server loading share structural configuration rules."""

import pytest
import yaml

from mcp_read_only_sql.config import load_connections
from mcp_read_only_sql.tools.validate_config import validate_config

BASE = {
    "connection_name": "database",
    "type": "postgresql",
    "servers": ["localhost:5432"],
    "db": "analytics",
    "username": "reader",
}


def write_config(tmp_path, content):
    path = tmp_path / "connections.yaml"
    path.write_text(yaml.safe_dump(content, sort_keys=False))
    data = path.read_bytes()
    assert data[-1:] == b"\n" and data[-2:-1] != b"\n"
    return path


@pytest.mark.parametrize("update", [
    {"max_result_bytes": 1024},
    {"password_env": "OLD_PASSWORD"},
    {"query_timeout": True},
    {"connection_timeout": False},
    {"username": 123},
    {"description": []},
    {"servers": "localhost"},
    {"servers": [{"host": "localhost"}]},
    {"servers": [{"host": "localhost", "port": True}]},
    {"servers": [{"host": "localhost", "port": 5432.5}]},
    {"servers": ["localhost:0"]},
    {"servers": ["localhost:65536"]},
    {"servers": [":5432"]},
    {"allowed_functions": [None]},
    {"ssh_tunnel": {"host": "", "user": "tunnel"}},
    {"ssh_tunnel": {"host": "bastion", "user": ""}},
    {"ssh_tunnel": {"host": "bastion", "user": "tunnel", "port": 0}},
    {"ssh_tunnel": {"host": "bastion", "user": "tunnel", "port": True}},
    {"ssh_tunnel": {"host": "bastion", "user": "tunnel", "ssh_password": "obsolete"}},
])
def test_invalid_config_is_rejected_by_runtime_and_validator(tmp_path, update):
    path = write_config(tmp_path, [{**BASE, **update}])
    with pytest.raises(ValueError):
        load_connections(path)
    assert not validate_config(path)


def test_duplicate_names_are_rejected_by_validator(tmp_path):
    path = write_config(tmp_path, [BASE, BASE])
    with pytest.raises(ValueError, match="Duplicate connection name"):
        load_connections(path)
    assert not validate_config(path)


@pytest.mark.parametrize("update", [
    {},
    {"db": " analytics ", "allowed_databases": ["analytics"]},
    {"ssh_tunnel": {"enabled": False}},
    {"ssh_tunnel": {"host": "bastion", "user": "tunnel"}},
    {"ssh_tunnel": {"host": "production-bastion"}},
    {"ssh_tunnel": {"host": "production-bastion", "user": None, "port": None}},
    {"servers": ["localhost"]},
])
def test_valid_runtime_config_passes_validator(tmp_path, update):
    path = write_config(tmp_path, [{**BASE, **update}])
    assert load_connections(path)
    assert validate_config(path)


def test_validator_keeps_private_key_file_diagnostics(tmp_path, capsys):
    path = write_config(tmp_path, [{**BASE, "ssh_tunnel": {
        "host": "bastion", "user": "tunnel", "private_key": str(tmp_path / "missing"),
    }}])
    assert load_connections(path)
    assert not validate_config(path)
    assert "SSH private key not found" in capsys.readouterr().out


@pytest.mark.parametrize("explicit", [False, True])
def test_validator_keeps_inferred_database_warning(tmp_path, capsys, explicit):
    config = {key: value for key, value in BASE.items() if key != "db"}
    config["allowed_databases"] = ["analytics", "archive"]
    if explicit:
        config["default_database"] = "analytics"
    path = write_config(tmp_path, [config])
    connection = load_connections(path)["database"]
    assert connection.default_database_inferred is not explicit
    assert validate_config(path)
    output = capsys.readouterr().out
    assert ("No default database specified" in output) is not explicit
