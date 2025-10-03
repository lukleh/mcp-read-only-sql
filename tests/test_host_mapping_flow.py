from conftest import make_recording_connector
from src.server import _display_hosts_for_connector


def test_ssh_local_display_host_maps_back_to_localhost():
    connector = make_recording_connector({
        "connection_name": "ssh_local",
        "type": "postgresql",
        "implementation": "cli",
        "servers": ["localhost:5432"],
        "db": "example",
        "username": "tester",
        "password": "secret",
        "ssh_tunnel": {
            "host": "remote.example.com",
            "user": "deploy",
            "private_key": "/tmp/key"
        }
    })

    display_hosts = _display_hosts_for_connector(connector)
    assert display_hosts == ["remote.example.com"]

    selected = connector._select_server(display_hosts[0])
    assert selected.host == "localhost"
    assert selected.port == 5432


def test_ssh_jump_display_host_matches_remote_db():
    connector = make_recording_connector({
        "connection_name": "ssh_jump",
        "type": "postgresql",
        "implementation": "cli",
        "servers": ["behind.example.com:5432"],
        "db": "example",
        "username": "tester",
        "password": "secret",
        "ssh_tunnel": {
            "host": "jump.example.com",
            "user": "deploy",
            "private_key": "/tmp/key"
        }
    })

    display_hosts = _display_hosts_for_connector(connector)
    assert display_hosts == ["behind.example.com"]

    selected = connector._select_server(display_hosts[0])
    assert selected.host == "behind.example.com"
    assert selected.port == 5432


def test_multiple_hosts_returns_unique_entries():
    connector = make_recording_connector({
        "connection_name": "multi",
        "type": "clickhouse",
        "implementation": "cli",
        "servers": [
            "server1.example.com:9000",
            "server2.example.com:9000",
            "server1.example.com:9000"
        ],
        "db": "default",
        "username": "tester",
        "password": "secret"
    })

    display_hosts = _display_hosts_for_connector(connector)
    assert display_hosts == ["server1.example.com", "server2.example.com"]
    assert connector._select_server(display_hosts[1]).host == "server2.example.com"
