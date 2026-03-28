"""Unit tests for Docker-backed test host override helpers."""

from tests.docker_test_config import (
    apply_docker_test_overrides,
    docker_test_server,
    docker_test_server_string,
    docker_test_servers,
    docker_test_ssh_tunnel,
)


def test_docker_test_helpers_default_to_localhost(monkeypatch):
    monkeypatch.delenv("TEST_DOCKER_HOST", raising=False)
    monkeypatch.delenv("TEST_SSH_HOST", raising=False)
    monkeypatch.delenv("TEST_POSTGRES_PORT", raising=False)
    monkeypatch.delenv("TEST_CLICKHOUSE_PORT", raising=False)
    monkeypatch.delenv("TEST_SSH_PORT", raising=False)

    assert docker_test_server("postgresql") == {"host": "localhost", "port": 5432}
    assert docker_test_server("clickhouse") == {"host": "localhost", "port": 9000}
    assert docker_test_server_string("postgresql") == "localhost:5432"
    assert docker_test_servers("clickhouse", 2) == ["localhost:9000", "localhost:9000"]
    assert docker_test_ssh_tunnel(password="secret") == {
        "enabled": True,
        "host": "localhost",
        "port": 2222,
        "user": "tunnel",
        "password": "secret",
    }


def test_docker_test_helpers_honor_env_overrides(monkeypatch):
    monkeypatch.setenv("TEST_DOCKER_HOST", "db.test")
    monkeypatch.setenv("TEST_SSH_HOST", "ssh.test")
    monkeypatch.setenv("TEST_POSTGRES_PORT", "15432")
    monkeypatch.setenv("TEST_CLICKHOUSE_PORT", "19000")
    monkeypatch.setenv("TEST_SSH_PORT", "3222")

    assert docker_test_server("postgresql") == {"host": "db.test", "port": 15432}
    assert docker_test_server("clickhouse") == {"host": "db.test", "port": 19000}
    assert docker_test_server_string("clickhouse") == "db.test:19000"
    assert docker_test_ssh_tunnel(private_key="/tmp/key") == {
        "enabled": True,
        "host": "ssh.test",
        "port": 3222,
        "user": "tunnel",
        "private_key": "/tmp/key",
    }


def test_apply_docker_test_overrides_rewrites_local_defaults(monkeypatch):
    monkeypatch.setenv("TEST_DOCKER_HOST", "db.test")
    monkeypatch.setenv("TEST_SSH_HOST", "ssh.test")
    monkeypatch.setenv("TEST_POSTGRES_PORT", "15432")
    monkeypatch.setenv("TEST_SSH_PORT", "3222")

    original = {
        "type": "postgresql",
        "servers": [
            {"host": "localhost", "port": 5432},
            "127.0.0.1:5432",
            "remote.example.com:5432",
        ],
        "ssh_tunnel": {
            "enabled": True,
            "host": "localhost",
            "port": 2222,
            "user": "tunnel",
        },
    }

    updated = apply_docker_test_overrides(original)

    assert updated["servers"] == [
        {"host": "db.test", "port": 15432},
        "db.test:15432",
        "remote.example.com:5432",
    ]
    assert updated["ssh_tunnel"]["host"] == "ssh.test"
    assert updated["ssh_tunnel"]["port"] == 3222
    assert original["servers"][0] == {"host": "localhost", "port": 5432}
    assert original["ssh_tunnel"]["host"] == "localhost"


def test_apply_docker_test_overrides_preserves_nondefault_ports(monkeypatch):
    monkeypatch.setenv("TEST_DOCKER_HOST", "db.test")
    monkeypatch.setenv("TEST_SSH_HOST", "ssh.test")
    monkeypatch.setenv("TEST_POSTGRES_PORT", "15432")
    monkeypatch.setenv("TEST_SSH_PORT", "3222")

    updated = apply_docker_test_overrides(
        {
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 9999}],
            "ssh_tunnel": {"host": "localhost", "port": 2299, "user": "tunnel"},
        }
    )

    assert updated["servers"] == [{"host": "db.test", "port": 9999}]
    assert updated["ssh_tunnel"]["host"] == "ssh.test"
    assert updated["ssh_tunnel"]["port"] == 2299
