"""Shared host and port helpers for Docker-backed test configurations."""

from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_DEFAULT_PORTS = {
    "postgresql": 5432,
    "clickhouse": 9000,
}
_DEFAULT_SSH_PORT = 2222


def docker_test_host() -> str:
    """Return the host used for public Docker test database ports."""
    return os.environ.get("TEST_DOCKER_HOST", "localhost")


def docker_test_ssh_host() -> str:
    """Return the host used for Docker-backed SSH tunnel tests."""
    return os.environ.get("TEST_SSH_HOST", docker_test_host())


def docker_test_port(db_type: str) -> int:
    """Return the externally reachable port for the given Docker-backed database."""
    if db_type == "postgresql":
        return int(
            os.environ.get("TEST_POSTGRES_PORT", str(_DEFAULT_PORTS["postgresql"]))
        )
    if db_type == "clickhouse":
        return int(
            os.environ.get("TEST_CLICKHOUSE_PORT", str(_DEFAULT_PORTS["clickhouse"]))
        )
    raise ValueError(f"Unsupported docker test database type: {db_type}")


def docker_test_ssh_port() -> int:
    """Return the externally reachable SSH port for Docker-backed tunnel tests."""
    return int(os.environ.get("TEST_SSH_PORT", str(_DEFAULT_SSH_PORT)))


def docker_test_server(db_type: str, port: int | None = None) -> dict[str, Any]:
    """Build a Docker-backed server definition for the requested database."""
    return {
        "host": docker_test_host(),
        "port": docker_test_port(db_type) if port is None else port,
    }


def docker_test_server_string(db_type: str, port: int | None = None) -> str:
    """Build a Docker-backed host:port string for the requested database."""
    server = docker_test_server(db_type, port=port)
    return f"{server['host']}:{server['port']}"


def docker_test_servers(db_type: str, count: int, port: int | None = None) -> list[str]:
    """Build repeated Docker-backed host:port strings for cluster-style test configs."""
    return [docker_test_server_string(db_type, port=port) for _ in range(count)]


DEFAULT_TEST_KNOWN_HOSTS = "/tmp/docker_test_known_hosts"


def docker_test_known_hosts() -> str:
    """known_hosts file the tunnels record the bastion in during a test run.

    The bastion image generates its host keys at build time, so a rebuilt
    image would look like a changed key to a file kept from an earlier run.
    run_tests.sh and the session fixture in conftest remove the default file
    before the run starts.
    """
    return os.environ.get("TEST_SSH_KNOWN_HOSTS", DEFAULT_TEST_KNOWN_HOSTS)


def docker_test_ssh_tunnel(
    *,
    enabled: bool = True,
    user: str = "tunnel",
    private_key: str | None = None,
    password: str | None = None,
    known_hosts_file: str | None = None,
) -> dict[str, Any]:
    """Build a Docker-backed SSH tunnel configuration."""
    config: dict[str, Any] = {
        "enabled": enabled,
        "host": docker_test_ssh_host(),
        "port": docker_test_ssh_port(),
        "user": user,
        "known_hosts_file": known_hosts_file or docker_test_known_hosts(),
    }
    if private_key is not None:
        config["private_key"] = private_key
    if password is not None:
        config["password"] = password
    return config


def apply_docker_test_overrides(config: dict[str, Any]) -> dict[str, Any]:
    """Rewrite localhost-style Docker test configs to use externally supplied hosts/ports."""
    updated = deepcopy(config)
    db_type = updated.get("type")
    default_port = _DEFAULT_PORTS.get(db_type)

    servers = updated.get("servers")
    if isinstance(servers, list):
        rewritten_servers = []
        for server in servers:
            if isinstance(server, dict):
                rewritten_servers.append(
                    _rewrite_server_dict(server, db_type, default_port)
                )
            elif isinstance(server, str):
                rewritten_servers.append(
                    _rewrite_server_string(server, db_type, default_port)
                )
            else:
                rewritten_servers.append(server)
        updated["servers"] = rewritten_servers

    ssh_tunnel = updated.get("ssh_tunnel")
    if isinstance(ssh_tunnel, dict):
        updated["ssh_tunnel"] = _rewrite_ssh_tunnel(ssh_tunnel)

    return updated


def _rewrite_server_dict(
    server: dict[str, Any], db_type: str | None, default_port: int | None
) -> dict[str, Any]:
    updated = dict(server)
    host = updated.get("host")
    if host in _LOCAL_HOSTS:
        updated["host"] = docker_test_host()
        if (
            default_port is not None
            and updated.get("port", default_port) == default_port
        ):
            updated["port"] = docker_test_port(db_type)
    return updated


def _rewrite_server_string(
    server: str, db_type: str | None, default_port: int | None
) -> str:
    if ":" in server:
        host, raw_port = server.rsplit(":", 1)
        port = int(raw_port)
        if host in _LOCAL_HOSTS:
            host = docker_test_host()
            if default_port is not None and port == default_port:
                port = docker_test_port(db_type)
        return f"{host}:{port}"

    if server in _LOCAL_HOSTS and db_type is not None:
        return docker_test_server_string(db_type)

    return server


def _rewrite_ssh_tunnel(ssh_tunnel: dict[str, Any]) -> dict[str, Any]:
    updated = dict(ssh_tunnel)
    host = updated.get("host")
    if host in _LOCAL_HOSTS:
        updated["host"] = docker_test_ssh_host()
        if updated.get("port", _DEFAULT_SSH_PORT) == _DEFAULT_SSH_PORT:
            updated["port"] = docker_test_ssh_port()
    return updated
