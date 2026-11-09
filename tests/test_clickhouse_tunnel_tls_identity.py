"""TLS verifies the remote database identity through a forwarded TCP port."""

import asyncio
import os
import select
import shutil
import socket
import ssl
import subprocess
from contextlib import asynccontextmanager, contextmanager
from threading import Event, Thread

import clickhouse_connect
import pytest

from mcp_read_only_sql.config import Connection
from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from mcp_read_only_sql.errors import ConnectorError


@pytest.fixture(autouse=True)
def isolated_client_environment(monkeypatch):
    for name in list(os.environ):
        if name.startswith("CLICKHOUSE_") or name.lower() in {"http_proxy", "https_proxy"}:
            monkeypatch.delenv(name, raising=False)


@pytest.fixture
def tls_certificate(tmp_path):
    cert = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=database.example", "-addext", "subjectAltName=DNS:database.example",
         "-keyout", str(key), "-out", str(cert)],
        capture_output=True, check=True, timeout=10,
    )
    return cert, key


@contextmanager
def _tls_forward(cert, key):
    stop = Event()
    identities = []
    received = []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.set_servername_callback(lambda connection, name, ctx: identities.append(name))
    listeners = []
    workers = []
    for _ in range(2):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.05)
        listeners.append(listener)
    endpoint, forward = listeners

    def serve():
        while not stop.is_set():
            try:
                raw, _ = endpoint.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                raw.settimeout(1)
                with context.wrap_socket(raw, server_side=True) as connection:
                    data = connection.recv(4096)
                    if data:
                        received.append(data)
                    if data.startswith(b"POST"):
                        body = b"TLS identity verified"
                        connection.sendall(
                            b"HTTP/1.1 400 Bad Request\r\nContent-Length: "
                            + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
                        )
            except OSError:
                raw.close()

    def relay(local):
        try:
            with local, socket.create_connection(endpoint.getsockname(), timeout=1) as remote:
                peers = {local: remote, remote: local}
                while not stop.is_set():
                    readable, _, _ = select.select(list(peers), [], [], 0.05)
                    for stream in readable:
                        data = stream.recv(65536)
                        if not data:
                            return
                        peers[stream].sendall(data)
        except OSError:
            # Certificate rejection and protocol EOF close either side.
            pass

    def accept():
        while not stop.is_set():
            try:
                local, _ = forward.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            worker = Thread(target=relay, args=(local,), daemon=True)
            workers.append(worker)
            worker.start()

    threads = [Thread(target=serve, daemon=True), Thread(target=accept, daemon=True)]
    for thread in threads:
        thread.start()
    try:
        yield forward.getsockname()[1], identities, received
    finally:
        stop.set()
        for listener in listeners:
            listener.close()
        for thread in threads + workers:
            thread.join(timeout=2)
            assert not thread.is_alive()


def _connector(kind, remote_host):
    return kind(Connection({
        "connection_name": "tls_identity", "type": "clickhouse",
        "implementation": "python" if kind is ClickHousePythonConnector else "cli",
        "servers": [{"host": remote_host, "port": 9440}], "db": "default",
        "username": "reader", "connection_timeout": 1, "query_timeout": 2,
    }))


@pytest.mark.anyio
@pytest.mark.parametrize("matches", [True, False])
async def test_python_tunnel_keeps_remote_certificate_identity(tls_certificate, monkeypatch, matches):
    cert, key = tls_certificate
    real_client = clickhouse_connect.get_client

    def trusted_client(**kwargs):
        return real_client(**kwargs, ca_cert=str(cert))

    monkeypatch.setattr(clickhouse_connect, "get_client", trusted_client)
    connector = _connector(ClickHousePythonConnector, "database.example" if matches else "other.example")
    with _tls_forward(cert, key) as (port, identities, received):
        @asynccontextmanager
        async def tunnel(server=None):
            yield port

        monkeypatch.setattr(connector, "_get_ssh_tunnel", tunnel)
        # The local endpoint intentionally rejects the version query after
        # TLS. Receiving application bytes proves certificate verification.
        with pytest.raises(ConnectorError):
            await connector.execute_query("SELECT 1")
        assert bool(received) is matches
        assert identities and all(name == connector.servers[0].host for name in identities)


@pytest.mark.anyio
@pytest.mark.parametrize("matches", [True, False])
async def test_cli_tunnel_verifies_identity_or_reports_required_upgrade(
    tls_certificate, monkeypatch, tmp_path, matches
):
    binary = shutil.which("clickhouse-client")
    if binary is None:
        pytest.skip("clickhouse-client is unavailable")
    help_result = await asyncio.to_thread(
        subprocess.run, [binary, "--help"], capture_output=True, text=True, timeout=10, check=True
    )
    supported = "--tls-sni-override" in help_result.stdout
    cert, key = tls_certificate
    config = tmp_path / ".clickhouse-client" / "config.xml"
    config.parent.mkdir()
    config.write_text(
        "<clickhouse><openSSL><client>"
        f"<caConfig>{cert}</caConfig><loadDefaultCAFile>false</loadDefaultCAFile>"
        "<verificationMode>strict</verificationMode><extendedVerification>true</extendedVerification>"
        "<invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler>"
        "</client></openSSL></clickhouse>"
    )
    monkeypatch.setenv("HOME", str(tmp_path))
    connector = _connector(ClickHouseCLIConnector, "database.example" if matches else "other.example")
    connector._decided_settings[(connector.servers[0].host, 9440)] = {"readonly": 1}
    with _tls_forward(cert, key) as (port, identities, received):
        @asynccontextmanager
        async def tunnel(server=None):
            yield port

        monkeypatch.setattr(connector, "_get_ssh_tunnel", tunnel)
        with pytest.raises(ConnectorError) as info:
            await connector.execute_query("SELECT 1")
        if supported:
            assert bool(received) is matches
            assert identities and all(name == connector.servers[0].host for name in identities)
        else:
            assert "26.1 or newer" in str(info.value)
            assert "implementation: python" in str(info.value)
            assert not received and not identities
