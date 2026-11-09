"""Exercise endpoint formatting through the installed driver's real HTTP stack."""

import socket
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest
from clickhouse_connect.driver import httpclient
from clickhouse_connect.driver.exceptions import ClickHouseError

from mcp_read_only_sql.config import Connection
from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector

_REACHED_ENDPOINT = "ClickHouse endpoint probe reached the expected listener"


@contextmanager
def _http_endpoint(host):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append(self.headers["Host"])
            # A recognizable server error proves URI parsing and connection
            # succeeded, without emulating the driver's initialization protocol.
            body = _REACHED_ENDPOINT.encode()
            self.send_response(400)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    class Server(HTTPServer):
        address_family = socket.AF_INET6 if ":" in host else socket.AF_INET

    server = Server((host, 0), Handler)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield server.server_port, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


@pytest.fixture(autouse=True)
def _isolated_proxy_settings(monkeypatch):
    for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "no_proxy", "NO_PROXY"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("host", ["::1", "[::1]", "127.0.0.1", "localhost"])
@pytest.mark.parametrize("canonical_input", [False, True])
def test_clickhouse_http_endpoint_reaches_real_listener(host, canonical_input):
    listener_host = "::1" if ":" in host else "127.0.0.1"
    with _http_endpoint(listener_host) as (port, requests):
        connection = Connection({
            "connection_name": "http_endpoint", "type": "clickhouse",
            "implementation": "python", "servers": [{"host": host, "port": port}],
            "db": "default", "username": "reader", "connection_timeout": 1,
        })
        server = connection.servers[0]
        connector = ClickHousePythonConnector(connection)
        with pytest.raises(ClickHouseError):
            connector._create_client(server.host if canonical_input else host, server.port, "default", None, False)
        assert requests == [f"[{listener_host}]:{port}" if ":" in listener_host else f"{host}:{port}"]
        assert server.host == ("::1" if ":" in host else host)


def test_tunnel_endpoint_stays_on_its_forwarded_http_port():
    with _http_endpoint("127.0.0.1") as (port, requests):
        connector = ClickHousePythonConnector(Connection({
            "connection_name": "tunnel_endpoint", "type": "clickhouse",
            "implementation": "python", "servers": ["[::1]:9000"],
            "db": "default", "username": "reader", "connection_timeout": 1,
        }))
        with pytest.raises(ClickHouseError):
            connector._create_client("127.0.0.1", port, "default", 9000, True)
        assert requests == [f"127.0.0.1:{port}"]


def test_ipv6_no_proxy_bypasses_configured_http_proxy(monkeypatch):
    with _http_endpoint("::1") as (port, requests), _http_endpoint("127.0.0.1") as (proxy_port, proxy_requests):
        monkeypatch.setenv("http_proxy", f"http://127.0.0.1:{proxy_port}")
        # Preserve each driver's matching convention: modern drivers accept a
        # bare host and legacy drivers require URI brackets from their caller.
        proxy_host = "::1" if hasattr(httpclient, "format_uri_host") else "[::1]"
        monkeypatch.setenv("no_proxy", proxy_host)
        connector = ClickHousePythonConnector(Connection({
            "connection_name": "direct_ipv6", "type": "clickhouse",
            "implementation": "python", "servers": [{"host": "::1", "port": port}],
            "db": "default", "username": "reader", "connection_timeout": 1,
        }))
        with pytest.raises(ClickHouseError):
            connector._create_client("::1", port, "default", None, False)
        assert requests == [f"[::1]:{port}"]
        assert proxy_requests == []
