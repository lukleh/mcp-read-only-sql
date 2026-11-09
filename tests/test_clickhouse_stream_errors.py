"""Transport failures remain operational errors after raw_stream returns."""

import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from threading import Event, Thread

import pytest
import urllib3
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from urllib3.exceptions import ProtocolError
from urllib3.util import Timeout

from mcp_read_only_sql.connectors.clickhouse.python import (
    ClickHousePythonConnector,
    _QueryCancellation,
)
from mcp_read_only_sql.errors import ConnectorError
from tests.test_run_query_file_output import build_stub_server, make_runtime_paths


@contextmanager
def _stalled_response():
    release = Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(200)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            self.wfile.write(b"column\n")
            self.wfile.flush()
            release.wait(3)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    pool = urllib3.PoolManager()
    thread.start()
    try:
        yield lambda: pool.request(
            "POST", f"http://127.0.0.1:{server.server_port}", preload_content=False,
            timeout=Timeout(connect=1, read=0.05), retries=False,
        )
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        pool.clear()
        assert not thread.is_alive()


@pytest.mark.anyio
async def test_stalled_raw_response_reports_operational_error_and_removes_file(
    clickhouse_config, monkeypatch, tmp_path
):
    responses = []
    closed = Event()
    with _stalled_response() as response:
        class Client:
            def raw_stream(self, *args, **kwargs):
                result = response()
                responses.append(result)
                return result

            def close(self):
                closed.set()

        connector = ClickHousePythonConnector(clickhouse_config)
        monkeypatch.setattr(connector, "_open_client", lambda *args: (Client(), {"readonly": 1}))
        paths = make_runtime_paths(tmp_path)
        server = build_stub_server(connector, paths)
        with pytest.raises(ToolError, match="ClickHouse: response stream failed:"):
            await server.mcp._tool_manager.call_tool(
                "run_query_read_only",
                {"connection_name": connector.name, "query": "SELECT 1"},
                Context(mcp_server=server.mcp, subscriptions=server.mcp._subscriptions),
                convert_result=False,
            )
    assert closed.is_set()
    assert responses and responses[0].closed
    assert list(paths.results_dir.rglob("*.tsv")) == []


@pytest.mark.parametrize("error", [ProtocolError("connection interrupted"), OSError("connection reset")])
def test_other_raw_read_failures_close_resources(clickhouse_config, monkeypatch, tmp_path, error):
    class Stream(BytesIO):
        def read(self, size=-1):
            raise error

    stream = Stream()
    closed = Event()

    class Client:
        def raw_stream(self, *args, **kwargs):
            return stream

        def close(self):
            closed.set()

    connector = ClickHousePythonConnector(clickhouse_config)
    monkeypatch.setattr(connector, "_open_client", lambda *args: (Client(), {"readonly": 1}))
    with pytest.raises(ConnectorError, match="ClickHouse: response stream failed:"):
        connector._execute_sync_query_to_file("localhost", 8123, "testdb", "SELECT 1", output_path=str(tmp_path / "result.tsv"))
    assert stream.closed and closed.is_set()


def test_cancellation_after_executor_shutdown_retains_worker_cleanup(clickhouse_config):
    async def run():
        loop = asyncio.get_running_loop()
        await loop.shutdown_default_executor()
        output = BytesIO()
        cancellation = _QueryCancellation(output)
        resource = BytesIO()
        future = loop.create_future()
        connector = ClickHousePythonConnector(clickhouse_config)
        connector._stop_worker(cancellation, future)
        assert output.closed
        with pytest.raises(TimeoutError, match="cancelled"):
            cancellation.watch(resource)
        assert resource.closed
        future.set_result(None)
        await asyncio.sleep(0)

    asyncio.run(run())
