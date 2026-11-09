"""Executor cancellation cannot restore a removed managed result path."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Event

import pytest
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError

from mcp_read_only_sql.connectors.clickhouse.python import ClickHousePythonConnector
from tests.test_run_query_file_output import build_stub_server, make_runtime_paths


class _Client:
    def __init__(self, raw_stream):
        self.raw_stream = raw_stream
        self.closed = Event()

    def close(self):
        self.closed.set()


async def _until(predicate):
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("worker did not finish")


def _connector(config, monkeypatch, client):
    connector = ClickHousePythonConnector(config)
    connector.connection_timeout = 0.02
    connector.query_timeout = 0.03
    selected = connector.servers[0]
    connector._decided_settings[(selected.host, selected.port)] = {"readonly": 1}
    monkeypatch.setattr(connector, "_create_client", lambda *args: client)
    return connector


@pytest.mark.anyio
async def test_late_http_response_cannot_recreate_timed_out_mcp_result(
    clickhouse_config, monkeypatch, tmp_path
):
    entered = Event()
    release = Event()
    stream = BytesIO(b"secret\nvalue\n")

    def raw_stream(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return stream

    client = _Client(raw_stream)
    connector = _connector(clickhouse_config, monkeypatch, client)
    paths = make_runtime_paths(tmp_path)
    server = build_stub_server(connector, paths)
    try:
        with pytest.raises(ToolError, match="timeout"):
            await server.mcp._tool_manager.call_tool(
                "run_query_read_only",
                {"connection_name": connector.name, "query": "SELECT 1"},
                Context(mcp_server=server.mcp, subscriptions=server.mcp._subscriptions),
                convert_result=False,
            )
        assert entered.is_set()
        assert list(paths.results_dir.rglob("*.tsv")) == []
    finally:
        release.set()
    await _until(lambda: stream.closed)
    assert client.closed.is_set()
    assert list(paths.results_dir.rglob("*.tsv")) == []


@pytest.mark.anyio
async def test_hard_timeout_does_not_wait_another_connection_timeout(
    clickhouse_config, monkeypatch, tmp_path
):
    entered = Event()
    release = Event()
    stream = BytesIO(b"late\ndata\n")

    def raw_stream(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return stream

    client = _Client(raw_stream)
    connector = _connector(clickhouse_config, monkeypatch, client)
    connector.connection_timeout = 0.5
    connector.query_timeout = 30
    connector.hard_timeout = 0.05
    paths = make_runtime_paths(tmp_path)
    server = build_stub_server(connector, paths)
    loop = asyncio.get_running_loop()
    start = loop.time()
    try:
        with pytest.raises(ToolError, match="hard timeout"):
            await server.mcp._tool_manager.call_tool(
                "run_query_read_only",
                {"connection_name": connector.name, "query": "SELECT 1"},
                Context(mcp_server=server.mcp, subscriptions=server.mcp._subscriptions),
                convert_result=False,
            )
        assert loop.time() - start < 0.3
        assert list(paths.results_dir.rglob("*.tsv")) == []
    finally:
        release.set()
    if entered.is_set():
        await _until(lambda: stream.closed)
    assert list(paths.results_dir.rglob("*.tsv")) == []


@pytest.mark.anyio
async def test_cancelled_stream_is_closed_without_further_output(
    clickhouse_config, monkeypatch, tmp_path
):
    entered = Event()
    released = Event()

    class Stream(BytesIO):
        def read(self, size=-1):
            entered.set()
            assert released.wait(3)
            return b"late data"

        def close(self):
            released.set()
            super().close()

    stream = Stream()
    client = _Client(lambda *args, **kwargs: stream)
    connector = _connector(clickhouse_config, monkeypatch, client)
    connector.query_timeout = 5
    path = tmp_path / "result.tsv"
    task = asyncio.create_task(connector.execute_query_to_file("SELECT 1", path))
    try:
        await _until(entered.is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _until(lambda: stream.closed)
        assert client.closed.is_set()
        assert path.read_bytes() == b""
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        released.set()


@pytest.mark.anyio
async def test_cancelled_queued_job_closes_its_output_before_worker_starts(
    clickhouse_config, monkeypatch, tmp_path
):
    release = Event()
    executor = ThreadPoolExecutor(max_workers=1)
    blocker = executor.submit(release.wait)
    loop = asyncio.get_running_loop()
    run_in_executor = loop.run_in_executor
    handles = []
    import mcp_read_only_sql.connectors.clickhouse.python as module

    open_output = module._open_result_output

    def recording_output(path):
        output = open_output(path)
        handles.append(output)
        return output

    def no_client(*args):
        raise AssertionError("cancelled queued job must not connect")

    connector = ClickHousePythonConnector(clickhouse_config)
    connector.connection_timeout = 0.02
    monkeypatch.setattr(module, "_open_result_output", recording_output)
    monkeypatch.setattr(connector, "_create_client", no_client)
    monkeypatch.setattr(loop, "run_in_executor", lambda pool, fn, *args: run_in_executor(executor, fn, *args))
    task = asyncio.create_task(connector.execute_query_to_file("SELECT 1", tmp_path / "result.tsv"))
    try:
        await _until(lambda: bool(handles))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert handles[0].closed
    finally:
        release.set()
        await asyncio.wrap_future(blocker)
        executor.shutdown(wait=True)
