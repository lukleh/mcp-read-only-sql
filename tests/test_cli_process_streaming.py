"""Exercise the shared client runner with actual OS pipes and child processes."""

import asyncio
import os
import sys

import pytest

from mcp_read_only_sql.connectors.clickhouse.cli import ClickHouseCLIConnector
from mcp_read_only_sql.connectors.postgresql.cli import PostgreSQLCLIConnector
from mcp_read_only_sql.utils.cli_process import stream_cli_process


@pytest.mark.anyio
async def test_large_rows_unicode_empty_rows_and_stderr_use_real_pipes():
    chunks = []
    payload = "x\tvalue\n" + "é" * 100000 + "\r\n\n"
    code = (
        "import sys; "
        "sys.stderr.buffer.write(b'e' * 200000); sys.stderr.flush(); "
        "payload = 'x\\tvalue\\n' + 'é' * 100000 + '\\r\\n\\n'; "
        "sys.stdout.buffer.write(payload.encode()); sys.stdout.flush()"
    )
    result = await stream_cli_process(
        [sys.executable, "-c", code], env=dict(os.environ), timeout=5,
        label="test-client", emit=chunks.append,
    )
    assert "".join(chunks) == payload.removesuffix("\n")
    assert result.stderr == b"e" * 200000
    assert result.returncode == 0


@pytest.mark.anyio
async def test_client_without_input_gets_eof_instead_of_protocol_stdin():
    chunks = []
    result = await stream_cli_process(
        [sys.executable, "-c", "import sys; assert sys.stdin.buffer.read() == b''; print('EOF')"],
        env=dict(os.environ), timeout=2, label="test-client", emit=chunks.append,
    )
    assert result.returncode == 0
    assert "".join(chunks) == "EOF"


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["sink", "cancel", "timeout", "stdin"])
async def test_every_failure_reaps_real_child(failure, monkeypatch):
    processes = []
    spawn = asyncio.create_subprocess_exec
    ready = asyncio.Event()

    async def recording_spawn(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        processes.append(process)
        return process

    def emit(text):
        ready.set()
        if failure == "sink":
            raise OSError("output disk full")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", recording_spawn)
    code = "import sys,time; sys.stdout.write('header\\nrow\\n'); sys.stdout.flush(); time.sleep(10)"
    if failure == "stdin":
        # Closing stdin immediately makes a large password/input write fail.
        code = "import os,time; os.close(0); time.sleep(10)"
    task = asyncio.create_task(stream_cli_process(
        [sys.executable, "-c", code], env=dict(os.environ),
        timeout=0.1 if failure == "timeout" else 5,
        label="test-client", emit=emit,
        stdin_data=b"x" * 1000000 if failure == "stdin" else None,
    ))
    if failure == "cancel":
        await asyncio.wait_for(ready.wait(), timeout=2)
        task.cancel()
    expected = {
        "sink": OSError, "cancel": asyncio.CancelledError,
        "timeout": TimeoutError, "stdin": (BrokenPipeError, ConnectionResetError),
    }[failure]
    with pytest.raises(expected):
        await task
    assert len(processes) == 1
    assert processes[0].returncode is not None


@pytest.mark.docker
@pytest.mark.usefixtures("docker_check")
@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["postgresql", "clickhouse"])
async def test_real_cli_exports_large_row_to_file(kind, request, tmp_path):
    if kind == "postgresql":
        connector = PostgreSQLCLIConnector(request.getfixturevalue("postgres_config"))
    else:
        connector = ClickHouseCLIConnector(request.getfixturevalue("clickhouse_config"))
    output = tmp_path / "result.tsv"
    await connector.execute_query_to_file("SELECT repeat('a', 100000) AS wide", output)
    assert output.read_text(encoding="utf-8") == "wide\n" + "a" * 100000
