import asyncio
import os
import signal
import sys
from threading import Event

import pytest

from mcp_read_only_sql.config.connection import SSHTunnelConfig
from mcp_read_only_sql.utils.ssh_tunnel_cli import CLISSHTunnel, HostKeySurvey
from mcp_read_only_sql.utils.timeout_wrapper import HardTimeoutError, with_hard_timeout


@pytest.mark.anyio
async def test_cli_tunnel_waits_until_forwarded_port_accepts_connections(monkeypatch):
    """Agent-only CLI tunnel should return only after the local listener is ready."""
    ssh_config = SSHTunnelConfig.from_dict(
        {
            "host": "bastion.example.com",
            "port": 22,
            "user": "tunnel",
        }
    )
    assert ssh_config is not None

    class FakeProcess:
        returncode = None

        async def communicate(self):
            return b"", b""

    class FakeWriter:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    ssh_args = []

    async def fake_create_subprocess_exec(*args, **kwargs):
        ssh_args.extend(args)
        return FakeProcess()

    attempts = 0

    async def fake_open_connection(host, port):
        nonlocal attempts
        attempts += 1
        assert host == "127.0.0.1"
        assert port == 45454
        if attempts < 3:
            raise ConnectionRefusedError
        return object(), FakeWriter()

    async def fake_sleep(delay):
        assert delay == 0.25

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    # The fake ssh records nothing and this test is about the port polling,
    # so treat the bastion as pinned rather than reading ~/.ssh/known_hosts.
    monkeypatch.setattr(
        CLISSHTunnel,
        "_survey",
        lambda self, options, destination: HostKeySurvey(
            "bastion.example.com", "/dev/null", pinned=True, certified=False
        ),
    )
    tunnel = CLISSHTunnel(ssh_config, "db.internal", 5432)
    monkeypatch.setattr(tunnel, "_find_free_port", lambda: 45454)

    local_port = await tunnel.start()

    assert local_port == 45454
    assert attempts == 3
    assert ssh_args[0] == "ssh"
    assert "-i" not in ssh_args
    assert "tunnel@bastion.example.com" in ssh_args


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["readiness", "host_key_survey"])
async def test_cancelled_startup_reaps_child(tmp_path, monkeypatch, stage):
    """Cancellation owns cleanup before the connector's context is entered."""
    config = SSHTunnelConfig.from_dict(
        {
            "host": "bastion.example.com",
            "user": "tunnel",
            "host_key_checking": "accept-new" if stage == "host_key_survey" else "yes",
            "known_hosts_file": str(tmp_path / "known_hosts"),
        }
    )
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    spawn = asyncio.create_subprocess_exec
    reached = asyncio.Event()
    release_survey = Event()
    processes = []
    surveys = 0

    async def fake_exec(*args, **kwargs):
        process = await spawn(
            sys.executable, "-c", "import time; time.sleep(60)", preexec_fn=os.setsid
        )
        processes.append(process)
        return process

    class Writer:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    async def fake_open(host, port):
        if stage == "readiness":
            reached.set()
            await asyncio.Event().wait()
        return object(), Writer()

    loop = asyncio.get_running_loop()

    def survey(options, destination):
        nonlocal surveys
        surveys += 1
        if surveys == 2:
            loop.call_soon_threadsafe(reached.set)
            release_survey.wait(5)
        return HostKeySurvey("bastion.example.com", str(tmp_path / "known_hosts"), False, False)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(asyncio, "open_connection", fake_open)
    monkeypatch.setattr(tunnel, "_survey", survey)
    task = asyncio.create_task(tunnel.start())
    try:
        await asyncio.wait_for(reached.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(processes) == 1
        assert processes[0].returncode is not None
        assert tunnel.ssh_process is None
    finally:
        release_survey.set()
        await tunnel.stop()
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


@pytest.mark.anyio
async def test_stop_reaps_already_exited_child(monkeypatch):
    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)

    class ExitedProcess:
        pid = 12345
        waited = False

        async def wait(self):
            self.waited = True
            return 0

    process = ExitedProcess()
    tunnel.ssh_process = process

    def missing_process(pid):
        raise ProcessLookupError

    monkeypatch.setattr(os, "getpgid", missing_process)
    await tunnel.stop()
    assert process.waited
    assert tunnel.ssh_process is None


@pytest.mark.anyio
@pytest.mark.parametrize("nested_timeout", [False, True])
async def test_repeated_startup_cancellation_force_kills_and_reaps(monkeypatch, nested_timeout):
    """An outer deadline interrupts graceful cleanup without orphaning a child."""
    config = SSHTunnelConfig.from_dict({
        "host": "bastion", "user": "tunnel", "ssh_timeout": 1 if nested_timeout else 30,
    })
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    spawn = asyncio.create_subprocess_exec
    killpg = os.killpg
    reached = asyncio.Event()
    terminated = asyncio.Event()
    processes = []

    async def fake_exec(*args, **kwargs):
        process = await spawn(
            sys.executable, "-u", "-c",
            "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready'); time.sleep(60)",
            stdout=asyncio.subprocess.PIPE, preexec_fn=os.setsid,
        )
        assert await process.stdout.readline() == b"ready\n"
        processes.append(process)
        return process

    async def blocked_open(host, port):
        reached.set()
        await asyncio.Event().wait()

    def observe_signal(pgid, sig):
        killpg(pgid, sig)
        if sig == signal.SIGTERM:
            terminated.set()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(asyncio, "open_connection", blocked_open)
    monkeypatch.setattr(os, "killpg", observe_signal)
    started = asyncio.get_running_loop().time()
    call = with_hard_timeout(tunnel.start(), 2) if nested_timeout else tunnel.start()
    task = asyncio.create_task(call)
    try:
        await asyncio.wait_for(reached.wait(), 5)
        if nested_timeout:
            with pytest.raises(HardTimeoutError):
                await task
        else:
            task.cancel()
            await asyncio.wait_for(terminated.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        assert asyncio.get_running_loop().time() - started < 3
        assert len(processes) == 1
        assert processes[0].returncode == -signal.SIGKILL
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()
