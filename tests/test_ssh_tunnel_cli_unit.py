import asyncio
import os
import shutil
import signal
import socket
import subprocess
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
        returncode = 0
        waited = False

        async def wait(self):
            self.waited = True
            return 0

    process = ExitedProcess()
    tunnel.ssh_process = process

    def unexpected_signal(*args):
        pytest.fail("An already-reaped process must not be signalled")

    monkeypatch.setattr(os, "getpgid", unexpected_signal)
    monkeypatch.setattr(os, "killpg", unexpected_signal)
    await tunnel.stop()
    assert process.waited
    assert tunnel.ssh_process is None


@pytest.mark.anyio
@pytest.mark.parametrize("cancellation", ["single", "repeated", "hard_timeout", "ssh_timeout"])
async def test_startup_cancellation_force_kills_and_reaps(monkeypatch, cancellation):
    """An outer deadline interrupts graceful cleanup without orphaning a child."""
    config = SSHTunnelConfig.from_dict({
        "host": "bastion", "user": "tunnel", "ssh_timeout": 1 if cancellation == "ssh_timeout" else 30,
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
    call = with_hard_timeout(tunnel.start(), 0.1) if cancellation == "hard_timeout" else tunnel.start()
    task = asyncio.create_task(call)
    try:
        await asyncio.wait_for(reached.wait(), 5)
        if cancellation == "hard_timeout":
            with pytest.raises(HardTimeoutError):
                await task
        elif cancellation == "ssh_timeout":
            with pytest.raises(TimeoutError, match="SSH: Connection timeout"):
                await task
        else:
            task.cancel()
            if cancellation == "repeated":
                await asyncio.wait_for(terminated.wait(), 1)
                task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        assert asyncio.get_running_loop().time() - started < 1.75
        assert len(processes) == 1
        assert processes[0].returncode == -signal.SIGKILL
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


@pytest.mark.anyio
async def test_cancellation_after_startup_completion_reaps_child(monkeypatch):
    """The caller can be cancelled after readiness but before owning the tunnel."""
    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    spawn = asyncio.create_subprocess_exec
    wait_for = asyncio.wait_for
    processes = []
    completed = False

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

    async def ready_open(host, port):
        return object(), Writer()

    async def interrupt_completed_startup(future, timeout):
        nonlocal completed
        result = await wait_for(future, timeout)
        if timeout == tunnel.ssh_timeout:
            assert future.done() and not future.cancelled()
            assert result == tunnel.local_port
            completed = True
            raise asyncio.CancelledError
        return result

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(asyncio, "open_connection", ready_open)
    monkeypatch.setattr(asyncio, "wait_for", interrupt_completed_startup)
    try:
        with pytest.raises(asyncio.CancelledError):
            await tunnel.start()
        assert completed
        assert len(processes) == 1
        assert processes[0].returncode is not None
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


@pytest.mark.anyio
async def test_normal_stop_preserves_graceful_shutdown():
    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-c",
        "import signal,sys,time; "
        "signal.signal(signal.SIGTERM, lambda *_: (time.sleep(.4), sys.exit(0))); "
        "print('ready'); time.sleep(60)",
        stdout=asyncio.subprocess.PIPE, preexec_fn=os.setsid,
    )
    tunnel.ssh_process = process
    try:
        assert await process.stdout.readline() == b"ready\n"
        started = asyncio.get_running_loop().time()
        await tunnel.stop()
        assert asyncio.get_running_loop().time() - started >= .35
        assert process.returncode == 0
        assert tunnel.ssh_process is None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.anyio
@pytest.mark.parametrize("hard_deadline", [False, True])
async def test_established_query_cancellation_uses_short_cleanup(monkeypatch, hard_deadline):
    import mcp_read_only_sql.connectors.base as connector_base
    from tests.conftest import make_recording_connector

    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-c",
        "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready'); time.sleep(60)",
        stdout=asyncio.subprocess.PIPE, preexec_fn=os.setsid,
    )
    connector = make_recording_connector({
        "connection_name": "cancel_body", "type": "postgresql",
        "servers": ["db.internal:5432"], "db": "default", "username": "reader",
        "ssh_tunnel": {"host": "bastion", "user": "tunnel"},
    })
    ready = asyncio.Event()

    async def started():
        return 45678

    async def query_body():
        async with connector._get_ssh_tunnel():
            ready.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(tunnel, "start", started)
    monkeypatch.setattr(connector_base, "CLISSHTunnel", lambda *args: tunnel)
    task = None
    try:
        assert await process.stdout.readline() == b"ready\n"
        tunnel.ssh_process = process
        begin = asyncio.get_running_loop().time()
        call = with_hard_timeout(query_body(), .1) if hard_deadline else query_body()
        task = asyncio.create_task(call)
        await asyncio.wait_for(ready.wait(), 1)
        if hard_deadline:
            with pytest.raises(HardTimeoutError):
                await task
        else:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert asyncio.get_running_loop().time() - begin < 1
        assert process.returncode == -signal.SIGKILL
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.anyio
async def test_startup_failure_racing_cancellation_keeps_cancellation(monkeypatch):
    from mcp_read_only_sql.errors import ConnectorError

    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    wait_for = asyncio.wait_for
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(60)", preexec_fn=os.setsid,
    )

    async def fail_startup():
        tunnel.ssh_process = process
        raise ConnectorError("startup failed")

    async def interrupt_failed_startup(future, timeout):
        try:
            return await wait_for(future, timeout)
        except ConnectorError:
            raise asyncio.CancelledError from None

    monkeypatch.setattr(tunnel, "_start_tunnel", fail_startup)
    monkeypatch.setattr(asyncio, "wait_for", interrupt_failed_startup)
    try:
        with pytest.raises(asyncio.CancelledError):
            await tunnel.start()
        assert process.returncode is not None
        assert tunnel.ssh_process is None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.anyio
@pytest.mark.skipif(
    not all(shutil.which(binary) for binary in ("sshpass", "ssh", "pgrep")),
    reason="requires local sshpass, ssh, and pgrep binaries",
)
@pytest.mark.parametrize("child", ["ssh", "forward_listener"])
async def test_spawn_cancellation_reaps_sshpass_descendant(monkeypatch, child):
    """Actual sshpass child sessions survive direct-child KILL; TERM must reach them."""
    import asyncio.unix_events

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    config = SSHTunnelConfig.from_dict({
        "host": "127.0.0.1", "port": listener.getsockname()[1],
        "user": "reader", "password": "local-fixture-only", "ssh_timeout": 10,
    })
    tunnel = CLISSHTunnel(config, "localhost", 5432)
    loop = asyncio.get_running_loop()
    transport_class = asyncio.unix_events._UnixSubprocessTransport
    original_init = transport_class.__init__
    spawn = asyncio.create_subprocess_exec
    pipes_ready = asyncio.Event()
    transports = []
    waiters = []
    processes = []
    descendant = None
    accepted = None

    def delayed_pipe_ready(self, loop, protocol, args, shell, stdin, stdout, stderr,
                           bufsize, waiter=None, extra=None, **kwargs):
        # Delay only publication of CPython's real constructed Process. Its
        # normal cancellation path would close/kill sshpass before we own it.
        inner = loop.create_future()
        inner.add_done_callback(lambda _: pipes_ready.set())
        original_init(self, loop, protocol, args, shell, stdin, stdout, stderr,
                      bufsize, waiter=inner, extra=extra, **kwargs)
        transports.append(self)
        waiters.append(waiter)

    async def local_spawn(*args, **kwargs):
        if child == "ssh":
            # Isolate the local banner connection from personal ssh settings.
            args = (*args[:3], "-F", "/dev/null", *args[3:])
        else:
            # A real password wrapper around a child with a forwarded-port
            # listener proves listener lifetime as well as descendant lifetime.
            code = (
                "import socket,time; "
                f"forward=socket.socket(); forward.bind(('127.0.0.1', {tunnel.local_port})); "
                "forward.listen(); "
                f"peer=socket.create_connection(('127.0.0.1', {config.port})); time.sleep(60)"
            )
            args = (*args[:2], sys.executable, "-c", code)
        process = await spawn(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(transport_class, "__init__", delayed_pipe_ready)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", local_spawn)
    task = asyncio.create_task(tunnel.start())
    try:
        await asyncio.wait_for(pipes_ready.wait(), 3)
        accepted, _ = await asyncio.wait_for(loop.sock_accept(listener), 3)
        parent_pid = transports[0].get_pid()
        found = (await asyncio.to_thread(
            subprocess.run,
            ["pgrep", "-P", str(parent_pid)], capture_output=True, text=True, check=False,
        )).stdout.split()
        assert len(found) == 1
        descendant = int(found[0])
        assert os.getpgid(descendant) != os.getpgid(parent_pid)
        assert tunnel.ssh_process is None
        tunnel_port = tunnel.local_port
        assert tunnel_port is not None
        task.cancel()
        # Leave construction suspended long enough for cancellation to arrive,
        # then publish it so the tunnel can acquire and stop the wrapper.
        await asyncio.sleep(.05)
        assert not waiters[0].cancelled()
        waiters[0].set_result(None)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1.5)
        assert len(processes) == 1
        assert processes[0].returncode is not None
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
        with pytest.raises(ProcessLookupError):
            os.kill(descendant, 0)
        descendant = None
        # Both the SSH banner connection and fixture's connection must close.
        while await asyncio.wait_for(loop.sock_recv(accepted, 4096), 1):
            pass
        if child == "forward_listener":
            with socket.socket() as check:
                assert check.connect_ex(("127.0.0.1", tunnel_port)) != 0
    finally:
        # Release construction even when an assertion fails, then retain and
        # clean every process owned by this test.
        for waiter in waiters:
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if descendant is not None:
            try:
                os.kill(descendant, signal.SIGTERM)
            except ProcessLookupError:
                pass
        await tunnel.stop(urgent=True)
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()
        if accepted is not None:
            accepted.close()
        listener.close()


@pytest.mark.anyio
async def test_spawn_failure_racing_cancellation_keeps_cancellation(monkeypatch):
    config = SSHTunnelConfig.from_dict({"host": "bastion", "user": "tunnel"})
    tunnel = CLISSHTunnel(config, "db.internal", 5432)
    reached = asyncio.Event()
    release = asyncio.Event()

    async def failed_spawn(*args, **kwargs):
        reached.set()
        await release.wait()
        raise OSError("subprocess construction failed")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failed_spawn)
    task = asyncio.create_task(tunnel.start())
    try:
        await reached.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert tunnel.ssh_process is None
        assert tunnel._stop_task is None
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
