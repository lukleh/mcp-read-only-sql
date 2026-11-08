"""Bounded streaming and unconditional cleanup for database client processes."""

import asyncio
import codecs
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass

from ..errors import ConnectorError


@dataclass(frozen=True)
class CLIResult:
    returncode: int | None
    stderr: bytes


async def stream_cli_process(
    command: Sequence[str],
    *,
    env: dict[str, str],
    timeout: float,
    label: str,
    emit: Callable[[str], object],
    stdin_data: bytes | None = None,
) -> CLIResult:
    """Stream UTF-8 output with one final LF removed, and always reap the child.

    Chunk reads have no row-size limit. An incremental decoder preserves UTF-8
    across chunk boundaries; holding one byte removes just the final record
    terminator while preserving empty rows, embedded newlines and carriage returns.
    Stderr is drained concurrently, including while password input is written.
    """
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE if stdin_data is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stderr_task = None
    completed = False
    try:
        if process.stdout is None or process.stderr is None:
            raise ConnectorError(f"{label}: failed to create subprocess pipes")
        stderr_task = asyncio.create_task(process.stderr.read())
        async with asyncio.timeout(timeout):
            if stdin_data is not None:
                if process.stdin is None:
                    raise ConnectorError(f"{label}: failed to create subprocess stdin pipe")
                process.stdin.write(stdin_data)
                await process.stdin.drain()
                process.stdin.close()
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            pending = b""
            while chunk := await process.stdout.read(64 * 1024):
                data = pending + chunk
                pending = data[-1:]
                text = decoder.decode(data[:-1])
                if text:
                    emit(text)
            text = decoder.decode(b"" if pending == b"\n" else pending, final=True)
            if text:
                emit(text)
            await process.wait()
            stderr = await stderr_task
            completed = True
            return CLIResult(process.returncode, stderr)
    except TimeoutError as exc:
        raise TimeoutError(f"{label}: Query timeout after {timeout}s") from exc
    finally:
        # This covers cancellation, password/pipe errors, decoder or output-sink
        # failures, and timeouts. A normal EOF alone does not prove child exit.
        if not completed:
            with suppress(ProcessLookupError):
                process.kill()
            with suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=1.0)
        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
        if stderr_task is not None:
            with suppress(Exception, asyncio.CancelledError):
                await stderr_task
