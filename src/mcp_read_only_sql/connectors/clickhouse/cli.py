import logging
import os
import re
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

from ...config import Connection
from ...errors import ConnectorError
from ...utils.cli_process import stream_cli_process
from ...utils.sql_guard import ReadOnlyQueryError, sanitize_read_only_sql
from ...utils.ssh_tunnel_cli import CLISSHTunnel
from ..base_cli import BaseCLIConnector
from .settings import PROBE_QUERY, decide_client_settings, refuses_sent_setting

logger = logging.getLogger(__name__)

# clickhouse-client writes this prompt to stderr under --ask-password; it is
# noise in front of the actual error text.
_PASSWORD_PROMPT = re.compile(r"^Password for user \([^)]*\):[ \t]*", re.MULTILINE)


class ClickHouseCLIConnector(BaseCLIConnector):
    """ClickHouse connector using clickhouse-client CLI tool"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        # Client-side settings decided for each server from its
        # ``system.settings``, keyed by the selected server. A profile is per
        # server, and one server's answer says nothing about another.
        self._decided_settings: dict[tuple[str, int], dict[str, object]] = {}

    async def _decide_settings(
        self, read_probe: Callable[[], Awaitable[list[list[str]]]]
    ) -> dict[str, object]:
        """Settings to send on this server, from its ``system.settings``.

        ``read_probe`` runs ``PROBE_QUERY`` without client-side settings and
        returns its rows. A setting the profile locks is left out, and a
        login whose profile locks ``readonly`` at 0 is refused outright, so
        a statement never runs with weaker settings than the login allows
        and is never re-run. Any failure propagates and nothing is
        remembered.
        """
        settings = decide_client_settings(await read_probe(), self.query_timeout)
        if len(settings) < 2:
            logger.warning(
                "ClickHouse: the profile of user %s locks the client-side %s "
                "setting; continuing without it",
                self.username,
                {"readonly", "max_execution_time"} - set(settings),
            )
        return settings

    @asynccontextmanager
    async def _get_ssh_tunnel(self, server: str | None = None):
        """Override SSH tunnel to ensure we tunnel to native port for clickhouse-client"""
        if self.ssh_config:
            # Get the server to connect to
            selected_server = self._select_server(server)

            # For ClickHouse CLI, we need native port (9000), not HTTP port (8123)
            # If config specifies port 8123, change it to 9000 for the SSH tunnel
            remote_port = selected_server.port
            remote_host = selected_server.host
            if remote_port == 8123:
                logger.debug(
                    "Changing SSH tunnel remote port from 8123 to 9000 for clickhouse-client"
                )
                remote_port = 9000
            elif remote_port == 8443:
                logger.debug(
                    "Changing SSH tunnel remote port from 8443 to 9440 for clickhouse-client"
                )
                remote_port = 9440

            tunnel = CLISSHTunnel(self.ssh_config, remote_host, remote_port)
            local_port = await tunnel.start()
            try:
                yield local_port
            finally:
                await tunnel.stop()
        else:
            yield None

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using clickhouse-client and return raw TSV output"""
        result = await self._run_query(
            query, database=database, server=server, output_path=None
        )
        return result if result is not None else ""

    async def execute_query_to_file(
        self,
        query: str,
        output_path: Path,
        database: str | None = None,
        server: str | None = None,
    ) -> None:
        """Execute a read-only query using clickhouse-client and stream TSV to a file."""
        await self._run_query(
            query,
            database=database,
            server=server,
            output_path=output_path,
        )

    async def _run_query(
        self,
        query: str,
        database: str | None = None,
        server: str | None = None,
        output_path: Path | None = None,
    ) -> str | None:
        """Run clickhouse-client and optionally stream output to a managed file."""
        sanitized_query = sanitize_read_only_sql(query)
        selected_server = self._select_server(server)

        async with self._get_ssh_tunnel(server) as local_port:
            # Use SSH tunnel port if available
            if local_port:
                host = "127.0.0.1"
                port = local_port
            else:
                host = selected_server.host
                port = selected_server.port

                # For direct connections, if port is HTTP (8123/8443), convert to native
                if port == 8123:
                    logger.debug(
                        "Changing port from 8123 to 9000 for clickhouse-client direct connection"
                    )
                    port = 9000
                elif port == 8443:
                    logger.debug(
                        "Changing port from 8443 to 9440 for clickhouse-client direct connection"
                    )
                    port = 9440

            # Use specified database or configured database (validated)
            db_name = self._resolve_database(database)

            # Build clickhouse-client command with read-only enforcement.
            # Resolve the client binary explicitly so installs that are not on
            # PATH still work (mirrors the psql connector).
            binary = self._resolve_binary("clickhouse-client")
            env = os.environ.copy()

            def build_command(
                statement: str,
                settings: dict[str, object],
                fmt: str = "TabSeparatedWithNames",
            ) -> list[str]:
                cmd = [binary]
                if port == 9440:
                    cmd.append("--secure")  # TLS native port
                    logger.debug("Adding --secure flag for TLS port 9440")
                cmd += [
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--user",
                    self.username,
                    "--database",
                    db_name,
                    "--connect_timeout",
                    str(self.connection_timeout),  # Connection timeout
                    "--format",
                    fmt,  # TSV, with a header line for statement output
                    "--query",
                    statement,
                ]
                # Server-side read-only mode and query timeout, minus whatever
                # this login's profile locks (see _decide_settings).
                for name, value in settings.items():
                    cmd += [f"--{name}", str(value)]
                if self.password:
                    cmd.append("--ask-password")
                return cmd

            async def run_client(cmd: list[str], emit: Callable[[str], object]) -> None:
                result = await stream_cli_process(
                    cmd,
                    env=env,
                    timeout=self.query_timeout,
                    label="clickhouse-client",
                    emit=emit,
                    stdin_data=f"{self.password}\n".encode() if self.password else None,
                )
                if result.returncode not in (0, None):
                    error_msg = (
                        _PASSWORD_PROMPT.sub("", result.stderr.decode()).strip()
                        if result.stderr else "Unknown error"
                    )
                    raise ConnectorError(f"clickhouse-client: {error_msg}")

            async def read_probe() -> list[list[str]]:
                chunks: list[str] = []
                await run_client(build_command(PROBE_QUERY, {}, fmt="TabSeparated"), chunks.append)
                return [line.split("\t") for line in "".join(chunks).splitlines()]

            server_key = (selected_server.host, selected_server.port)

            try:
                settings = self._decided_settings.get(server_key)
                if settings is None or "readonly" not in settings:
                    # Unknown, or known to run without readonly=1. The latter
                    # is only safe while this server's profile stays
                    # read-only, so it is read again before every statement.
                    settings = await self._decide_settings(read_probe)
                    self._decided_settings[server_key] = settings
                cmd = build_command(sanitized_query, settings)

                async def run_statement(emit_line: Callable[[str], object]) -> None:
                    try:
                        await run_client(cmd, emit_line)
                    except ConnectorError as exc:
                        if refuses_sent_setting(settings, str(exc)):
                            # The server refuses a setting system.settings
                            # said was changeable: a profile change, or the
                            # statement's own SETTINGS clause naming one of
                            # ours. Forget the answer so the next statement
                            # reads system.settings again.
                            self._decided_settings.pop(server_key, None)
                        raise

                if output_path is None:
                    chunks: list[str] = []
                    await run_statement(chunks.append)
                    return "".join(chunks)
                with Path(output_path).open("w", encoding="utf-8", newline="") as handle:  # noqa: ASYNC230 -- local managed-file writes
                    await run_statement(handle.write)
                return None

            except FileNotFoundError:
                raise FileNotFoundError(
                    "clickhouse-client: command not found. Please install ClickHouse client tools."
                )
            except ReadOnlyQueryError:
                raise
            except TimeoutError as exc:
                logger.error(f"Query execution error: {exc}")
                raise
            except OSError as e:
                # Spawning or talking to the clickhouse-client process failed
                logger.error(f"Query execution error: {e}")
                raise ConnectorError(f"clickhouse-client: {e}") from e
