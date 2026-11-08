import logging
import os
from pathlib import Path

from ...errors import ConnectorError
from ...utils.cli_process import stream_cli_process
from ...utils.sql_guard import (
    SHADOW_GUARD_PREFIX,
    ReadOnlyQueryError,
    postgresql_shadow_guard_block,
    postgresql_shadow_query,
    sanitize_postgresql_read_only_sql,
)
from ..base_cli import BaseCLIConnector

logger = logging.getLogger(__name__)


class PostgreSQLCLIConnector(BaseCLIConnector):
    """PostgreSQL connector using psql CLI tool"""

    async def execute_query(
        self, query: str, database: str | None = None, server: str | None = None
    ) -> str:
        """Execute a read-only query using psql and return raw TSV output"""
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
        """Execute a read-only query using psql and stream TSV to a file."""
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
        """Run the psql command and optionally stream output to a managed file."""
        sanitized_query = sanitize_postgresql_read_only_sql(
            query, self.connection.allowed_functions
        )
        shadow_query = postgresql_shadow_query(
            query, self.connection.allowed_functions
        )
        shadow_guard = (
            postgresql_shadow_guard_block(shadow_query) + ";" if shadow_query else ""
        )
        selected_server = self._select_server(server)

        async with self._get_ssh_tunnel(server) as local_port:
            # Use SSH tunnel port if available
            if local_port:
                host = "127.0.0.1"
                port = local_port
            else:
                host = selected_server.host
                port = selected_server.port

            # Use specified database or configured database (validated)
            db_name = self._resolve_database(database)

            # Build psql command with read-only enforcement. psql's
            # --single-transaction opens the transaction; the command string
            # makes it read-only before the statement runs. The statement
            # timeout is capped at the hard timeout: when psql is killed at
            # the hard timeout, the server ends the statement by then too.
            statement_timeout_ms = int(min(self.query_timeout, self.hard_timeout) * 1000)
            wrapped_query = f"""
                SET TRANSACTION READ ONLY;
                SET LOCAL statement_timeout = {statement_timeout_ms};
                {shadow_guard}
                {sanitized_query};
            """

            # Build psql command with individual parameters.
            # Resolve the client binary explicitly so installs that are not on
            # PATH (e.g. Homebrew keg-only libpq on macOS) still work.
            # -q keeps command tags ("SET", "DO") off stdout and CSV mode
            # prints no footer, so every line psql prints belongs to the
            # header or to a row and nothing has to be filtered. With a tab as
            # the CSV separator the output is TSV whose fields are quoted only
            # when they contain a tab, a quote or a line break (psql 12+).
            cmd = [
                self._resolve_binary("psql"),
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "-q",  # No command tags on stdout
                "-h",
                host,  # Host
                "-p",
                str(port),  # Port
                "-d",
                db_name,  # Database
                "-U",
                self.username,  # Username
                "--csv",  # CSV quoting rules, no footer
                "-P",
                "csv_fieldsep=\t",  # Tab as the field separator
                "-c",
                wrapped_query,  # Query to execute
            ]

            async def run_psql(env_vars: dict[str, str]) -> str | None:
                async def run(emit):
                    result = await stream_cli_process(
                        cmd, env=env_vars, timeout=self.query_timeout, label="psql", emit=emit
                    )
                    if result.returncode not in (0, None):
                        error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                        if SHADOW_GUARD_PREFIX in error_msg:
                            start = error_msg.index(SHADOW_GUARD_PREFIX)
                            raise ReadOnlyQueryError(error_msg[start:].splitlines()[0].strip())
                        raise ConnectorError(f"psql: {error_msg}")

                if output_path is None:
                    chunks: list[str] = []
                    await run(chunks.append)
                    return "".join(chunks)
                with Path(output_path).open("w", encoding="utf-8", newline="") as handle:  # noqa: ASYNC230 -- local managed-file writes
                    await run(handle.write)
                return None

            use_pgoptions = getattr(self.connection, "cli_requires_pgoptions", True)
            attempts = [True] if not use_pgoptions else [True, False]

            for first_attempt in attempts:
                env = os.environ.copy()
                env["PGPASSWORD"] = self.password
                env["PGCONNECT_TIMEOUT"] = str(self.connection_timeout)
                if first_attempt and use_pgoptions:
                    env["PGOPTIONS"] = "-c default_transaction_read_only=on"
                else:
                    env.pop("PGOPTIONS", None)

                try:
                    return await run_psql(env)
                except RuntimeError as exc:
                    message = str(exc).lower()
                    if (
                        first_attempt
                        and use_pgoptions
                        and "unsupported startup parameter" in message
                    ):
                        logger.warning(
                            "psql: remote server rejected default_transaction_read_only; retrying without PGOPTIONS"
                        )
                        continue
                    raise
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "psql: command not found. Please install PostgreSQL client tools."
                    )
                except ReadOnlyQueryError:
                    raise
                except TimeoutError as exc:
                    logger.error(f"Query execution error: {exc}")
                    raise
                except OSError as e:
                    # Spawning or talking to the psql process failed
                    logger.error(f"Query execution error: {e}")
                    raise ConnectorError(f"psql: {e}") from e
