"""Base class for connectors that shell out to a database client."""

from ..cli_binaries import resolve_cli_binary
from ..config import Connection
from .base import BaseConnector


class BaseCLIConnector(BaseConnector):
    """Base class for connectors that shell out to a database client."""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self._ssh_tunnel = None
        # Resolved CLI client paths, cached per connector instance. Connectors
        # are rebuilt on config reload, so this re-resolves after a reload while
        # avoiding a lookup (and a possible `brew --prefix` probe) per query.
        self._binary_cache: dict[str, str] = {}

    def _resolve_binary(self, name: str) -> str:
        """Resolve and cache the absolute path to a CLI client binary.

        Called synchronously from the async ``_run_query``. On the macOS
        fallback path resolution may run a blocking ``brew --prefix`` probe (see
        ``cli_binaries._brew_prefix``); caching the result here keeps that to at
        most once per connector instead of once per query. Failures are not
        cached, so resolution retries on the next query (e.g. if the client is
        installed mid-session).
        """
        cached = self._binary_cache.get(name)
        if cached is None:
            cached = resolve_cli_binary(name)
            self._binary_cache[name] = cached
        return cached
