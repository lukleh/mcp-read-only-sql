"""Client-side ClickHouse settings and the read-only-profile fallback.

Both ClickHouse connectors ask the server for ``readonly=1`` and a
``max_execution_time`` on every query. A login whose profile already sets
``readonly`` (1 or 2) refuses those requests: ClickHouse answers
``Cannot modify '<setting>' setting in readonly mode`` and clickhouse-connect
refuses them client-side as ``Setting <name> is readonly`` before sending.
Such a profile is stricter than anything the client asks for, so the
connectors retry once without client-side settings and rely on the profile
plus their own hard timeout.
"""

from __future__ import annotations

import re

_REFUSAL = re.compile(
    r"Setting \w+ is (?:unknown or )?readonly"  # clickhouse-connect validation
    r"|Cannot modify '\w+' setting in readonly mode"  # server, via any client
)


def client_settings(query_timeout: float) -> dict[str, object]:
    """Settings sent with each query when the login's profile allows it."""
    return {"readonly": 1, "max_execution_time": query_timeout}


def refuses_client_settings(message: str) -> bool:
    """True when ``message`` says the session's profile forbids these settings."""
    return _REFUSAL.search(message) is not None
