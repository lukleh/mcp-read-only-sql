"""Client-side ClickHouse settings and the read-only-profile fallback.

Both ClickHouse connectors ask the server for ``readonly=1`` and a
``max_execution_time`` on every query. A login whose profile already sets
``readonly`` (1 or 2) refuses such settings by name: ClickHouse answers
``Cannot modify '<setting>' setting in readonly mode`` and clickhouse-connect
refuses them client-side as ``Setting <name> is readonly`` before sending.

The connectors find out which settings a login accepts once, with a probe
that does not involve the caller's statement (``SELECT 1`` for the CLI, the
client construction for clickhouse-connect), drop only the refused setting,
and remember the result for the connector's lifetime. A statement is never
re-run with weaker settings than the ones it was first sent with: a
statement's own ``SETTINGS`` clause produces the same refusal text, and
re-running it without ``readonly=1`` would run it unguarded.
"""

from __future__ import annotations

import re

_REFUSED = re.compile(
    r"Setting (\w+) is (?:unknown or )?readonly"  # clickhouse-connect validation
    r"|Cannot modify '(\w+)' setting in readonly mode"  # server, via any client
)


def client_settings(query_timeout: float) -> dict[str, object]:
    """Settings sent with each query when the login's profile allows them."""
    return {"readonly": 1, "max_execution_time": query_timeout}


def refused_setting(message: str) -> str | None:
    """Name of the setting a read-only refusal names, or None otherwise."""
    match = _REFUSED.search(message)
    if match is None:
        return None
    return match.group(1) or match.group(2)


def without_refused(
    settings: dict[str, object], message: str
) -> dict[str, object] | None:
    """``settings`` minus the one ``message`` refuses, or None if it refuses none.

    None also when the refused name is not one of ``settings``: that refusal
    came from the statement itself, not from the client-side settings.
    """
    name = refused_setting(message)
    if name is None or name not in settings:
        return None
    return {key: value for key, value in settings.items() if key != name}
