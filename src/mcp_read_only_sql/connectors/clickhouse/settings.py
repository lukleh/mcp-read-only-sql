"""Client-side ClickHouse settings, decided from ``system.settings``.

Both ClickHouse connectors ask the server for ``readonly=1`` and a
``max_execution_time`` on every statement. A login whose profile already
sets ``readonly`` (1 or 2) refuses such settings, and a profile constraint
can lock either one. Rather than sending them and interpreting the refusal,
the connectors read ``system.settings`` first, which every login can read:
its ``readonly`` column says whether a setting can be changed in this
session and its ``value`` column what the profile set. The decision is made
from those two facts and nothing else.

A statement is never re-run with weaker settings than the ones it was first
sent with. A refusal that names one of the sent settings only forgets the
remembered decision, so the next statement reads ``system.settings`` again.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from ...errors import ConnectorError

# Sent without any client-side settings, so it reports the profile itself.
PROBE_QUERY = (
    "SELECT name, value, readonly FROM system.settings "
    "WHERE name IN ('readonly', 'max_execution_time')"
)

# Client validation, profile constraints, and server read-only mode.
_REFUSED = re.compile(
    r"Setting (\w+) (?:is (?:unknown or )?readonly|should not be changed)"
    r"|Cannot modify '(\w+)' setting in readonly mode"
)


def client_settings(query_timeout: float) -> dict[str, object]:
    """The settings the connectors want to send with each statement."""
    return {"readonly": 1, "max_execution_time": query_timeout}


def decide_client_settings(
    rows: Iterable[Iterable[object]], query_timeout: float
) -> dict[str, object]:
    """The settings to send, from ``PROBE_QUERY``'s rows (name, value, readonly).

    A locked setting (``readonly`` column 1) is not sent. If ``readonly``
    itself is locked, the profile must already be read-only (value 1 or 2);
    a login whose ``readonly`` is locked at 0 can be neither made read-only
    by the client nor is it read-only by profile, and running statements
    for it is refused. A setting missing from the rows is treated as
    changeable, so it is sent and the server has the last word.
    """
    facts: dict[str, tuple[str, bool]] = {}
    for row in rows:
        fields = [str(field) for field in row]
        if len(fields) >= 3:
            facts[fields[0]] = (fields[1], fields[2] == "1")
    readonly_value, readonly_locked = facts.get("readonly", ("0", False))
    _, timeout_locked = facts.get("max_execution_time", ("0", False))

    settings = client_settings(query_timeout)
    if readonly_locked:
        if readonly_value not in ("1", "2"):
            raise ConnectorError(
                "ClickHouse: this login cannot be made read-only: its profile "
                "locks the readonly setting at 0. Refusing to run statements."
            )
        del settings["readonly"]
    if timeout_locked:
        del settings["max_execution_time"]
    return settings


def refused_setting(message: str) -> str | None:
    """Name of the setting a read-only refusal names, or None otherwise."""
    match = _REFUSED.search(message)
    if match is None:
        return None
    return match.group(1) or match.group(2)


def refuses_sent_setting(settings: dict[str, object], message: str) -> bool:
    """Whether ``message`` refuses one of the settings the connector sent.

    False when it names a setting the connector did not send: that refusal
    came from the statement's own SETTINGS clause.
    """
    name = refused_setting(message)
    return name is not None and name in settings
