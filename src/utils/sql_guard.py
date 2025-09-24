"""PostgreSQL-oriented SQL sanitization helpers for read-only enforcement."""

from __future__ import annotations

import re

__all__ = ["ReadOnlyQueryError", "sanitize_read_only_sql"]


class ReadOnlyQueryError(ValueError):
    """Raised when user SQL would escape the enforced read-only context."""


_TRANSACTION_PREFIX = re.compile(
    r"^(COMMIT|ROLLBACK|ABORT|END|BEGIN|START\s+TRANSACTION|SET\s+TRANSACTION|"
    r"SET\s+SESSION\s+CHARACTERISTICS|SAVEPOINT|RELEASE\s+SAVEPOINT|"
    r"ROLLBACK\s+TO\s+SAVEPOINT|PREPARE\s+TRANSACTION|COMMIT\s+PREPARED|"
    r"ROLLBACK\s+PREPARED)(\s|;|$)",
    flags=re.IGNORECASE,
)


def sanitize_read_only_sql(query: str) -> str:
    """Return a trimmed SQL string that is safe for read-only execution.

    Ensures there is at most a single statement (optional trailing semicolon)
    and blocks PostgreSQL transaction-control commands that could disable
    read-only mode.
    """

    if query is None:
        raise ReadOnlyQueryError("Query must not be None")

    stripped = query.strip()
    if not stripped:
        raise ReadOnlyQueryError("Query must not be empty")

    _ensure_single_statement(stripped)
    _reject_transaction_control(stripped)
    return stripped


def _ensure_single_statement(query: str) -> None:
    semicolons = _find_semicolons_outside_literals(query)
    if not semicolons:
        return
    if len(semicolons) > 1:
        raise ReadOnlyQueryError("Multiple SQL statements are not allowed in read-only mode")
    last = semicolons[0]
    if not _only_trailing_semicolon(query, last):
        raise ReadOnlyQueryError("Multiple SQL statements are not allowed in read-only mode")


def _reject_transaction_control(query: str) -> None:
    stripped = query.lstrip()
    if _TRANSACTION_PREFIX.match(stripped):
        raise ReadOnlyQueryError("Transaction control statements are not allowed in read-only mode")


def _only_trailing_semicolon(query: str, index: int) -> bool:
    tail = query[index + 1 :]
    return _remove_comments(tail).strip() == ""


def _remove_comments(sql: str) -> str:
    result: list[str] = []
    length = len(sql)
    i = 0
    in_block = 0
    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""
        if in_block:
            if ch == "*" and nxt == "/":
                in_block -= 1
                i += 2
            elif ch == "/" and nxt == "*":
                in_block += 1
                i += 2
            else:
                i += 1
            continue
        if ch == "-" and nxt == "-":
            i += 2
            while i < length and sql[i] not in "\r\n":
                i += 1
            continue
        if ch == "/" and nxt == "*":
            in_block = 1
            i += 2
            continue
        result.append(ch)
        i += 1
    return "".join(result)


def _find_semicolons_outside_literals(query: str) -> list[int]:
    semicolons: list[int] = []
    length = len(query)
    i = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = 0
    dollar_tag: str | None = None

    while i < length:
        ch = query[i]
        nxt = query[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if ch in "\r\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment -= 1
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment += 1
                i += 2
                continue
            i += 1
            continue

        if dollar_tag:
            if query.startswith(dollar_tag, i):
                i += len(dollar_tag)
                dollar_tag = None
            else:
                i += 1
            continue

        if in_single:
            if ch == "'":
                if nxt == "'":
                    i += 2
                    continue
                in_single = False
            i += 1
            continue

        if in_double:
            if ch == '"':
                if nxt == '"':
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue

        if ch == "/" and nxt == "*":
            in_block_comment = 1
            i += 2
            continue

        if ch == "'":
            in_single = True
            i += 1
            continue

        if ch == '"':
            in_double = True
            i += 1
            continue

        if ch == "$":
            tag_end = i + 1
            while tag_end < length and (query[tag_end].isalnum() or query[tag_end] == "_"):
                tag_end += 1
            if tag_end < length and query[tag_end] == "$":
                dollar_tag = query[i : tag_end + 1]
                i = tag_end + 1
                continue

        if ch == ";":
            semicolons.append(i)

        i += 1

    return semicolons
