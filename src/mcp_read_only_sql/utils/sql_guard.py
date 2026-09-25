"""SQL sanitization helpers for read-only enforcement.

Two layers live here:

- ``sanitize_read_only_sql`` is dialect-agnostic text hygiene shared by every
  connector: one statement only, no transaction control up front.
- ``sanitize_postgresql_read_only_sql`` additionally parses the statement with
  PostgreSQL's own grammar (via ``pglast``) and applies an allow-list policy:
  only SELECT/EXPLAIN/SHOW statement shapes, and only function calls that
  PostgreSQL itself declares side-effect free (plus a short reviewed list).

The AST policy is a client-side filter, not a privilege boundary. A read-only
transaction still lets a sufficiently privileged role run server-side
programs, write server files, or signal other backends through ordinary
function calls; this layer refuses the plain forms of those. It cannot see
inside views, user-defined functions, operators, or types that already exist
in the database.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pglast import ast, parser, visitors

from .pg_catalog_functions import PG_CATALOG_NON_VOLATILE_FUNCTIONS

__all__ = [
    "ReadOnlyQueryError",
    "sanitize_postgresql_read_only_sql",
    "sanitize_read_only_sql",
]


class ReadOnlyQueryError(ValueError):
    """Raised when user SQL would escape the enforced read-only context."""


_TRANSACTION_PREFIX = re.compile(
    r"^(COMMIT|ROLLBACK|ABORT|END|BEGIN|START\s+TRANSACTION|SET\s+TRANSACTION|"
    r"SET\s+SESSION\s+CHARACTERISTICS|SAVEPOINT|RELEASE\s+SAVEPOINT|"
    r"ROLLBACK\s+TO\s+SAVEPOINT|PREPARE\s+TRANSACTION|COMMIT\s+PREPARED|"
    r"ROLLBACK\s+PREPARED)(\s|;|$)",
    flags=re.IGNORECASE,
)

# Statement node classes that may appear anywhere in the tree (top level, CTE,
# EXPLAIN body, sub-select). Everything else, including every DML/DDL/utility
# statement pglast knows about, is refused by class.
_ALLOWED_STATEMENTS = (ast.SelectStmt, ast.ExplainStmt, ast.VariableShowStmt)

# Non-volatile pg_catalog functions that still must not be reachable: they
# assign a transaction id or execute a SQL string passed as an argument.
_EXCLUDED_FUNCTIONS = frozenset(
    {
        "pg_current_xact_id",
        "ts_rewrite",
        "txid_current",
    }
)

# Volatile pg_catalog functions reviewed as read-only: they return
# nondeterministic values or server state but change nothing. pg_sleep* only
# waits, and the connector's statement_timeout bounds the wait.
_ALLOWED_VOLATILE_FUNCTIONS = frozenset(
    {
        "array_sample",
        "array_shuffle",
        "clock_timestamp",
        "current_query",
        "gen_random_uuid",
        "pg_blocking_pids",
        "pg_current_wal_flush_lsn",
        "pg_current_wal_insert_lsn",
        "pg_current_wal_lsn",
        "pg_database_size",
        "pg_get_wal_replay_pause_state",
        "pg_indexes_size",
        "pg_is_in_recovery",
        "pg_is_wal_replay_paused",
        "pg_jit_available",
        "pg_last_committed_xact",
        "pg_last_wal_receive_lsn",
        "pg_last_wal_replay_lsn",
        "pg_last_xact_replay_timestamp",
        "pg_lock_status",
        "pg_partition_ancestors",
        "pg_partition_tree",
        "pg_relation_size",
        "pg_safe_snapshot_blocking_pids",
        "pg_sequence_last_value",
        "pg_sleep",
        "pg_sleep_for",
        "pg_sleep_until",
        "pg_table_size",
        "pg_tablespace_size",
        "pg_total_relation_size",
        "pg_xact_commit_timestamp",
        "pg_xact_status",
        "random",
        "random_normal",
        "timeofday",
        "txid_status",
    }
)

PG_CATALOG_ALLOWED_FUNCTIONS: frozenset[str] = (
    PG_CATALOG_NON_VOLATILE_FUNCTIONS | _ALLOWED_VOLATILE_FUNCTIONS
) - _EXCLUDED_FUNCTIONS

_ALLOWED_TABLESAMPLE_METHODS = frozenset({"system", "bernoulli"})

_STATEMENT_LABELS = {
    "CallStmt": "CALL",
    "CopyStmt": "COPY",
    "DeleteStmt": "DELETE",
    "DoStmt": "DO",
    "InsertStmt": "INSERT",
    "LoadStmt": "LOAD",
    "MergeStmt": "MERGE",
    "TransactionStmt": "transaction control",
    "TruncateStmt": "TRUNCATE",
    "UpdateStmt": "UPDATE",
    "VariableSetStmt": "SET",
}


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


def sanitize_postgresql_read_only_sql(
    query: str, allowed_functions: Iterable[str] = ()
) -> str:
    """Sanitize ``query`` and enforce the PostgreSQL read-only AST policy.

    ``allowed_functions`` extends the built-in allow-list with names from the
    connection configuration, either bare (``my_helper``) or schema-qualified
    (``public.my_helper``).
    """

    stripped = sanitize_read_only_sql(query)
    try:
        statements = parser.parse_sql(stripped)
    except parser.ParseError as exc:
        raise ReadOnlyQueryError(
            f"SQL could not be parsed, so it cannot be validated for read-only mode: {exc}"
        ) from exc
    if len(statements) != 1:
        raise ReadOnlyQueryError(
            "Multiple SQL statements are not allowed in read-only mode"
        )
    _ReadOnlyPolicy(frozenset(allowed_functions))(statements[0].stmt)
    return stripped


class _ReadOnlyPolicy(visitors.Visitor):
    """Walk the parse tree and refuse anything outside the read-only policy."""

    def __init__(self, extra_functions: frozenset[str]):
        super().__init__()
        self._extra_functions = extra_functions

    def visit(self, ancestors, node):
        if isinstance(node, ast.Node) and type(node).__name__.endswith("Stmt"):
            self._check_statement(node)

    def visit_SelectStmt(self, ancestors, node):
        if node.lockingClause:
            raise ReadOnlyQueryError(
                "SELECT ... FOR UPDATE/SHARE is not allowed in read-only mode"
            )
        if node.intoClause:
            raise ReadOnlyQueryError("SELECT INTO is not allowed in read-only mode")

    def visit_FuncCall(self, ancestors, node):
        parts = [str(part.sval) for part in node.funcname]
        qualified = ".".join(parts)
        if qualified in self._extra_functions:
            return
        if len(parts) == 1:
            name = parts[0]
        elif len(parts) == 2 and parts[0] == "pg_catalog":
            name = parts[1]
        else:
            raise ReadOnlyQueryError(_function_message(qualified))
        if name not in PG_CATALOG_ALLOWED_FUNCTIONS and name not in self._extra_functions:
            raise ReadOnlyQueryError(_function_message(qualified))

    def visit_A_Expr(self, ancestors, node):
        parts = [str(part.sval) for part in node.name or ()]
        if len(parts) > 1 and parts[0] != "pg_catalog":
            raise ReadOnlyQueryError(
                f"Operator {'.'.join(parts)} outside pg_catalog is not allowed in read-only mode"
            )

    def visit_RangeTableSample(self, ancestors, node):
        method = ".".join(str(part.sval) for part in node.method)
        if method.lower() not in _ALLOWED_TABLESAMPLE_METHODS:
            raise ReadOnlyQueryError(
                f"TABLESAMPLE method {method} is not allowed in read-only mode"
            )

    @staticmethod
    def _check_statement(node: ast.Node) -> None:
        if isinstance(node, _ALLOWED_STATEMENTS):
            return
        class_name = type(node).__name__
        label = _STATEMENT_LABELS.get(class_name, class_name.removesuffix("Stmt"))
        raise ReadOnlyQueryError(
            f"{label} statements are not allowed in read-only mode "
            "(only SELECT, EXPLAIN and SHOW are accepted)"
        )


def _function_message(qualified: str) -> str:
    return (
        f"Function {qualified}() is not on the PostgreSQL read-only allow-list; "
        "add it to allowed_functions for this connection if it is safe to call"
    )


def _ensure_single_statement(query: str) -> None:
    semicolons = _find_semicolons_outside_literals(query)
    if not semicolons:
        return
    if len(semicolons) > 1:
        raise ReadOnlyQueryError(
            "Multiple SQL statements are not allowed in read-only mode"
        )
    last = semicolons[0]
    if not _only_trailing_semicolon(query, last):
        raise ReadOnlyQueryError(
            "Multiple SQL statements are not allowed in read-only mode"
        )


def _reject_transaction_control(query: str) -> None:
    stripped = query.lstrip()
    if _TRANSACTION_PREFIX.match(stripped):
        raise ReadOnlyQueryError(
            "Transaction control statements are not allowed in read-only mode"
        )


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
            while tag_end < length and (
                query[tag_end].isalnum() or query[tag_end] == "_"
            ):
                tag_end += 1
            if tag_end < length and query[tag_end] == "$":
                dollar_tag = query[i : tag_end + 1]
                i = tag_end + 1
                continue

        if ch == ";":
            semicolons.append(i)

        i += 1

    return semicolons
