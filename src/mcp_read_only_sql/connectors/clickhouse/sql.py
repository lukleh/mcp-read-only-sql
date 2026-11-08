"""ClickHouse lexical boundaries for client-controlled output.

The database enforces read-only SQL. This lexer only distinguishes tokens
from quoted data and comments, so client output clauses cannot change the
managed TSV contract. It does not attempt to classify SQL statement shapes.
"""

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from ...utils.sql_guard import ReadOnlyQueryError

_HEREDOC = re.compile(r"\$[A-Za-z0-9_]*\$")
_WORD = re.compile(r"[A-Za-z0-9_$]+")

@dataclass(frozen=True)
class _Token:
    kind: str
    value: str


def _tokens(query: str) -> list[_Token]:
    """Tokenize ClickHouse quoting, heredocs, and nested/line comments."""
    tokens = []
    i = 0
    while i < len(query):
        ch = query[i]
        if ch.isspace():
            i += 1
        elif query.startswith(("--", "//", "#!", "# "), i):
            while i < len(query) and query[i] not in "\r\n":
                i += 1
        elif query.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(query) and depth:
                if query.startswith("/*", i):
                    depth += 1
                    i += 2
                elif query.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise ReadOnlyQueryError("Unterminated ClickHouse comment")
        elif ch in "'\"`":
            quote = ch
            i += 1
            while i < len(query):
                if query[i] == "\\":
                    i += 2
                elif query[i] == quote:
                    i += 1
                    if i < len(query) and query[i] == quote:
                        i += 1
                        continue
                    break
                else:
                    i += 1
            else:
                raise ReadOnlyQueryError("Unterminated ClickHouse quoted value")
            tokens.append(_Token("string" if quote == "'" else "identifier", ""))
        elif ch == "$" and (match := _HEREDOC.match(query, i)):
            tag = match.group()
            end = query.find(tag, match.end())
            if end < 0:
                # An unmatched $tag$ prefix can instead be an identifier.
                word = _WORD.match(query, i)
                assert word is not None
                tokens.append(_Token("word", word.group().upper()))
                i = word.end()
            else:
                tokens.append(_Token("string", ""))
                i = end + len(tag)
        elif match := _WORD.match(query, i):
            tokens.append(_Token("word", match.group().upper()))
            i = match.end()
        else:
            tokens.append(_Token("symbol", ch))
            i += 1
    return tokens


def sanitize_clickhouse_read_only_sql(query: str) -> str:
    """Keep one statement and reserve output format for the connector."""
    if not query or not query.strip():
        raise ReadOnlyQueryError("Query must not be empty")
    stripped = query.strip()
    tokens = _tokens(stripped)
    semicolons = [i for i, token in enumerate(tokens) if token.value == ";"]
    if semicolons and (len(semicolons) != 1 or semicolons[0] != len(tokens) - 1):
        raise ReadOnlyQueryError("Multiple SQL statements are not allowed in read-only mode")
    # INSERT's FORMAT describes input data, not client output. The database's
    # readonly setting refuses the statement as before.
    if tokens and tokens[0].value == "INSERT":
        return stripped
    if any(token.kind == "word" and token.value == "FORMAT" for token in tokens):
        # FORMAT is not a reserved identifier: only a dialect parse can tell
        # an output clause from a column, alias, or function with that name.
        # Queries without the token remain entirely in ClickHouse's grammar.
        try:
            trees = sqlglot.parse(stripped, read="clickhouse")
        except SqlglotError as exc:
            raise ReadOnlyQueryError(
                "Cannot validate ClickHouse FORMAT usage; quote identifiers named FORMAT "
                "or remove the output clause"
            ) from exc
        if len(trees) != 1 or trees[0] is None or any(
            isinstance(node, exp.Command) for node in trees[0].walk()
        ):
            raise ReadOnlyQueryError("Cannot validate ClickHouse FORMAT usage")
        if any(
            node.args.get("format") is not None or isinstance(node, exp.FileFormatProperty)
            for node in trees[0].walk()
        ):
            raise ReadOnlyQueryError(
                "ClickHouse FORMAT clauses are not allowed; results use TabSeparatedWithNames"
            )
    return stripped
