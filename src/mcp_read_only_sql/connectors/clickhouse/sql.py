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
_ENCODED_STRING = re.compile(r"(?:[xX]'[0-9A-Fa-f]*'|[bB]'[01]*')")
# ClickHouse also treats these format characters as UTF-8 whitespace.
_EXTRA_WHITESPACE = frozenset("\u180e\u200b\u200c\u200d\u2060\ufeff")

@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    end: int


def _tokens(query: str) -> tuple[list[_Token], bool]:
    """Tokenize ClickHouse quoting, heredocs, and nested/line comments."""
    tokens = []
    parser_compatible = True
    i = 0
    while i < len(query):
        ch = query[i]
        if ch.isspace() or ch in _EXTRA_WHITESPACE:
            if ch in _EXTRA_WHITESPACE:
                parser_compatible = False
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
        elif ch in "xXbB" and query[i + 1:i + 2] == "'":
            # ClickHouse lexes adjacent hex/binary prefixes as string literals.
            # These forms allow digits only, without ordinary quote escapes.
            encoded = _ENCODED_STRING.match(query, i)
            if encoded is None:
                raise ReadOnlyQueryError("Invalid ClickHouse encoded string literal")
            i = encoded.end()
            tokens.append(_Token("string", "", i))
        elif ch in "‘“":
            parser_compatible = False
            # ClickHouse's Unicode quotes do not have escape sequences.
            closing = "’" if ch == "‘" else "”"
            end = query.find(closing, i + 1)
            if end < 0:
                raise ReadOnlyQueryError("Unterminated ClickHouse quoted value")
            i = end + 1
            tokens.append(_Token("string" if ch == "‘" else "identifier", "", i))
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
            tokens.append(_Token("string" if quote == "'" else "identifier", "", i))
        elif ch == "$" and (match := _HEREDOC.match(query, i)):
            tag = match.group()
            end = query.find(tag, match.end())
            if end < 0:
                # An unmatched $tag$ prefix can instead be an identifier.
                word = _WORD.match(query, i)
                assert word is not None
                tokens.append(_Token("word", word.group().upper(), word.end()))
                i = word.end()
            else:
                tokens.append(_Token("string", "", end + len(tag)))
                i = end + len(tag)
        elif match := _WORD.match(query, i):
            tokens.append(_Token("word", match.group().upper(), match.end()))
            i = match.end()
        else:
            tokens.append(_Token("symbol", ch, i + 1))
            i += 1
    return tokens, parser_compatible


def sanitize_clickhouse_read_only_sql(query: str) -> str:
    """Keep one statement and reserve output format for the connector."""
    if not query or not query.strip():
        raise ReadOnlyQueryError("Query must not be empty")
    stripped = query.strip()
    tokens, parser_compatible = _tokens(stripped)
    if not tokens:
        raise ReadOnlyQueryError("Query must not be empty")
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
        if not parser_compatible:
            # SQLGlot treats these native quote/whitespace forms as identifiers,
            # which can hide output clauses. Refuse rather than rewrite SQL.
            raise ReadOnlyQueryError(
                "Cannot validate ClickHouse FORMAT usage with Unicode quotes or "
                "format-character whitespace; use ASCII syntax or quote identifiers named FORMAT"
            )
        try:
            # The lexer has already checked the optional trailing semicolon.
            # Parse only through the last SQL token, excluding that semicolon
            # and trailing comments, including ClickHouse's // syntax.
            last_token = tokens[-2] if semicolons else tokens[-1]
            trees = sqlglot.parse(stripped[:last_token.end], read="clickhouse")
        except SqlglotError as exc:
            raise ReadOnlyQueryError(
                "Cannot validate ClickHouse FORMAT usage; quote identifiers named FORMAT "
                "or remove the output clause"
            ) from exc
        if len(trees) != 1 or trees[0] is None or any(
            isinstance(node, exp.Command) for node in trees[0].walk()
        ):
            raise ReadOnlyQueryError(
                "Cannot validate ClickHouse FORMAT usage; omit output FORMAT clauses "
                "or quote identifiers named FORMAT"
            )
        # Formatting functions also use a "format" argument for input data;
        # only query modifiers and file-format properties control output.
        if any(
            isinstance(node, exp.FileFormatProperty)
            or (
                isinstance(node, (exp.Query, exp.Table, exp.TableFromRows, exp.Values))
                and node.args.get("format") is not None
            )
            for node in trees[0].walk()
        ):
            raise ReadOnlyQueryError(
                "ClickHouse FORMAT clauses are not allowed; results use TabSeparatedWithNames"
            )
    return stripped
