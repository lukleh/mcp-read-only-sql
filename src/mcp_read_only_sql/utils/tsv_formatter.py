"""TSV formatter for database query results."""

from typing import Any, TextIO

# Characters that make psql's CSV mode quote a field when tab is the separator.
_QUOTE_TRIGGERS = ("\t", '"', "\r", "\n")


def format_tsv_field(value: Any) -> str:
    """Render one field the way ``psql --csv`` with a tab separator does.

    NULL and the empty string are both empty. A value containing the
    separator, a double quote, CR or LF is enclosed in double quotes with
    inner quotes doubled. Both connector implementations therefore produce
    the same bytes for the same row.
    """
    if value is None:
        return ""
    text = str(value)
    if any(trigger in text for trigger in _QUOTE_TRIGGERS):
        return '"' + text.replace('"', '""') + '"'
    return text


def format_tsv_line(values: list[Any]) -> str:
    """Render a single TSV line (without trailing newline)."""

    return "\t".join(format_tsv_field(value) for value in values)


def write_tsv_text_line(handle: TextIO, line: str, wrote_content: bool) -> bool:
    """Write a preformatted TSV line without adding a trailing newline."""
    if wrote_content:
        handle.write("\n")
    handle.write(line)
    return True
